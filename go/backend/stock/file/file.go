package file

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type fileStock[I stock.Index, V any] struct {
	directory       string
	encoder         stock.ValueEncoder[V]
	values          *utils.BufferedFile
	freelist        *fileBasedStack[I]
	numValueSlots   I
	numValuesInFile int64
}

func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	metafile := directory + "/meta.json"
	valuefile := directory + "/values.dat"
	freelistfile := directory + "/freelist.dat"
	numValueSlots := I(0)
	numValuesInFile := int64(0)

	// Create the direcory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	// If there is a meta-file in the directory, check its content.
	if data, err := os.ReadFile(metafile); err == nil {
		var meta metadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, err
		}

		// Check meta-data format information.
		if meta.Version != dataFormatVersion {
			return nil, fmt.Errorf("invalid file format version, got %d, wanted %d", meta.Version, dataFormatVersion)
		}
		indexSize := int(unsafe.Sizeof(I(0)))
		if meta.IndexTypeSize != indexSize {
			return nil, fmt.Errorf("invalid index type encoding, expected %d byte, found %d", indexSize, meta.IndexTypeSize)
		}
		valueSize := encoder.GetEncodedSize()
		if meta.ValueTypeSize != valueSize {
			return nil, fmt.Errorf("invalid value type encoding, expected %d byte, found %d", valueSize, meta.ValueTypeSize)
		}

		// Check size of the value file.
		{
			stats, err := os.Stat(valuefile)
			if err != nil {
				return nil, err
			}
			expectedSize := meta.NumValuesInFile * int64(valueSize)
			if got, want := stats.Size(), expectedSize; got < want {
				return nil, fmt.Errorf("insufficient value file size, got %d, wanted %d", got, want)
			}
		}

		// Check size of the free-list file.
		{
			stats, err := os.Stat(freelistfile)
			if err != nil {
				return nil, err
			}
			if got, want := stats.Size(), int64(meta.FreeListLength*indexSize); got != want {
				return nil, fmt.Errorf("invalid free-list file size, got %d, wanted %d", got, want)
			}
		}

		numValueSlots = I(meta.ValueListLength)
		numValuesInFile = meta.NumValuesInFile
	}

	values, err := utils.OpenBufferedFile(valuefile)
	if err != nil {
		return nil, err
	}

	freelist, err := openFileBasedStack[I](freelistfile)
	if err != nil {
		return nil, err
	}

	// Create new files
	return &fileStock[I, V]{
		encoder:         encoder,
		directory:       directory,
		values:          values,
		freelist:        freelist,
		numValueSlots:   numValueSlots,
		numValuesInFile: numValuesInFile,
	}, nil
}

func (s *fileStock[I, V]) New() (I, error) {
	index := s.numValueSlots

	// Reuse free index positions or grow list of values.
	if !s.freelist.Empty() {
		free, err := s.freelist.Pop()
		if err != nil {
			return 0, err
		}
		index = free
	} else {
		s.numValueSlots++
	}

	return I(index), nil
}

func (s *fileStock[I, V]) Get(index I) (V, error) {
	var res V
	if index >= I(s.numValueSlots) || index < 0 {
		return res, nil
	}
	if index >= I(s.numValuesInFile) {
		return res, nil
	}
	// Load value from the file.
	valueSize := s.encoder.GetEncodedSize()
	offset := int64(valueSize) * int64(index)
	buffer := make([]byte, valueSize)
	err := s.values.Read(offset, buffer)
	if err != nil {
		return res, err
	}

	if err := s.encoder.Load(buffer, &res); err != nil {
		return res, err
	}
	return res, nil
}

func (s *fileStock[I, V]) Set(index I, value V) error {
	if index >= s.numValueSlots || index < 0 {
		return fmt.Errorf("index out of range, got %d, range [0,%d)", index, s.numValueSlots)
	}

	// Encode the value to be written.
	valueSize := s.encoder.GetEncodedSize()
	buffer := make([]byte, valueSize)
	s.encoder.Store(buffer, &value)

	// If the new data is beyond the end of the current file and empty, we can skip
	// the write operation.
	if index >= I(s.numValuesInFile) && allZero(buffer) {
		return nil
	}

	// Write a serialized form of the value to disk.
	offset := int64(valueSize) * int64(index)
	if err := s.values.Write(offset, buffer); err != nil {
		return err
	}
	if int64(index) >= s.numValuesInFile {
		s.numValuesInFile = int64(index) + 1
	}
	return nil
}

func (s *fileStock[I, V]) Delete(index I) error {
	return s.freelist.Push(index)
}

func (s *fileStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("freelist", s.freelist.GetMemoryFootprint())
	return res
}

func (s *fileStock[I, V]) Flush() error {
	// Write metadata.
	var index I
	indexSize := int(unsafe.Sizeof(index))
	metadata, err := json.Marshal(metadata{
		Version:         dataFormatVersion,
		IndexTypeSize:   indexSize,
		ValueTypeSize:   s.encoder.GetEncodedSize(),
		ValueListLength: int(s.numValueSlots),
		FreeListLength:  s.freelist.Size(),
		NumValuesInFile: s.numValuesInFile,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.directory+"/meta.json", metadata, 0600); err != nil {
		return err
	}

	// Flush freelist and value file.
	return errors.Join(
		s.values.Flush(),
		s.freelist.Flush(),
	)
}

func (s *fileStock[I, V]) Close() error {
	// Flush is executed before the closing of the file since
	// in Go the evaluation order of arguments is fixed.
	// see: https://go.dev/ref/spec#Order_of_evaluation
	return errors.Join(
		s.Flush(),
		s.values.Close(),
		s.freelist.Close(),
	)
}

const dataFormatVersion = 0

// metadata is the helper type to read and write metadata from/to the disk.
type metadata struct {
	Version         int
	IndexTypeSize   int
	ValueTypeSize   int
	ValueListLength int
	FreeListLength  int
	NumValuesInFile int64
}

func allZero(data []byte) bool {
	for _, cur := range data {
		if cur != 0 {
			return false
		}
	}
	return true
}
