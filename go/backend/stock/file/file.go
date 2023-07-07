package file

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type fileStock[I stock.Index, V any] struct {
	directory     string
	encoder       stock.ValueEncoder[V]
	values        *os.File
	freelist      *fileBasedStack[I]
	numValueSlots I
}

func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	metafile := directory + "/meta.json"
	valuefile := directory + "/values.dat"
	freelistfile := directory + "/freelist.dat"
	numValueSlots := I(0)

	// Create the direcory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	// If there is a meta-file in the directory, check its content.
	if _, err := os.Stat(metafile); err == nil {
		data, err := os.ReadFile(metafile)
		if err != nil {
			return nil, err
		}

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
			if got, want := stats.Size(), int64(meta.ValueListLength*valueSize); got != want {
				return nil, fmt.Errorf("invalid value file size, got %d, wanted %d", got, want)
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
	}

	values, err := os.OpenFile(valuefile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	freelist, err := openFileBasedStack[I](freelistfile)
	if err != nil {
		return nil, err
	}

	// Create new files
	return &fileStock[I, V]{
		encoder:       encoder,
		directory:     directory,
		values:        values,
		freelist:      freelist,
		numValueSlots: numValueSlots,
	}, nil
}

func (s *fileStock[I, V]) New() (I, *V, error) {
	index := s.numValueSlots

	// Reuse free index positions or grow list of values.
	if !s.freelist.Empty() {
		free, err := s.freelist.Pop()
		if err != nil {
			return 0, nil, err
		}
		index = free
	} else {
		s.numValueSlots++

		// Add zero bytes to the end of the file.
		if _, err := s.values.Seek(0, 2); err != nil {
			return 0, nil, err
		}

		zeros := make([]byte, s.encoder.GetEncodedSize())
		if _, err := s.values.Write(zeros); err != nil {
			return 0, nil, err
		}
	}

	var value V
	return I(index), &value, nil
}

func (s *fileStock[I, V]) Get(index I) (*V, error) {
	if index >= s.numValueSlots || index < 0 {
		return nil, nil
	}
	// Load value from the file.
	valueSize := s.encoder.GetEncodedSize()
	offset := int64(valueSize) * int64(index)
	if _, err := s.values.Seek(offset, 0); err != nil {
		return nil, err
	}

	buffer := make([]byte, valueSize)
	_, err := s.values.Read(buffer)
	if err != nil {
		return nil, err
	}

	res := new(V)
	if err := s.encoder.Load(buffer, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *fileStock[I, V]) Set(index I, value *V) error {
	if index >= s.numValueSlots || index < 0 {
		return fmt.Errorf("index out of range, got %d, range [0,%d)", index, s.numValueSlots)
	}

	// Write a serialized form of the value to disk.
	valueSize := s.encoder.GetEncodedSize()
	offset := int64(valueSize) * int64(index)
	pos, err := s.values.Seek(offset, 0)
	if err != nil {
		return err
	}
	if pos != offset {
		return fmt.Errorf("failed to seek to required position, wanted %d, got %d", offset, pos)
	}

	buffer := make([]byte, valueSize)
	s.encoder.Store(buffer, value)
	n, err := s.values.Write(buffer)
	if err != nil {
		return err
	}
	if n != valueSize {
		return fmt.Errorf("failed to write sufficient bytes to file, wanted %d, got %d", valueSize, n)
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
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.directory+"/meta.json", metadata, 0600); err != nil {
		return nil
	}

	// Flush freelist and value file.
	return errors.Join(
		s.freelist.Flush(),
		s.values.Sync(),
	)
}

func (s *fileStock[I, V]) Close() error {
	// Flush is executed before the closing of the file since
	// in Go the evaluation order of arguments is fixed.
	// see: https://go.dev/ref/spec#Order_of_evaluation
	return errors.Join(
		s.Flush(),
		s.values.Close(),
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
}
