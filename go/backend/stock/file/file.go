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
	directory       string
	encoder         stock.ValueEncoder[V]
	values          *file
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
			if got, want := stats.Size(), meta.NumValuesInFile*int64(valueSize); got != want {
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
		numValuesInFile = meta.NumValuesInFile
	}

	values, err := openFile(valuefile)
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
	}

	var value V
	return I(index), &value, nil
}

func (s *fileStock[I, V]) Get(index I) (*V, error) {
	if index >= I(s.numValueSlots) || index < 0 {
		return nil, nil
	}
	if index >= I(s.numValuesInFile) {
		return new(V), nil
	}
	// Load value from the file.
	valueSize := s.encoder.GetEncodedSize()
	offset := int64(valueSize) * int64(index)
	if err := s.values.seek(offset); err != nil {
		return nil, err
	}

	buffer := make([]byte, valueSize)
	err := s.values.read(buffer)
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

	// Encode the value to be written.
	valueSize := s.encoder.GetEncodedSize()
	buffer := make([]byte, valueSize)
	s.encoder.Store(buffer, value)

	// If the new data is beyond the end of the current file and empty, we can skip
	// the write operation.
	if index >= I(s.numValuesInFile) && allZero(buffer) {
		return nil
	}

	// Grow the file if needed.
	if index >= I(s.numValuesInFile) {
		data := make([]byte, int((int64(index)-s.numValuesInFile+1)*int64(valueSize)))
		copy(data[int(int64(valueSize)*(int64(index)-s.numValuesInFile)):], buffer)
		if err := s.values.seek(s.numValuesInFile * int64(valueSize)); err != nil {
			return err
		}
		if err := s.values.write(data); err != nil {
			return err
		}
		s.numValuesInFile = int64(index) + 1
		return nil
	}

	// Write a serialized form of the value to disk.
	offset := int64(valueSize) * int64(index)
	if err := s.values.seek(offset); err != nil {
		return err
	}
	if err := s.values.write(buffer); err != nil {
		return err
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
		s.values.flush(),
		s.freelist.Flush(),
	)
}

func (s *fileStock[I, V]) Close() error {
	// Flush is executed before the closing of the file since
	// in Go the evaluation order of arguments is fixed.
	// see: https://go.dev/ref/spec#Order_of_evaluation
	return errors.Join(
		s.Flush(),
		s.values.close(),
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

// file is a simple wrapper arround *os.File coordinating seek, read, and write
// operations.

// It has been observed that data is frequently accessed sequentially
// in stocks when being used by state tries (especially during initialization),
// and that many seek operations can be skipped because the current file pointer
// is actually pointing to the desired location. This class filters out unnecessary
// seek operations.
type file struct {
	file     *os.File // the file handle to represent
	position int64    // the current position in the file
}

func openFile(path string) (*file, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &file{file: f}, nil
}

func (f *file) seek(position int64) error {
	if f.position == position {
		return nil
	}
	pos, err := f.file.Seek(position, 0)
	if err != nil {
		return err
	}
	if pos != position {
		return fmt.Errorf("failed to seek to required position, wanted %d, got %d", position, pos)
	}
	f.position = position
	return nil
}

func (f *file) write(src []byte) error {
	n, err := f.file.Write(src)
	if err != nil {
		return err
	}
	if n != len(src) {
		return fmt.Errorf("failed to write sufficient bytes to file, wanted %d, got %d", len(src), n)
	}
	f.position += int64(n)
	return nil
}

func (f *file) read(dst []byte) error {
	n, err := f.file.Read(dst)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return fmt.Errorf("failed to read sufficient bytes from file, wanted %d, got %d", len(dst), n)
	}
	f.position += int64(n)
	return nil
}

func (f *file) flush() error {
	return f.file.Sync()
}

func (f *file) close() error {
	return f.file.Close()
}
