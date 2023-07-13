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
			expectedSize := meta.NumValuesInFile * int64(valueSize)
			if expectedSize%bufferSize != 0 {
				expectedSize += bufferSize - expectedSize%bufferSize
			}
			if expectedSize == 0 {
				expectedSize = bufferSize
			}
			if got, want := stats.Size(), expectedSize; got != want {
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
	buffer := make([]byte, valueSize)
	err := s.values.read(offset, buffer)
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

	// Write a serialized form of the value to disk.
	offset := int64(valueSize) * int64(index)
	if err := s.values.write(offset, buffer); err != nil {
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

const bufferSize = 4092

// file is a simple wrapper arround *os.File coordinating seek, read, and write
// operations.
// It has been observed that data is frequently accessed sequentially
// in stocks when being used by state tries (especially during initialization),
// and that many seek operations can be skipped because the current file pointer
// is actually pointing to the desired location. This class filters out unnecessary
// seek operations.
// The wrapper also adds a write buffer to combine multiple updates into a single
// file write update which -- in particular during priming -- can reduce processing
// time significantly.
// TODO: move this into an extra file and add unit tests.
type file struct {
	file         *os.File         // the file handle to represent
	filesize     int64            // the current size of the file
	position     int64            // the current position in the file
	buffer       [bufferSize]byte // a buffer for write operations
	bufferOffset int64            // the offset of the write buffer
}

func openFile(path string) (*file, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	stats, err := os.Stat(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	size := stats.Size()
	if size%bufferSize != 0 {
		f.Close()
		return nil, fmt.Errorf("invalid file size, got %d, expected multiple of %d", size, bufferSize)
	}
	res := &file{
		file:     f,
		filesize: size,
	}

	if err := res.readInternal(0, res.buffer[:]); err != nil {
		f.Close()
		return nil, err
	}

	return res, nil
}

func (f *file) write(position int64, src []byte) error {

	if len(src) > bufferSize {
		panic(fmt.Sprintf("writing data > %d bytes not supported so far, got %d", bufferSize, len(src)))
	}

	// If the data to be written covers multiple buffer blocks the write needs to be
	// split in two.
	if position/bufferSize != (position+int64(len(src))-1)/bufferSize {
		covered := bufferSize - position%bufferSize
		offsetB := f.bufferOffset + bufferSize
		return errors.Join(
			f.write(position, src[0:covered]),
			f.write(offsetB, src[covered:]),
		)
	}

	// Check whether the write operation targets the internal buffer.
	if position >= f.bufferOffset && position+int64(len(src)) <= f.bufferOffset+int64(len(f.buffer)) {
		copy(f.buffer[int(position-f.bufferOffset):], src)
		return nil
	}

	// Flush the buffer and load another.
	if err := f.writeInternal(f.bufferOffset, f.buffer[:]); err != nil {
		return err
	}
	newOffset := position - position%bufferSize
	if err := f.readInternal(newOffset, f.buffer[:]); err != nil {
		return err
	}
	f.bufferOffset = newOffset
	return f.write(position, src)
}

func (f *file) writeInternal(position int64, src []byte) error {
	// Grow file if required.
	if f.filesize < position {
		data := make([]byte, int(position-f.filesize))
		copy(data[int(position-f.filesize):], src)
		if err := f.writeInternal(f.filesize, data); err != nil {
			return err
		}
	}

	if err := f.seek(position); err != nil {
		return err
	}
	n, err := f.file.Write(src)
	if err != nil {
		return err
	}
	if n != len(src) {
		return fmt.Errorf("failed to write sufficient bytes to file, wanted %d, got %d", len(src), n)
	}
	f.position += int64(n)
	if f.position > f.filesize {
		f.filesize = f.position
	}
	return nil
}

func (f *file) read(position int64, dst []byte) error {
	// Read data from buffer if covered.
	if position >= f.bufferOffset && position+int64(len(dst)) <= f.bufferOffset+int64(len(f.buffer)) {
		copy(dst, f.buffer[int(position-f.bufferOffset):])
		return nil
	}
	return f.readInternal(position, dst)
}

func (f *file) readInternal(position int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	// If read segment exceeds the current file size, read covered part and pad with zeros.
	if position >= f.filesize {
		for i := range dst {
			dst[i] = 0
		}
		return nil
	}
	if position+int64(len(dst)) > f.filesize {
		covered := f.filesize - position
		if err := f.readInternal(position, dst[0:covered]); err != nil {
			return err
		}
		for i := covered; i < int64(len(dst)); i++ {
			dst[i] = 0
		}
		return nil
	}
	if err := f.seek(position); err != nil {
		return err
	}
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

func (f *file) flush() error {
	return errors.Join(
		f.writeInternal(f.bufferOffset, f.buffer[:]),
		f.file.Sync(),
	)
}

func (f *file) close() error {
	return f.file.Close()
}
