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

// OpenStock opens a stock retained in the given directory. To that end, meta
// data is loaded and verified. A non-existing directory will be implicitly
// created and an empty directory is a valid target to be initialized as an
// empty stock.
func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	// Create the directory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	// Verify the content of the stock and get its metadata.
	meta, err := verifyStockInternal[I, V](encoder, directory)
	if err != nil {
		return nil, err
	}

	_, valuefile, freelistfile := getFileNames(directory)
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
		numValueSlots:   I(meta.ValueListLength),
		numValuesInFile: meta.NumValuesInFile,
	}, nil
}

// VerifyStock verifies the consistency of the meta-information maintained for the stock in
// the given directory. This includes
//   - checking the presence and size of all required files
//   - checking the correct metadata for a stock using the given index and encoder
//   - checking the value range of elements in the free-list
//
// For compatibility with the OpenStock function above, an empty directory is considered a
// valid stock as well.
func VerifyStock[I stock.Index, V any](directory string, encoder stock.ValueEncoder[V]) error {
	if !isDirectory(directory) {
		return fmt.Errorf("directory %v does not exist", directory)
	}
	_, err := verifyStockInternal[I, V](encoder, directory)
	return err
}

func verifyStockInternal[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (metadata, error) {
	var meta metadata

	// If none of the needed files exist, the stock is empty and thus consistent.
	metafile, valuefile, freelistfile := getFileNames(directory)
	if !exists(metafile) && !exists(valuefile) && !exists(freelistfile) {
		return meta, nil
	}

	// Missing files are a problem.
	for _, file := range []string{metafile, valuefile, freelistfile} {
		if !exists(file) {
			return meta, fmt.Errorf("required `%v` not found", file)
		}
	}

	// Attempt to parse the meta-data.
	data, err := os.ReadFile(metafile)
	if err != nil {
		return meta, err
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}

	// Check meta-data format information.
	if meta.Version != dataFormatVersion {
		return meta, fmt.Errorf("invalid file format version, got %d, wanted %d", meta.Version, dataFormatVersion)
	}
	indexSize := int(unsafe.Sizeof(I(0)))
	if meta.IndexTypeSize != indexSize {
		return meta, fmt.Errorf("invalid index type encoding, expected %d byte, found %d", indexSize, meta.IndexTypeSize)
	}
	valueSize := encoder.GetEncodedSize()
	if meta.ValueTypeSize != valueSize {
		return meta, fmt.Errorf("invalid value type encoding, expected %d byte, found %d", valueSize, meta.ValueTypeSize)
	}

	// Check size of the value file.
	{
		stats, err := os.Stat(valuefile)
		if err != nil {
			return meta, err
		}
		expectedSize := meta.NumValuesInFile * int64(valueSize)
		if got, want := stats.Size(), expectedSize; got < want {
			return meta, fmt.Errorf("insufficient value file size, got %d, wanted %d", got, want)
		}
	}

	// Check size of the free-list file.
	{
		stats, err := os.Stat(freelistfile)
		if err != nil {
			return meta, err
		}
		if got, want := stats.Size(), int64(meta.FreeListLength*indexSize); got != want {
			return meta, fmt.Errorf("invalid free-list file size, got %d, wanted %d", got, want)
		}
	}

	// Check the content of the free-list file.
	{
		stack, err := openFileBasedStack[I](freelistfile)
		if err != nil {
			return meta, err
		}
		defer stack.Close()
		list, err := stack.GetAll()
		if err != nil {
			return meta, err
		}
		for _, entry := range list {
			if entry < 0 || entry >= I(meta.ValueListLength) {
				return meta, fmt.Errorf("invalid value in free list: %d not in range [%d,%d)", entry, 0, meta.ValueListLength)
			}
		}
	}

	return meta, nil
}

func getFileNames(directory string) (metafile string, valuefile string, freelistfile string) {
	metafile = directory + "/meta.json"
	valuefile = directory + "/values.dat"
	freelistfile = directory + "/freelist.dat"
	return
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
	if index >= s.numValueSlots || index < 0 {
		return nil
	}
	return s.freelist.Push(index)
}

func (s *fileStock[I, V]) GetIds() (stock.IndexSet[I], error) {
	free, err := s.freelist.GetAll()
	if err != nil {
		return nil, err
	}
	res := stock.MakeComplementSet[I](0, s.numValueSlots)
	for _, i := range free {
		res.Remove(i)
	}
	return res, nil
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

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
