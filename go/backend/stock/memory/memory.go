package memory

import (
	"encoding/json"
	"fmt"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// inMemoryStock provides an in-memory implementation of the stock.Stock interface.
type inMemoryStock[I stock.Index, V any] struct {
	values    []V
	freeList  []I
	directory string
	encoder   stock.ValueEncoder[V]
}

func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	res := &inMemoryStock[I, V]{
		values:    make([]V, 0, 10),
		freeList:  make([]I, 0, 10),
		directory: directory,
		encoder:   encoder,
	}

	// Test whether a meta file exists in this directory.
	metafile := directory + "/meta.json"
	if _, err := os.Stat(metafile); err != nil {
		return res, nil
	}

	// If there are files in the directory, load the data.
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

	// Load list of values.
	{
		valuefile := directory + "/values.dat"
		stats, err := os.Stat(valuefile)
		if err != nil {
			return nil, err
		}
		if got, want := stats.Size(), int64(meta.ValueListLength*valueSize); got != want {
			return nil, fmt.Errorf("invalid value file size, got %d, wanted %d", got, want)
		}
		res.values = make([]V, meta.ValueListLength)
		buffer := make([]byte, valueSize)
		file, err := os.Open(valuefile)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		for i := 0; i < meta.ValueListLength; i++ {
			_, err := file.Read(buffer)
			if err != nil {
				return nil, err
			}
			res.values[i], err = encoder.Load(buffer)
			if err != nil {
				return nil, err
			}
		}
	}

	// Load freelist.
	{
		freelistfile := directory + "/freelist.dat"
		stats, err := os.Stat(freelistfile)
		if err != nil {
			return nil, err
		}
		if got, want := stats.Size(), int64(meta.FreeListLength*indexSize); got != want {
			return nil, fmt.Errorf("invalid free-list file size, got %d, wanted %d", got, want)
		}
		res.freeList = make([]I, meta.FreeListLength)
		buffer := make([]byte, indexSize)
		file, err := os.Open(freelistfile)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		for i := 0; i < meta.FreeListLength; i++ {
			_, err := file.Read(buffer)
			if err != nil {
				return nil, err
			}
			res.freeList[i] = stock.DecodeIndex[I](buffer)
		}
	}

	return res, nil
}

func (s *inMemoryStock[I, V]) New() (I, *V, error) {
	lenValues := len(s.values)
	index := I(lenValues)

	// Reuse free index positions or grow list of values.
	lenFreeList := len(s.freeList)
	if lenFreeList > 0 {
		index = s.freeList[lenFreeList-1]
		s.freeList = s.freeList[0 : lenFreeList-1]
	} else {
		var value V
		s.values = append(s.values, value)
	}

	return I(index), &s.values[index], nil
}

func (s *inMemoryStock[I, V]) Get(index I) (*V, error) {
	if index >= I(len(s.values)) || index < 0 {
		return nil, nil
	}
	return &s.values[index], nil
}

func (s *inMemoryStock[I, V]) Set(index I, value *V) error {
	if index >= I(len(s.values)) || index < 0 {
		return fmt.Errorf("invalid index")
	}
	trg := &s.values[index]
	if value != trg {
		*trg = *value
	}
	return nil
}

func (s *inMemoryStock[I, V]) Delete(index I) error {
	s.freeList = append(s.freeList, index)
	return nil
}

func (s *inMemoryStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	indexSize := unsafe.Sizeof(I(0))
	valueSize := unsafe.Sizeof(s.values[0])
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("values", common.NewMemoryFootprint(valueSize*uintptr(cap(s.values))))
	res.AddChild("freelist", common.NewMemoryFootprint(indexSize*uintptr(cap(s.freeList))))
	return res
}

func (s *inMemoryStock[I, V]) Flush() error {
	// Write metadata.
	var index I
	indexSize := int(unsafe.Sizeof(index))
	metadata, err := json.Marshal(metadata{
		Version:         dataFormatVersion,
		IndexTypeSize:   indexSize,
		ValueTypeSize:   s.encoder.GetEncodedSize(),
		ValueListLength: len(s.values),
		FreeListLength:  len(s.freeList),
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.directory+"/meta.json", metadata, 0600); err != nil {
		return nil
	}

	// Write list of values.
	if f, err := os.Create(s.directory + "/values.dat"); err != nil {
		return err
	} else {
		defer f.Close()

		buffer := make([]byte, s.encoder.GetEncodedSize())
		for _, v := range s.values {
			s.encoder.Store(buffer, &v)
			_, err := f.Write(buffer)
			if err != nil {
				return err
			}
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	// Write free list.
	if f, err := os.Create(s.directory + "/freelist.dat"); err != nil {
		return err
	} else {
		defer f.Close()

		buffer := make([]byte, indexSize)
		for _, i := range s.freeList {
			stock.EncodeIndex(i, buffer)
			if _, err := f.Write(buffer); err != nil {
				return err
			}
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (s *inMemoryStock[I, V]) Close() error {
	return s.Flush()
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
