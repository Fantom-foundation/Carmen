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
	values        *os.File
	freelist      *fileBasedStack[I]
	encoder       stock.ValueEncoder[V]
	numValueSlots I
}

func OpenStock[I stock.Index, V any](encoder stock.ValueEncoder[V], directory string) (stock.Stock[I, V], error) {
	metafile := directory + "/meta.json"
	valuefile := directory + "/values.dat"
	freelistfile := directory + "/freelist.dat"
	numValueSlots := I(0)

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

	values, err := os.OpenFile(valuefile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	freelist, err := openFileBasedStack[I](freelistfile)
	if err != nil {
		return nil, err
	}

	// Create new files
	return &fileStock[I, V]{
		values:        values,
		freelist:      freelist,
		encoder:       encoder,
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
	}

	var value V
	return I(index), &value, nil
}

func (s *fileStock[I, V]) Get(index I) (*V, error) {
	if index >= s.numValueSlots || index < 0 {
		return nil, nil
	}
	// TODO: fetch the value from disk
	// TODO: figure out how to trigger disk write-back
	return nil, nil
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
	return fmt.Errorf("not implemented")
	/*
		// Write metadata.
		var index I
		indexSize := int(unsafe.Sizeof(index))
		metadata, err := json.Marshal(metadata{
			Version:         dataFormatVersion,
			IndexTypeSize:   indexSize,
			ValueTypeSize:   0, // TODO: fill in
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
	*/
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
