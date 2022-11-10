package index

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// Array wraps more instances of the index and delegates the calls to all of them.
// It replicates all operations to all indexes and throws an exception when returned values
// for indexes diverge. It is more for testing purposes at the moment
type Array[K comparable, I common.Identifier] struct {
	indexes []Index[K, I]
}

// NewIndexArray creates a new instance of the index backed
func NewIndexArray[K comparable, I common.Identifier]() *Array[K, I] {
	return &Array[K, I]{}
}

func (m *Array[K, I]) Add(index Index[K, I]) {
	m.indexes = append(m.indexes, index)
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Array[K, I]) GetOrAdd(key K) (I, error) {
	var res I
	for i, idx := range m.indexes {
		if r, err := idx.GetOrAdd(key); err != nil {
			return res, err
		} else {
			res = r
			if i > 0 && res != r {
				return res, fmt.Errorf("result of index %d does not match: %d != %d", i, res, r)
			}
		}
	}
	return res, nil
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists
func (m *Array[K, I]) Get(key K) (I, error) {
	var res I
	for i, idx := range m.indexes {
		if r, err := idx.Get(key); err != nil {
			return res, err
		} else {
			res = r
			if i > 0 && res != r {
				return res, fmt.Errorf("result of index %d does not match: %d != %d", i, res, r)
			}
		}
	}
	return res, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Array[K, I]) Contains(key K) bool {
	var res bool
	for i, idx := range m.indexes {
		r := idx.Contains(key)
		res = r
		if i > 0 && res != r {
			panic(fmt.Errorf("result of index %d does not match: %t != %t", i, res, r))
		}
	}
	return res
}

// GetStateHash returns the index hash.
func (m *Array[K, I]) GetStateHash() (common.Hash, error) {
	var res common.Hash
	for i, idx := range m.indexes {
		if r, err := idx.GetStateHash(); err != nil {
			return common.Hash{}, err
		} else {
			res = r
			if i > 0 && res != r {
				return common.Hash{}, fmt.Errorf("result of index %d does not match: %x != %x", i, res, r)
			}
		}
	}
	return res, nil
}

// Flush clean-ups all possible dirty values
func (m *Array[K, I]) Flush() error {
	var resErr error
	for _, idx := range m.indexes {
		if err := idx.Flush(); err != nil {
			resErr = err
		}
	}

	return resErr
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Array[K, I]) Close() error {
	var resErr error
	for _, idx := range m.indexes {
		if err := idx.Close(); err != nil {
			resErr = err
		}
	}

	return resErr
}

// GetMemoryFootprint provides the size of the index in memory in bytes
func (m *Array[K, I]) GetMemoryFootprint() uintptr {
	size := unsafe.Sizeof(*m)
	for _, index := range m.indexes {
		size += index.GetMemoryFootprint()
	}
	return size
}
