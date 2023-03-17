package pagedfile

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/array/pagedarray"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"unsafe"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[I common.Identifier, V any] struct {
	array    *pagedarray.Array[I, V]
	hashTree hashtree.HashTree
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], pageSize int64, hashtreeFactory hashtree.Factory, poolSize int) (*Store[I, V], error) {
	// create directory structure
	err := os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, fmt.Errorf("failed to create hashes directory; %s", err)
	}

	arr, err := pagedarray.NewArray[I, V](path, serializer, pageSize, poolSize)
	if err != nil {
		return nil, err
	}
	hashTree := hashtreeFactory.Create(arr)
	arr.SetOnDirtyPageCallback(func(pageId int) {
		hashTree.MarkUpdated(pageId)
	})
	return &Store[I, V]{
		array:    arr,
		hashTree: hashTree,
	}, nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	return m.array.Set(id, value)
}

// Get a value of the item (or a zero value, if not defined)
func (m *Store[I, V]) Get(id I) (value V, err error) {
	return m.array.Get(id)
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (hash common.Hash, err error) {
	// flush propagates dirty pages from the pagepool
	if err := m.array.Flush(); err != nil {
		return hash, err
	}
	// update the hashTree and get the hash
	return m.hashTree.HashRoot()
}

// Flush all changes to the disk
func (m *Store[I, V]) Flush() (err error) {
	// flush dirty pages and update the hashTree
	if _, err = m.GetStateHash(); err != nil {
		return err
	}
	// flush data file changes to disk
	if err = m.array.Flush(); err != nil {
		return err
	}
	return nil
}

// Close the store
func (m *Store[I, V]) Close() (err error) {
	if err = m.Flush(); err != nil {
		return err
	}
	return m.array.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.AddChild("array", m.array.GetMemoryFootprint())
	return mf
}
