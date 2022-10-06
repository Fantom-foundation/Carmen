package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/hashindex"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is an in-memory implementation of index.Index.
type Memory[K comparable, I common.Identifier] struct {
	data          map[K]I
	keySerializer common.Serializer[K]
	hashIndex     *hashindex.HashIndex[K]
}

// NewMemory constructs a new Memory instance.
func NewMemory[K comparable, I common.Identifier](serializer common.Serializer[K]) index.Index[K, I] {
	memory := Memory[K, I]{
		data:          make(map[K]I),
		keySerializer: serializer,
		hashIndex:     hashindex.NewHashIndex[K](serializer),
	}
	return &memory
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Memory[K, I]) GetOrAdd(key K) (I, error) {
	idx, exists := m.data[key]
	if !exists {
		idx = I(len(m.data))
		m.data[key] = idx
		m.hashIndex.AddKey(key)
	}
	return idx, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Memory[K, I]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

// GetStateHash returns the index hash.
func (m *Memory[K, I]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Memory[K, I]) Close() error {
	return nil
}
