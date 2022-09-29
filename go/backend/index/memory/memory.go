package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is an in-memory implementation of index.Index.
type Memory[K comparable] struct {
	data       map[K]uint32
	hash       []byte
	serializer common.Serializer[K]
	hashIndex  *index.HashIndex[K]
}

// NewMemory constructs a new Memory instance.
func NewMemory[K comparable](serializer common.Serializer[K]) *Memory[K] {
	memory := Memory[K]{
		data:       make(map[K]uint32),
		hash:       []byte{},
		serializer: serializer,
		hashIndex:  index.NewHashIndex[K](serializer),
	}
	return &memory
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Memory[K]) GetOrAdd(key K) (uint32, error) {
	idx, exists := m.data[key]
	if !exists {
		idx = uint32(len(m.data))
		m.data[key] = idx
		m.hashIndex.AddKey(key)
	}
	return idx, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Memory[K]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

// GetStateHash returns the index hash.
func (m *Memory[K]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Memory[K]) Close() error {
	return nil
}
