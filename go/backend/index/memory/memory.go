package memory

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is an in-memory implementation of index.Index.
type Memory[K comparable] struct {
	data       map[K]uint64
	hash       []byte
	serializer common.Serializer[K]
}

// NewMemory constructs a new Memory instance.
func NewMemory[K comparable](serializer common.Serializer[K]) *Memory[K] {
	memory := Memory[K]{
		data:       make(map[K]uint64),
		hash:       []byte{},
		serializer: serializer,
	}
	return &memory
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Memory[K]) GetOrAdd(key K) (uint64, error) {
	index, exists := m.data[key]
	if !exists {
		index = uint64(len(m.data))
		m.data[key] = index
		m.addKeyIntoHash(key) // recursive hash for each new key
	}
	return index, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Memory[K]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

// GetStateHash returns the index hash.
func (m *Memory[K]) GetStateHash() common.Hash {
	return common.HashSerializer{}.FromBytes(m.hash)
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Memory[K]) Close() error {
	return nil
}

// addKeyIntoHash appends a new key to the state hash
func (m *Memory[K]) addKeyIntoHash(key K) {
	h := sha256.New()
	h.Write(m.hash)
	h.Write(m.serializer.ToBytes(key))
	m.hash = h.Sum(nil)
}
