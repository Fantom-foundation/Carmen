package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type Memory[K comparable] struct {
	data       map[K]uint64
	hash       []byte
	serializer common.Serializer[K]
	hashIndex  *index.HashIndex[K]
}

func NewMemory[K comparable](serializer common.Serializer[K]) *Memory[K] {
	memory := Memory[K]{
		data:       make(map[K]uint64),
		hash:       []byte{},
		serializer: serializer,
		hashIndex:  index.NewHashIndex[K](serializer),
	}
	return &memory
}

func (m *Memory[K]) GetOrAdd(key K) (uint64, error) {
	idx, exists := m.data[key]
	if !exists {
		idx = uint64(len(m.data))
		m.data[key] = idx
		m.hashIndex.AddKey(key)
	}
	return idx, nil
}

func (m *Memory[K]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

func (m *Memory[K]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

func (m *Memory[K]) Close() error {
	return nil
}
