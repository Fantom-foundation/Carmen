package memory

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type Memory[K common.Serializable] struct {
	data map[K]uint64
	hash common.Hash
}

func NewMemory[K common.Serializable]() *Memory[K] {
	memory := Memory[K]{
		data: make(map[K]uint64),
		hash: common.Hash{},
	}
	return &memory
}

func (m *Memory[K]) GetOrAdd(key K) (uint64, error) {
	index, exists := m.data[key]
	if !exists {
		index = uint64(len(m.data))
		m.data[key] = index
		m.hashKey(key) // recursive hash for each new key
	}
	return index, nil
}

func (m *Memory[K]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

func (m *Memory[K]) GetStateHash() common.Hash {
	return m.hash
}

func (m *Memory[K]) Close() error {
	return nil
}

func (m *Memory[K]) hashKey(key K) {
	h := sha256.New()
	h.Write(m.hash.Bytes())
	h.Write(key.ToBytes())
	// TODO measure performance of copying byte array every time
	m.hash = common.BytesToHash(h.Sum(nil))
}
