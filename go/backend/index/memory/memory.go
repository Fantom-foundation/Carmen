package memory

import "github.com/Fantom-foundation/Carmen/go/common"

type Memory[K comparable] struct {
	data map[K]uint64
}

func NewMemory[K comparable]() *Memory[K] {
	memory := Memory[K]{
		data: make(map[K]uint64),
	}
	return &memory
}

func (m *Memory[K]) GetOrAdd(key K) (uint64, error) {
	index, exists := m.data[key]
	if !exists {
		index = uint64(len(m.data))
		m.data[key] = index
	}
	return index, nil
}

func (m *Memory[K]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

func (m *Memory[K]) GetStateHash() common.Hash {
	return [32]byte{} // not implemented
}

func (m *Memory[K]) Close() error {
	return nil
}
