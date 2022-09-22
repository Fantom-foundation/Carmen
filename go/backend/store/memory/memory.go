package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is in-memory Store implementations - it maps IDs to values
type Memory[V any] struct {
	data        []byte
	hashTree    HashTree
	serializer  common.Serializer[V]
	itemSize    int
	itemDefault V
}

func NewMemory[V any](serializer common.Serializer[V], itemDefault V) *Memory[V] {
	memory := Memory[V]{
		data:        make([]byte, 0),
		serializer:  serializer,
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	return &memory
}

func (m *Memory[V]) itemStart(id uint64) int {
	return int(id) * m.serializer.Size()
}

func (m *Memory[V]) Set(id uint64, value V) error {
	itemStart := m.itemStart(id)
	if itemStart == len(m.data) {
		m.data = append(m.data, m.serializer.ToBytes(value)...)
		return nil
	}
	if itemStart > len(m.data) {
		return fmt.Errorf("index too high")
	}
	copy(m.data[itemStart:itemStart+m.itemSize], m.serializer.ToBytes(value))
	return nil
}

func (m *Memory[V]) Get(id uint64) *V {
	itemStart := m.itemStart(id)
	item := m.itemDefault
	if itemStart+m.itemSize < len(m.data) {
		item = m.serializer.SetBytes(m.data[itemStart : itemStart+m.itemSize])
	}
	return &item
}

func (m *Memory[V]) GetStateHash() common.Hash {
	return [32]byte{} // not implemented
}

func (m *Memory[V]) Close() error {
	return nil
}
