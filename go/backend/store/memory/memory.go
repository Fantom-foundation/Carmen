package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is in-memory Store implementations - it maps IDs to values
type Memory[V common.Serializable] struct {
	data        []byte
	notExisting V
	itemSize    int
}

func NewMemory[V common.Serializable](notExisting V) *Memory[V] {
	var helper V
	memory := Memory[V]{
		data:        make([]byte, 0),
		notExisting: notExisting,
		itemSize:    helper.Size(),
	}
	return &memory
}

func (m *Memory[V]) itemStart(id uint64) int {
	return int(id) * m.itemSize
}

func (m *Memory[V]) Set(id uint64, value V) error {
	itemStart := m.itemStart(id)
	if itemStart == len(m.data) {
		m.data = append(m.data, value.ToBytes()...)
		return nil
	}
	if itemStart > len(m.data) {
		return fmt.Errorf("index too high")
	}
	copy(m.data[itemStart:itemStart+m.itemSize], value.ToBytes())
	return nil
}

func (m *Memory[V]) Get(id uint64, itemToOverride V) bool {
	itemStart := m.itemStart(id)
	if itemStart < len(m.data) {
		itemToOverride.SetBytes(m.data[itemStart : itemStart+m.itemSize])
		return true
	} else {
		return false
	}
}

func (m *Memory[V]) GetStateHash() common.Hash {
	return [32]byte{} // not implemented
}

func (m *Memory[V]) Close() error {
	return nil
}
