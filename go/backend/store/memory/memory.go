package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type Memory[V common.Serializable] struct {
	data        []V
	notExisting V
}

func NewMemory[V common.Serializable](notExisting V) *Memory[V] {
	memory := Memory[V]{
		data:        make([]V, 0),
		notExisting: notExisting,
	}
	return &memory
}

func (m *Memory[V]) Set(id uint64, value V) error {
	if id == uint64(len(m.data)) {
		m.data = append(m.data, value)
	}
	if id > uint64(len(m.data)) {
		return fmt.Errorf("index too high")
	}
	m.data[id] = value
	return nil
}

func (m *Memory[V]) Get(id uint64) V {
	if m.Contains(id) {
		return m.data[id]
	} else {
		return m.notExisting
	}
}

func (m *Memory[V]) Contains(id uint64) bool {
	return int(id) < len(m.data)
}

func (m *Memory[V]) GetStateHash() common.Hash {
	return [32]byte{} // not implemented
}

func (m *Memory[V]) Close() error {
	return nil
}
