package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// PageSize is the amount of items stored in one database page
const PageSize = 32

// Memory is in-memory Store implementations - it maps IDs to values
type Memory[V any] struct {
	data        [][]byte // data of pages [page][byte of page]
	hashTree    HashTree
	serializer  common.Serializer[V]
	itemSize    int
	itemDefault V
}

func NewMemory[V any](serializer common.Serializer[V], itemDefault V) *Memory[V] {
	memory := Memory[V]{
		data:        [][]byte{make([]byte, 0, PageSize*serializer.Size())},
		serializer:  serializer,
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	memory.hashTree = NewHashTree(func(page int) []byte {
		return memory.data[page]
	})
	return &memory
}

func (m *Memory[V]) itemPosition(id uint64) (page int, position int) {
	return int(id / PageSize), int(id%PageSize) * m.serializer.Size()
}

func (m *Memory[V]) Set(id uint64, value V) error {
	page, itemPosition := m.itemPosition(id)
	if page == len(m.data) {
		m.data = append(m.data, []byte{})
	}
	if page > len(m.data) {
		return fmt.Errorf("index too high")
	}
	if itemPosition == len(m.data[page]) {
		m.data[page] = append(m.data[page], m.serializer.ToBytes(value)...)
		return nil
	}
	if itemPosition > len(m.data[page]) {
		return fmt.Errorf("index too high")
	}
	copy(m.data[page][itemPosition:itemPosition+m.itemSize], m.serializer.ToBytes(value))
	return nil
}

func (m *Memory[V]) Get(id uint64) *V {
	page, itemPosition := m.itemPosition(id)
	item := m.itemDefault
	if page < len(m.data) && itemPosition+m.itemSize < len(m.data[page]) {
		item = m.serializer.SetBytes(m.data[page][itemPosition : itemPosition+m.itemSize])
	}
	return &item
}

func (m *Memory[V]) GetStateHash() common.Hash {
	m.hashTree.Commit()
	return m.hashTree.GetHash()
}

func (m *Memory[V]) Close() error {
	return nil
}
