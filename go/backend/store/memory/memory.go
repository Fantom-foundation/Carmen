package memory

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is an in-memory store.Store implementation - it maps IDs to values
type Memory[V any] struct {
	data        [][]byte // data of pages [page][byte of page]
	hashTree    HashTree
	serializer  common.Serializer[V]
	pageSize    uint32 // the amount of items stored in one database page
	itemSize    int    // the amount of bytes per one value
	itemDefault V
}

// NewMemory constructs a new instance of Memory.
// It needs a serializer of data items and the default value for a not-set item.
func NewMemory[V any](serializer common.Serializer[V], itemDefault V, pageSize uint32, hashTreeFactor int) *Memory[V] {
	memory := Memory[V]{
		data:        [][]byte{make([]byte, 0, pageSize*uint32(serializer.Size()))},
		serializer:  serializer,
		pageSize:    pageSize,
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	memory.hashTree = NewHashTree(hashTreeFactor, &memory)
	return &memory
}

// itemPosition provides the position of an item in data pages
func (m *Memory[V]) itemPosition(id uint32) (page int, position int) {
	return int(id / m.pageSize), int(id%m.pageSize) * m.serializer.Size()
}

func (m *Memory[V]) GetPage(page int) ([]byte, error) {
	return m.data[page], nil
}

// Set a value of an item
func (m *Memory[V]) Set(id uint32, value V) error {
	page, itemPosition := m.itemPosition(id)
	for page >= len(m.data) {
		m.data = append(m.data, make([]byte, m.pageSize*uint32(m.itemSize)))
	}
	copy(m.data[page][itemPosition:itemPosition+m.itemSize], m.serializer.ToBytes(value))
	m.hashTree.MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Memory[V]) Get(id uint32) (V, error) {
	page, itemPosition := m.itemPosition(id)
	item := m.itemDefault
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+m.itemSize])
	}
	return item, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Memory[V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Close the store
func (m *Memory[V]) Close() error {
	return nil // no-op for in-memory database
}
