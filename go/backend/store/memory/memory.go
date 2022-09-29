package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Memory is an in-memory store.Store implementation - it maps IDs to values
type Memory[V any] struct {
	data            [][]byte // data of pages [page][byte of page]
	hashTreeFactory store.HashTreeFactory
	serializer      common.Serializer[V]
	pageSize        uint64 // the amount of items stored in one database page
	itemSize        int    // the amount of bytes per one value
	branchingFactor int
	itemDefault     V
}

// NewMemory constructs a new instance of Memory.
// It needs a serializer of data items and the default value for a not-set item.
func NewMemory[V any](serializer common.Serializer[V], itemDefault V, pageSize uint64, branchingFactor int) *Memory[V] {
	memory := Memory[V]{
		data:            [][]byte{make([]byte, 0, pageSize*uint64(serializer.Size()))},
		hashTreeFactory: CreateHashTreeFactory(branchingFactor),
		serializer:      serializer,
		pageSize:        pageSize,
		itemSize:        serializer.Size(),
		branchingFactor: branchingFactor,
		itemDefault:     itemDefault,
	}
	return &memory
}

// itemPosition provides the position of an item in data pages
func (m *Memory[V]) itemPosition(id uint64) (page int, position int) {
	return int(id / m.pageSize), int(id%m.pageSize) * m.serializer.Size()
}

func (m *Memory[V]) GetPage(page int) ([]byte, error) {
	return m.data[page], nil
}

// Set a value of an item
func (m *Memory[V]) Set(id uint64, value V) error {
	page, itemPosition := m.itemPosition(id)
	for page >= len(m.data) {
		m.data = append(m.data, make([]byte, m.pageSize*uint64(m.itemSize)))
	}
	copy(m.data[page][itemPosition:itemPosition+m.itemSize], m.serializer.ToBytes(value))
	m.hashTreeFactory.Create(m).MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Memory[V]) Get(id uint64) (V, error) {
	page, itemPosition := m.itemPosition(id)
	item := m.itemDefault
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+m.itemSize])
	}
	return item, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Memory[V]) GetStateHash() (common.Hash, error) {
	return m.hashTreeFactory.Create(m).HashRoot()
}

// Close the store
func (m *Memory[V]) Close() error {
	return nil // no-op for in-memory database
}
