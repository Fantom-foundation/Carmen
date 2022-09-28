package memory

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Store is an in-memory store.Store implementation - it maps IDs to values
type Store[I common.Identifier, V any] struct {
	data        [][]byte // data of pages [page][byte of page]
	hashTree    HashTree
	serializer  common.Serializer[V]
	pageSize    int // the amount of items stored in one database page
	itemSize    int // the amount of bytes per one value
	itemDefault V
}

// NewStore constructs a new instance of Store.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](serializer common.Serializer[V], itemDefault V, pageSize int, hashTreeFactor int) *Store[I, V] {
	memory := Store[I, V]{
		data:        [][]byte{},
		serializer:  serializer,
		pageSize:    pageSize,
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	memory.hashTree = NewHashTree(hashTreeFactor, &memory)
	return &memory
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int) {
	return int(id) / m.pageSize, int(id) % m.pageSize * m.itemSize
}

func (m *Store[I, V]) GetPage(page int) ([]byte, error) {
	return m.data[page], nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	page, itemPosition := m.itemPosition(id)
	for page >= len(m.data) {
		m.data = append(m.data, make([]byte, m.pageSize*m.itemSize))
	}
	copy(m.data[page][itemPosition:itemPosition+m.itemSize], m.serializer.ToBytes(value))
	m.hashTree.MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (V, error) {
	page, itemPosition := m.itemPosition(id)
	item := m.itemDefault
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+m.itemSize])
	}
	return item, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Close the store
func (m *Store[I, V]) Close() error {
	return nil // no-op for in-memory database
}
