package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Store is an in-memory store.Store implementation - it maps IDs to values
type Store[I common.Identifier, V any] struct {
	data            [][]byte // data of pages [page][byte of page]
	hashTree        hashtree.HashTree
	serializer      common.Serializer[V]
	pageSize        int // the amount of bytes of one page
	pageItems       int // the amount of items stored in one page
	itemSize        int // the amount of bytes per one value
	branchingFactor int
	itemDefault     V
}

// NewStore constructs a new instance of Store.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](serializer common.Serializer[V], itemDefault V, pageSize int, branchingFactor int) (*Store[I, V], error) {
	if pageSize < serializer.Size() {
		return nil, fmt.Errorf("memory store pageSize too small (minimum %d)", serializer.Size())
	}

	memory := &Store[I, V]{
		data:            [][]byte{},
		serializer:      serializer,
		pageSize:        pageSize,
		pageItems:       pageSize / serializer.Size(),
		itemSize:        serializer.Size(),
		branchingFactor: branchingFactor,
		itemDefault:     itemDefault,
	}
	memory.hashTree = CreateHashTreeFactory(branchingFactor).Create(memory)
	return memory, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	// casting to I for division in proper bit width
	return int(id / I(m.pageItems)), (int64(id) % int64(m.pageItems)) * int64(m.itemSize)
}

func (m *Store[I, V]) GetPage(page int) ([]byte, error) {
	return m.data[page], nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	page, itemPosition := m.itemPosition(id)
	for page >= len(m.data) {
		m.data = append(m.data, make([]byte, m.pageSize))
	}
	copy(m.data[page][itemPosition:itemPosition+int64(m.itemSize)], m.serializer.ToBytes(value))
	m.hashTree.MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (V, error) {
	page, itemPosition := m.itemPosition(id)
	item := m.itemDefault
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+int64(m.itemSize)])
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
