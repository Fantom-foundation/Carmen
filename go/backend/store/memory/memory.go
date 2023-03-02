package memory

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Store is an in-memory store.Store implementation - it maps IDs to values
type Store[I common.Identifier, V any] struct {
	data           [][]byte // data of pages [page][byte of page]
	hashTree       hashtree.HashTree
	serializer     common.Serializer[V]
	pageSize       int // the amount of bytes of one page
	pageItems      int // the amount of items stored in one page
	hashedPageSize int // the amount of the page bytes to be passed into the hashing function - rounded to whole items
	itemSize       int // the amount of bytes per one value
}

// NewStore constructs a new instance of Store.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](serializer common.Serializer[V], pageSize int, hashtreeFactory hashtree.Factory) (*Store[I, V], error) {
	if pageSize < serializer.Size() {
		return nil, fmt.Errorf("memory store pageSize too small (minimum %d)", serializer.Size())
	}

	itemSize := serializer.Size()
	memory := &Store[I, V]{
		data:           [][]byte{},
		serializer:     serializer,
		pageSize:       pageSize,
		pageItems:      pageSize / itemSize,
		hashedPageSize: pageSize / itemSize * itemSize,
		itemSize:       itemSize,
	}
	memory.hashTree = hashtreeFactory.Create(memory)
	return memory, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	// casting to I for division in proper bit width
	return int(id / I(m.pageItems)), (int64(id) % int64(m.pageItems)) * int64(m.itemSize)
}

func (m *Store[I, V]) GetPage(page int) ([]byte, error) {
	return m.data[page][0:m.hashedPageSize], nil
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
func (m *Store[I, V]) Get(id I) (item V, err error) {
	page, itemPosition := m.itemPosition(id)
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+int64(m.itemSize)])
	}
	return item, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Flush the store
func (m *Store[I, V]) Flush() error {
	return nil // no-op for in-memory database
}

// Close the store
func (m *Store[I, V]) Close() error {
	return nil // no-op for in-memory database
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	dataSize := uintptr(len(m.data) * m.pageSize)
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m) + dataSize)
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	return mf
}
