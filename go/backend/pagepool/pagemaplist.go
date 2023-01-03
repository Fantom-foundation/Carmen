package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// PageMapList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
// This implementation is a Map api on top of PageList
type PageMapList[K comparable, V comparable] struct {
	delegate *PageList[K, V]
}

// NewPageMapList creates a new instance, each block will have the given maximal capacity.
func NewPageMapList[K comparable, V comparable](bucket, pageItems int, pagePool *PagePool[*KVPage[K, V]]) *PageMapList[K, V] {
	return &PageMapList[K, V]{
		delegate: NewPageList[K, V](bucket, pageItems, pagePool),
	}
}

// InitPageMapList creates a new instance, each block will have the given maximal capacity.
func InitPageMapList[K comparable, V comparable](bucket, pageItems int, pagePool *PagePool[*KVPage[K, V]], data []common.MapEntry[K, V]) (*PageMapList[K, V], error) {
	delegate := NewPageList[K, V](bucket, pageItems, pagePool)
	p := &PageMapList[K, V]{
		delegate: delegate,
	}
	err := delegate.bulkInsert(data)
	return p, err
}

func (m *PageMapList[K, V]) ForEach(callback func(K, V)) error {
	return m.delegate.forEach(callback)
}

func (m *PageMapList[K, V]) Get(key K) (val V, exists bool, err error) {
	return m.delegate.get(key)
}

func (m *PageMapList[K, V]) GetOrAdd(key K, val V) (V, bool, error) {
	return m.delegate.addOrPut(key, val, getOrAdd)
}

func (m *PageMapList[K, V]) Put(key K, val V) error {
	_, _, err := m.delegate.addOrPut(key, val, put)
	return err
}

func (m *PageMapList[K, V]) GetEntries() ([]common.MapEntry[K, V], error) {
	return m.delegate.getEntries()
}

func (m *PageMapList[K, V]) GetAll(key K) ([]V, error) {
	return m.delegate.getAll(key)
}

func (m *PageMapList[K, V]) Remove(key K) (exists bool, err error) {
	return m.delegate.remove(key, nil, remove)
}

func (m *PageMapList[K, V]) Size() int {
	return m.delegate.size()
}

func (m *PageMapList[K, V]) Clear() error {
	return m.delegate.clear()
}

func (m *PageMapList[K, V]) PrintDump() {
	m.delegate.printDump()
}

func (m *PageMapList[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	return m.delegate.GetMemoryFootprint()
}
