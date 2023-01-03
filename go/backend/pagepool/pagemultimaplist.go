package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// PageMultiMapList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
// This implementation is a MultiMap api on top of PageList
type PageMultiMapList[K comparable, V comparable] struct {
	delegate *PageList[K, V]
}

// NewPageMultiMapList creates a new instance, each block will have the given maximal capacity.
func NewPageMultiMapList[K comparable, V comparable](bucket, pageItems int, pagePool *PagePool[*KVPage[K, V]]) *PageMultiMapList[K, V] {
	return &PageMultiMapList[K, V]{
		delegate: NewPageList[K, V](bucket, pageItems, pagePool),
	}
}

// InitPageMultiMapList creates a new instance, each block will have the given maximal capacity.
func InitPageMultiMapList[K comparable, V comparable](bucket, pageItems int, pagePool *PagePool[*KVPage[K, V]], data []common.MapEntry[K, V]) (*PageMultiMapList[K, V], error) {
	delegate := NewPageList[K, V](bucket, pageItems, pagePool)
	p := &PageMultiMapList[K, V]{
		delegate: delegate,
	}

	err := delegate.bulkInsert(data)
	return p, err
}

func (m *PageMultiMapList[K, V]) ForEach(callback func(K, V)) error {
	return m.delegate.forEach(callback)
}

func (m *PageMultiMapList[K, V]) Get(key K) (val V, exists bool, err error) {
	return m.delegate.get(key)
}

func (m *PageMultiMapList[K, V]) Add(key K, val V) error {
	_, _, err := m.delegate.addOrPut(key, val, add)
	return err
}

// GetEntries collects data from all blocks and returns them as one slice
func (m *PageMultiMapList[K, V]) GetEntries() ([]common.MapEntry[K, V], error) {
	return m.delegate.getEntries()
}

func (m *PageMultiMapList[K, V]) GetAll(key K) ([]V, error) {
	return m.delegate.getAll(key)
}

func (m *PageMultiMapList[K, V]) Remove(key K, val V) (bool, error) {
	return m.delegate.remove(key, &val, removeVal)
}

func (m *PageMultiMapList[K, V]) RemoveAll(key K) error {
	_, err := m.delegate.remove(key, nil, removeAll)
	return err
}

func (m *PageMultiMapList[K, V]) Size() int {
	return m.delegate.size()
}

func (m *PageMultiMapList[K, V]) Clear() error {
	return m.delegate.clear()
}

func (m *PageMultiMapList[K, V]) PrintDump() {
	m.delegate.printDump()
}

func (m *PageMultiMapList[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	return m.delegate.GetMemoryFootprint()
}
