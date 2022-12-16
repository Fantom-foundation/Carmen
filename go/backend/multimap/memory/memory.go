package memory

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// MultiMap is an in-memory multimap.MultiMap implementation - it maps IDs to values
type MultiMap[K common.Identifier, V common.Identifier] struct {
	data map[K]map[V]bool
}

func NewMultiMap[K common.Identifier, V common.Identifier]() *MultiMap[K, V] {
	return &MultiMap[K, V]{
		data: make(map[K]map[V]bool),
	}
}

// Add adds the given key/value pair.
func (m *MultiMap[K, V]) Add(key K, value V) error {
	set, exists := m.data[key]
	if !exists {
		set = make(map[V]bool)
		m.data[key] = set
	}
	set[value] = true
	return nil
}

// Remove removes a single key/value entry.
func (m *MultiMap[K, V]) Remove(key K, value V) error {
	set, exists := m.data[key]
	if !exists {
		return nil
	}
	delete(set, value)
	return nil
}

// RemoveAll removes all entries with the given key.
func (m *MultiMap[K, V]) RemoveAll(key K) error {
	delete(m.data, key)
	return nil
}

// ForEach applies the given operation on each value associated to the given key.
func (m *MultiMap[K, V]) ForEach(key K, callback func(V)) error {
	set, exists := m.data[key]
	if !exists {
		return nil
	}
	for value := range set {
		callback(value)
	}
	return nil
}

// Flush the store
func (m *MultiMap[K, V]) Flush() error {
	return nil // no-op for in-memory database
}

// Close the store
func (m *MultiMap[K, V]) Close() error {
	return nil // no-op for in-memory database
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *MultiMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	var k K
	var v V
	size := unsafe.Sizeof(*m)
	for _, d := range m.data {
		size += unsafe.Sizeof(k) + uintptr(len(d))*unsafe.Sizeof(v)
	}
	return common.NewMemoryFootprint(size)
}
