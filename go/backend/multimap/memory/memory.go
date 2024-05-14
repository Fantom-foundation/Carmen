// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package memory

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// MultiMap is an in-memory multimap.MultiMap implementation - it maps IDs to values
type MultiMap[K comparable, V comparable] struct {
	data map[K]map[V]bool
}

func NewMultiMap[K comparable, V comparable]() *MultiMap[K, V] {
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

// GetAll provides all values associated with the given key.
func (m *MultiMap[K, V]) GetAll(key K) ([]V, error) {
	set, exists := m.data[key]
	if !exists {
		return nil, nil
	}
	values := make([]V, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	return values, nil
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
