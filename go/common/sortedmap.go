// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import "unsafe"

// SortedMap implements a memory map for the key-value pairs.
// Its elements are sorted on insertion by the key.
type SortedMap[K comparable, V any] struct {
	list       []MapEntry[K, V]
	comparator Comparator[K]
	size       int
}

// NewSortedMap creates a new instance.
func NewSortedMap[K comparable, V any](capacity int, comparator Comparator[K]) *SortedMap[K, V] {
	list := make([]MapEntry[K, V], 0, capacity)
	return &SortedMap[K, V]{
		list:       list,
		comparator: comparator,
	}
}

// InitSortedMap creates a new instance with input data
func InitSortedMap[K comparable, V any](capacity int, data []MapEntry[K, V], comparator Comparator[K]) *SortedMap[K, V] {
	list := make([]MapEntry[K, V], 0, capacity)
	for i := 0; i < len(data); i++ {
		list = append(list, data[i])
	}
	m := &SortedMap[K, V]{
		list:       list,
		comparator: comparator,
	}
	m.size = len(data)
	return m
}

// ForEach all entries - calls the callback for each key-value pair in the table
func (m *SortedMap[K, V]) ForEach(callback func(K, V)) {
	for i := 0; i < m.size; i++ {
		callback(m.list[i].Key, m.list[i].Val)
	}
}

// Get returns a value from the table or false.
func (m *SortedMap[K, V]) Get(key K) (val V, exists bool) {
	if index, exists := m.findItem(key); exists {
		return m.list[index].Val, true
	}

	return
}

// Put associates a key to a value.
func (m *SortedMap[K, V]) Put(key K, val V) {
	index, exists := m.findItem(key)
	if exists {
		m.list[index].Val = val
		return
	}

	// found insert
	if index < m.size {
		// shift
		for j := m.size - 1; j >= index; j-- {
			if j+1 == len(m.list) {
				m.list = append(m.list, m.list[j])
			} else {
				m.list[j+1] = m.list[j]
			}
		}

		m.list[index].Key = key
		m.list[index].Val = val

		m.size += 1
		return
	}

	// no place found - put at the end
	if m.size == len(m.list) {
		m.list = append(m.list, MapEntry[K, V]{key, val})
	} else {
		m.list[m.size].Key = key
		m.list[m.size].Val = val
	}

	m.size += 1
}

// Remove deletes the key from the map and returns whether an element was removed.
func (m *SortedMap[K, V]) Remove(key K) (exists bool) {
	if index, exists := m.findItem(key); exists {
		// shift
		for j := index; j < m.size-1; j++ {
			m.list[j] = m.list[j+1]
		}
		m.size -= 1

		return true
	}

	return false
}

// GetEntries iterates all entries in this map and returns them as a slice.
func (m *SortedMap[K, V]) GetEntries() []MapEntry[K, V] {
	return m.list[0:m.Size()]
}

func (m *SortedMap[K, V]) Size() int {
	return m.size
}

func (m *SortedMap[K, V]) Clear() {
	m.size = 0
}

// findItem finds a key in the list, if it exists.
// It returns the index of the key that was found, and it returns true.
// If the key does not exist, it returns false and the index is equal to the last
// visited position in the list, traversed using binary search.
// The index is increased by one when the last visited key was lower than the input key
// so the new key may be inserted after this key.
// It means the index can be used as a position to insert the key in the list.
func (m *SortedMap[K, V]) findItem(key K) (index int, exists bool) {
	start := 0
	end := m.size - 1
	mid := start
	var res int
	for start <= end {
		mid = (start + end) / 2
		res = m.comparator.Compare(&m.list[mid].Key, &key)
		if res == 0 {
			return mid, true
		} else if res < 0 {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	if res < 0 {
		mid += 1
	}
	return mid, false
}

func (m *SortedMap[K, V]) GetMemoryFootprint() *MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	entrySize := unsafe.Sizeof(MapEntry[K, V]{})

	return NewMemoryFootprint(selfSize + uintptr(len(m.list))*entrySize)
}
