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

const kFastMapBuckets = 1 << 16

// ShortHasher is an interface for types implementing hash functions for values.
type ShortHasher[K any] interface {
	Hash(K) uint16
}

// FastMap is a hash based map type mapping keys to values using a customizable hash function.
// Furthermore, it supports a O(1) clear operation and minimizes memory allocations.
type FastMap[K comparable, V any] struct {
	buckets     [kFastMapBuckets]fmPtr
	data        []fastMapEntry[K, V]
	generation  uint16
	hasher      ShortHasher[K]
	size        int
	usedBuckets []uint16
}

// NewFastMap creates a FastMap based on the given hasher.
func NewFastMap[K comparable, V any](hasher ShortHasher[K]) *FastMap[K, V] {
	res := &FastMap[K, V]{
		// The initial size is just an optimization to reduce initial resizing operations.
		data:        make([]fastMapEntry[K, V], 0, 10000),
		hasher:      hasher,
		usedBuckets: make([]uint16, 0, kFastMapBuckets),
	}
	// Clear is required to bring the map in a valid state.
	res.Clear()
	return res
}

// Get retrieves a value stored in the map or the types default value, if not present.
// The second return value is set to true if the value was present, false otherwise.
func (m *FastMap[K, V]) Get(key K) (V, bool) {
	hash := m.hasher.Hash(key)
	cur := m.toPos(m.buckets[hash])
	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			return m.data[cur].value, true
		}
		cur = m.toPos(m.data[cur].next)
	}
	var res V
	return res, false
}

// Put updates the value associated to the given key in this map.
func (m *FastMap[K, V]) Put(key K, value V) {
	hash := m.hasher.Hash(key)
	cur := m.toPos(m.buckets[hash])

	// Register newly used bucket.
	if cur < 0 {
		m.usedBuckets = append(m.usedBuckets, hash)
	}

	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			m.data[cur].value = value
			return
		}
		cur = m.toPos(m.data[cur].next)
	}
	new := len(m.data)
	m.data = append(m.data, fastMapEntry[K, V]{})
	m.data[new].key = key
	m.data[new].value = value
	m.data[new].next = m.buckets[hash]
	m.buckets[hash] = m.toPtr(int64(new))
	m.size++
}

// Remove removes the entry with the given key from this map and returns
// whether the key has been present before the delete operation.
func (m *FastMap[K, V]) Remove(key K) bool {
	hash := m.hasher.Hash(key)
	cur := m.toPos(m.buckets[hash])
	ptr := &m.buckets[hash]
	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			*ptr = m.data[cur].next
			m.size--
			return true
		}
		ptr = &m.data[cur].next
		cur = m.toPos(m.data[cur].next)
	}
	return false
}

// Clear removes all entries of this map. This is an O(1)
func (m *FastMap[K, V]) Clear() {
	// In the vast majority of cases, this only requires O(1) updates.
	m.data = m.data[0:0] // < reuse underlying array
	m.generation++       // < invalidate all fast map pointers
	m.size = 0
	m.usedBuckets = m.usedBuckets[0:0]
	// When we have a generation overflow we need to
	// reset all buckets to make sure nothing that was
	// added previously accidentially becomes valid again.
	if m.generation == 0 {
		for i := range m.buckets {
			m.buckets[i] = -1
		}
	}
}

// Size returns the number of elements in this map.
func (m *FastMap[K, V]) Size() int {
	return m.size
}

// ForEach applies the given operation to each key/value pair in the map.
func (m *FastMap[K, V]) ForEach(op func(K, V)) {
	for _, i := range m.usedBuckets {
		pos := m.toPos(m.buckets[i])
		for 0 <= pos && pos < int64(len(m.data)) {
			entry := &m.data[pos]
			op(entry.key, entry.value)
			pos = m.toPos(entry.next)
		}
	}
}

// CopyTo copy all the map content to another FastMap.
func (m *FastMap[K, V]) CopyTo(dest *FastMap[K, V]) {
	m.ForEach(func(key K, value V) {
		dest.Put(key, value)
	})
}

// fmPtr is the pointer type used inside the FastMap, comprising an
// a position in the FastMap's data store and a generation counter.
// The format devices the 64 bit of the fmPtr
//   - first bit for valid/not (=negative values are invalid pointers)
//   - 47 position bits for up to 2^47 elements in the map
//   - 16 generation bits, for up to 2^16 generations of content befor
//     a full reset is required
//
// as long as the value is positive. Negative values are considered
// nil pointers.
type fmPtr int64

// toPtr converts a position index into a FastMap pointer.
func (m *FastMap[K, V]) toPtr(pos int64) fmPtr {
	// Negative positions become nil pointers.
	if pos < 0 {
		return fmPtr(pos)
	}
	return fmPtr(int64(pos)<<16 | int64(m.generation))
}

// toPos converts a FastMap pointer into a position.
func (m *FastMap[K, V]) toPos(ptr fmPtr) int64 {
	// Identify nil pointers or invalid pointers.
	if ptr < 0 || uint16(ptr) != m.generation {
		return -1
	}
	return int64(ptr) >> 16
}

type fastMapEntry[K any, V any] struct {
	key   K
	value V
	next  fmPtr
}
