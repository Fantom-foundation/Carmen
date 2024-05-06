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

import (
	"fmt"
	"unsafe"
)

// LruCache implements a memory overlay for the key-value pair
type LruCache[K comparable, V any] struct {
	cache    map[K]*entry[K, V]
	capacity int
	head     *entry[K, V]
	tail     *entry[K, V]
}

// NewLruCache returns a new instance
func NewLruCache[K comparable, V any](capacity int) *LruCache[K, V] {
	return &LruCache[K, V]{
		cache:    make(map[K]*entry[K, V], capacity),
		capacity: capacity,
	}
}

// Iterate all cached entries - calls the callback for each key-value pair in the cache
func (c *LruCache[K, V]) Iterate(callback func(K, V) bool) {
	for key, value := range c.cache {
		if !callback(key, value.val) {
			return // terminate iteration if false returned from the callback
		}
	}
}

// Iterate all cached entries by passing a mutable value reference to the provided callback.
func (c *LruCache[K, V]) IterateMutable(callback func(K, *V) bool) {
	for key, value := range c.cache {
		if !callback(key, &value.val) {
			return // terminate iteration if false returned from the callback
		}
	}
}

// Get returns a value from the cache or false. If the value exists, its number of use is updated
func (c *LruCache[K, V]) Get(key K) (V, bool) {
	var val V
	item, exists := c.cache[key]
	if exists {
		val = item.val
		c.touch(item)
	}

	return val, exists
}

// Set associates a key to the cache.
// If the key is already present, the value is updated and the key marked as
// used. If the value is not present, a new entry is added to this
// cache. This causes another entry to be removed if the cache size is exceeded.
func (c *LruCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	item, exists := c.cache[key]

	// create entry if it does not exist
	if !exists {
		if len(c.cache) >= c.capacity {
			item = c.dropLast() // reuse evicted object for the new entry
			evictedKey = item.key
			evictedValue = item.val
			evicted = true
		} else {
			item = new(entry[K, V])
		}
		item.key = key
		item.val = val
		c.cache[key] = item

		// Make the new entry the head of the LRU queue.
		item.prev = nil
		item.next = c.head
		if c.head != nil {
			c.head.prev = item
		}
		c.head = item

		// The very first item is a head and a tail at the same time.
		if c.tail == nil {
			c.tail = c.head
		}
	} else {
		// if the entry exists, set the value only and move it to the head
		item.val = val
		c.touch(item)
	}
	return
}

func (c *LruCache[K, V]) GetOrSet(key K, val V) (current V, present bool, evictedKey K, evictedValue V, evicted bool) {
	current, present = c.Get(key)
	if !present {
		evictedKey, evictedValue, evicted = c.Set(key, val)
	}

	return current, present, evictedKey, evictedValue, evicted
}

// Remove deletes the key from the map and returns the deleted value
func (c *LruCache[K, V]) Remove(key K) (original V, exists bool) {
	item, exists := c.cache[key]
	if exists {
		original = item.val
		delete(c.cache, key)

		// single item list
		if c.head == c.tail {
			c.head = nil
			c.tail = nil
		}

		// update not in the tail
		if item.next != nil {
			item.next.prev = item.prev

			if item == c.head {
				c.head = item.next
			}
		}

		// update not in the head
		if item.prev != nil {
			item.prev.next = item.next

			if item == c.tail {
				c.tail = item.prev
			}
		}
	}

	return
}

func (c *LruCache[K, V]) Clear() {
	if len(c.cache) > 0 {
		c.cache = make(map[K]*entry[K, V], c.capacity)
	}
	c.head = nil
	c.tail = nil
}

// touch marks the entry used
func (c *LruCache[K, V]) touch(item *entry[K, V]) {
	// already head
	if item == c.head {
		return
	}

	// remove en from the list
	item.prev.next = item.next
	if item.next != nil { // not tail
		item.next.prev = item.prev
	} else {
		c.tail = item.prev
	}

	// and put it in front
	item.prev = nil
	item.next = c.head
	c.head.prev = item
	c.head = item
}

// dropLast drop the last element from the queue and returns it
func (c *LruCache[K, V]) dropLast() (dropped *entry[K, V]) {
	dropped = c.tail
	delete(c.cache, c.tail.key)
	c.tail = c.tail.prev
	c.tail.next = nil
	return dropped
}

// GetMemoryFootprint provides the size of the cache in memory in bytes
// If V is a pointer type, it needs to provide the size of a referenced value.
// If the size is different for individual values, use GetDynamicMemoryFootprint instead.
func (c *LruCache[K, V]) GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entrySize := unsafe.Sizeof(entry[K, V]{})
	mf := NewMemoryFootprint(selfSize + uintptr(c.capacity)*(entrySize+referencedValueSize))
	return mf
}

// GetDynamicMemoryFootprint provides the size of the cache in memory in bytes for values,
// which reference dynamic amount of memory - like slices.
func (c *LruCache[K, V]) GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entryPointerSize := unsafe.Sizeof(&entry[K, V]{})
	size := uintptr(c.capacity) * entryPointerSize
	for _, value := range c.cache {
		size += unsafe.Sizeof(entry[K, V]{})
		size += valueSizeProvider(value.val)
	}
	mf := NewMemoryFootprint(selfSize + size)
	return mf
}

// entry is a cache item wrapping an index, a key and references to previous and next elements.
type entry[K comparable, V any] struct {
	key  K
	val  V
	prev *entry[K, V]
	next *entry[K, V]
}

func (e entry[K, V]) String() string {
	return fmt.Sprintf("Entry: %v -> %v", e.key, e.val)
}
