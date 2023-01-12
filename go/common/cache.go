package common

import (
	"fmt"
	"unsafe"
)

const MissHitMeasuring = true

// Cache implements a memory overlay for the key-value pair
type Cache[K comparable, V any] struct {
	cache    map[K]*entry[K, V]
	capacity int
	head     *entry[K, V]
	tail     *entry[K, V]
	misses   int
	hits     int
}

// NewCache returns a new instance
func NewCache[K comparable, V any](capacity int) *Cache[K, V] {
	return &Cache[K, V]{
		cache:    make(map[K]*entry[K, V], capacity),
		capacity: capacity,
	}
}

// Iterate all cached entries - calls the callback for each key-value pair in the cache
func (c *Cache[K, V]) Iterate(callback func(K, V) bool) {
	for key, value := range c.cache {
		if !callback(key, value.val) {
			return // terminate iteration if false returned from the callback
		}
	}
}

// Iterate all cached entries by passing a mutable value reference to the provided callback.
func (c *Cache[K, V]) IterateMutable(callback func(K, *V) bool) {
	for key, value := range c.cache {
		if !callback(key, &value.val) {
			return // terminate iteration if false returned from the callback
		}
	}
}

// Get returns a value from the cache or false. If the value exists, its number of use is updated
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var val V
	item, exists := c.cache[key]
	if exists {
		val = item.val
		c.touch(item)
		if MissHitMeasuring {
			c.hits++
		}
	} else {
		if MissHitMeasuring {
			c.misses++
		}
	}

	return val, exists
}

// Set associates a key to the cache.
// If the key is already present, the value is updated and the key marked as
// used. If the value is not present, a new entry is added to this
// cache. This causes another entry to be removed if the cache size is exceeded.
func (c *Cache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V) {
	item, exists := c.cache[key]

	// create entry if it does not exist
	if !exists {
		if len(c.cache) >= c.capacity {
			item = c.dropLast() // reuse evicted object for the new entry
			evictedKey = item.key
			evictedValue = item.val
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

		// The very first en is head and tail at the same time.
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

// Remove deletes the key from the map and returns the deleted value
func (c *Cache[K, V]) Remove(key K) (original V, exists bool) {
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

func (c *Cache[K, V]) Clear() {
	c.cache = make(map[K]*entry[K, V], c.capacity)
	c.head = nil
	c.tail = nil
}

// touch marks the entry used
func (c *Cache[K, V]) touch(item *entry[K, V]) {
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
func (c *Cache[K, V]) dropLast() (dropped *entry[K, V]) {
	if c.tail == nil {
		return nil // no tail - empty list
	}

	dropped = c.tail
	delete(c.cache, c.tail.key)
	c.tail = c.tail.prev
	c.tail.next = nil
	return dropped
}

// GetMemoryFootprint provides the size of the cache in memory in bytes
// If V is a pointer type, it needs to provide the size of a referenced value.
// If the size is different for individual values, use GetDynamicMemoryFootprint instead.
func (c *Cache[K, V]) GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entrySize := unsafe.Sizeof(entry[K, V]{})
	mf := NewMemoryFootprint(selfSize + uintptr(c.capacity)*(entrySize+referencedValueSize))
	if MissHitMeasuring {
		mf.SetNote(c.getHitRatioReport())
	}
	return mf
}

// GetDynamicMemoryFootprint provides the size of the cache in memory in bytes for values,
// which reference dynamic amount of memory - like slices.
func (c *Cache[K, V]) GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entryPointerSize := unsafe.Sizeof(&entry[K, V]{})
	size := uintptr(c.capacity) * entryPointerSize
	for _, value := range c.cache {
		size += unsafe.Sizeof(entry[K, V]{})
		size += valueSizeProvider(value.val)
	}
	mf := NewMemoryFootprint(selfSize + size)
	if MissHitMeasuring {
		mf.SetNote(c.getHitRatioReport())
	}
	return mf
}

func (c *Cache[K, V]) getHitRatioReport() string {
	hitRatio := float32(c.hits) / float32(c.hits+c.misses)
	return fmt.Sprintf("(misses: %d, hits: %d, hitRatio: %f)", c.misses, c.hits, hitRatio)
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
