package common

import (
	"fmt"
	"golang.org/x/exp/constraints"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

// NWaysCache is a cache witch configurable capacity and the number of ways.
// It divides its capacity into sets such that every set can accommodate up to the confined number
// of elements (=ways). When a key is inserted, its corresponding set is computed first.
// Then the set is linearly iterated to find a free spot within this set.
// The key is inserted when a free spot is found. If the set is already full, another key is evicted.
// Since key association to the set is computing using modulo, it is cheap and thus fast.
type NWaysCache[K constraints.Integer, V any] struct {
	items   []nWaysCacheEntry[K, V]
	locks   []sync.Mutex // locks for each Way of the cache
	nways   uint         // number of ways
	numsets uint         // number of sets: nways * numsets -> capacity rounded up
	ticker  *atomic.Uint64

	misses int
	hits   int
}

// NewNWaysCache creates a new N-ways Cache with the configured capacity and number of ways.
// Note that actual capacity will be aligned to multiplies of ways.
func NewNWaysCache[K constraints.Integer, V any](capacity, ways int) *NWaysCache[K, V] {
	numsets := int(math.Ceil(float64(capacity) / float64(ways)))
	locks := make([]sync.Mutex, numsets)

	return &NWaysCache[K, V]{
		items:   make([]nWaysCacheEntry[K, V], numsets*ways), // adjust capacity by rounding up
		locks:   locks,
		nways:   uint(ways),
		numsets: uint(numsets),
		ticker:  &atomic.Uint64{},
	}
}

func (c *NWaysCache[K, V]) Get(key K) (V, bool) {
	setIndex := uint(key) % c.numsets
	c.locks[setIndex].Lock()
	defer c.locks[setIndex].Unlock()

	oldest := c.ticker.Add(1)

	// find first position of the sat
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = oldest
			if MissHitMeasuring {
				c.hits++
			}
			return c.items[i].value, true
		}
	}

	if MissHitMeasuring {
		c.misses++
	}
	var v V
	return v, false
}

func (c *NWaysCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	setIndex := uint(key) % c.numsets
	c.locks[setIndex].Lock()
	defer c.locks[setIndex].Unlock()

	oldest := c.ticker.Add(1)
	var oldestIndex uint

	// find first free position
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		// either empty position or replacing the same key
		if c.items[i].used == 0 || c.items[i].key == key {
			evictedKey = c.items[i].key
			evictedValue = c.items[i].value
			c.items[i].key = key
			c.items[i].value = val
			c.items[i].used = oldest
			return evictedKey, evictedValue, false
		}
		if c.items[i].used < oldest {
			oldest = c.items[i].used
			oldestIndex = i
		}
	}

	// evict the oldest used key
	evictedKey = c.items[oldestIndex].key
	evictedValue = c.items[oldestIndex].value
	c.items[oldestIndex].key = key
	c.items[oldestIndex].value = val
	c.items[oldestIndex].used = oldest
	return evictedKey, evictedValue, true
}

func (c *NWaysCache[K, V]) Remove(key K) (original V, exists bool) {
	setIndex := uint(key) % c.numsets
	c.locks[setIndex].Lock()
	defer c.locks[setIndex].Unlock()

	// find first free position
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = 0
			return c.items[i].value, true
		}
	}

	return original, false
}

// GetMemoryFootprint provides the size of the cache in memory in bytes
// If V is a pointer type, it needs to provide the size of a referenced value.
// If the size is different for individual values, use GetDynamicMemoryFootprint instead.
func (c *NWaysCache[K, V]) GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entrySize := unsafe.Sizeof(entry[K, V]{})
	capacity := c.numsets * c.nways
	mf := NewMemoryFootprint(selfSize + uintptr(capacity)*(entrySize+referencedValueSize))
	if MissHitMeasuring {
		mf.SetNote(c.getHitRatioReport())
	}
	return mf
}

// GetDynamicMemoryFootprint provides the size of the cache in memory in bytes for values,
// which reference dynamic amount of memory - like slices.
func (c *NWaysCache[K, V]) GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entryPointerSize := unsafe.Sizeof(&entry[K, V]{})
	capacity := c.numsets * c.nways
	size := uintptr(capacity) * entryPointerSize
	for _, value := range c.items {
		size += unsafe.Sizeof(entry[K, V]{})
		if value.used > 0 {
			size += valueSizeProvider(value.value)
		}
	}
	mf := NewMemoryFootprint(selfSize + size)
	if MissHitMeasuring {
		mf.SetNote(c.getHitRatioReport())
	}
	return mf
}

func (c *NWaysCache[K, V]) getHitRatioReport() string {
	hitRatio := float32(c.hits) / float32(c.hits+c.misses)
	return fmt.Sprintf("(misses: %d, hits: %d, hitRatio: %f)", c.misses, c.hits, hitRatio)
}

type nWaysCacheEntry[K constraints.Integer, V any] struct {
	key   K
	value V
	used  uint64
}
