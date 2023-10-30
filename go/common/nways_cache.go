package common

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/exp/constraints"
)

// PaddingMultiplier is used for extending the size of arrays that stores mutexes
// and potentially other variables.
// It serves as padding for memory that may be cached and shared by more threads,
// trying to avoid False Sharing (see https://en.wikipedia.org/wiki/False_sharing#:~:text=In%20computer%20science%2C%20false%20sharing,managed%20by%20the%20caching%20mechanism.)
// by adding extra elements.
// The mutex size is 8bytes, for 64bytes of cache, data are stored in the array at every 8th position.
const PaddingMultiplier = 8

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
	tickers []uint64

	misses atomic.Uint64
	hits   atomic.Uint64
}

// NewNWaysCache creates a new N-ways Cache with the configured capacity and number of ways.
// Note that actual capacity will be aligned to multiplies of ways.
func NewNWaysCache[K constraints.Integer, V any](capacity, ways int) *NWaysCache[K, V] {
	numsets := int(math.Ceil(float64(capacity) / float64(ways)))
	return &NWaysCache[K, V]{
		items:   make([]nWaysCacheEntry[K, V], numsets*ways), // adjust capacity by rounding up
		locks:   make([]sync.Mutex, PaddingMultiplier*numsets),
		nways:   uint(ways),
		numsets: uint(numsets),
		tickers: make([]uint64, PaddingMultiplier*numsets),
	}
}

func (c *NWaysCache[K, V]) Get(key K) (V, bool) {
	setIndex := (uint(key) % c.numsets) * PaddingMultiplier
	c.locks[setIndex].Lock()
	c.tickers[setIndex]++

	// find first position of the sat
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = c.tickers[setIndex]
			if MissHitMeasuring {
				c.hits.Add(1)
			}
			value := c.items[i].value
			c.locks[setIndex].Unlock()
			return value, true
		}
	}

	if MissHitMeasuring {
		c.misses.Add(1)
	}
	c.locks[setIndex].Unlock()
	var v V
	return v, false
}

func (c *NWaysCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	setIndex := (uint(key) % c.numsets) * PaddingMultiplier
	c.locks[setIndex].Lock()
	c.tickers[setIndex]++
	oldest := c.tickers[setIndex]

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
			c.items[i].used = c.tickers[setIndex]
			c.locks[setIndex].Unlock()
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
	c.items[oldestIndex].used = c.tickers[setIndex]
	c.locks[setIndex].Unlock()
	return evictedKey, evictedValue, true
}

func (c *NWaysCache[K, V]) Remove(key K) (original V, exists bool) {
	setIndex := (uint(key) % c.numsets) * PaddingMultiplier
	c.locks[setIndex].Lock()

	// find first free position
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = 0
			value := c.items[i].value

			// if we are not at the last position
			if i < position+c.nways-1 {
				// swap removed value with the last non-empty entry to avoid holes in the entries
				// as the method Set() relies on the fact that non-empty positions are consecutive,
				// followed by empty entries after them.
				for j := position + c.nways - 1; j > i; j-- {
					if c.items[j].used > 0 {
						c.items[i] = c.items[j] // swap
						c.items[j].used = 0     // free this item
						break
					}
				}
			}
			c.locks[setIndex].Unlock()
			return value, true
		}
	}

	c.locks[setIndex].Unlock()
	return original, false
}

// GetOrSet tries to locate the input key in the cache. IF the key exists, its value is returned.
// If the key does not exist, the input value is stored under this key.
// When the key is stored in this cache, another key and value may be evicted.
// This method returns true if the key was present in the cache. It also returns if another key was evicted due
// to inserting this key.
func (c *NWaysCache[K, V]) GetOrSet(key K, val V) (current V, present bool, evictedKey K, evictedValue V, evicted bool) {
	setIndex := (uint(key) % c.numsets) * PaddingMultiplier
	c.locks[setIndex].Lock()
	c.tickers[setIndex]++
	oldest := c.tickers[setIndex]

	var oldestIndex uint

	// find first free position
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		// key found in a non-empty location -> return its value.
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = c.tickers[setIndex]
			if MissHitMeasuring {
				c.hits.Add(1)
			}
			value := c.items[i].value
			c.locks[setIndex].Unlock()
			return value, true, evictedKey, evictedValue, false
		}
		if c.items[i].used < oldest {
			oldest = c.items[i].used
			oldestIndex = i
		}
	}

	if MissHitMeasuring {
		c.misses.Add(1)
	}

	// evict the oldest used key
	evictedKey = c.items[oldestIndex].key
	evictedValue = c.items[oldestIndex].value
	isEvicted := c.items[oldestIndex].used > 0
	c.items[oldestIndex].key = key
	c.items[oldestIndex].value = val
	c.items[oldestIndex].used = c.tickers[setIndex]
	c.locks[setIndex].Unlock()
	return current, false, evictedKey, evictedValue, isEvicted
}

// Iterate calls the callback method for each entry in this cache.
// This method is locking around the callback, i.e. the client should
// not hold the method for a long time.
func (c *NWaysCache[K, V]) Iterate(callback func(K, V) bool) {
	for i := 0; i < int(c.numsets); i += 1 {
		c.locks[i*PaddingMultiplier].Lock()
		for j := 0; j < int(c.nways); j++ {
			pos := i*int(c.nways) + j
			if c.items[pos].used > 0 {
				callback(c.items[pos].key, c.items[pos].value)
			}
		}
		c.locks[i*PaddingMultiplier].Unlock()
	}
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
	hits := c.hits.Load()
	misses := c.misses.Load()
	hitRatio := float32(hits) / float32(hits+misses)
	return fmt.Sprintf("(n-way, size: %d, misses: %d, hits: %d, hitRatio: %f)", len(c.items), misses, hits, hitRatio)
}

type nWaysCacheEntry[K constraints.Integer, V any] struct {
	key   K
	value V
	used  uint64
}
