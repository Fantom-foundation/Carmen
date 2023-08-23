package common

import (
	"golang.org/x/exp/constraints"
	"math"
)

// NWaysCache is a cache witch configurable capacity and the number of ways.
// It divides its capacity into sets such that every set can accommodate up to the confined number
// of elements (=ways). When a key is inserted, its corresponding set is computed first.
// Then the set is linearly iterated to find a free spot within this set.
// The key is inserted when a free spot is found. If the set is already full, another key is evicted.
// Since key association to the set is computing using modulo, it is cheap and thus fast.
type NWaysCache[K constraints.Integer, V any] struct {
	items   []nWaysCacheEntry[K, V]
	nways   uint // number of ways
	numsets uint // number of sets: nways * numsets -> capacity rounded up
	ticker  uint64
}

// NewNWaysCache creates a new N-ways Cache with the configured capacity and number of ways.
// Note that actual capacity will be aligned to multiplies of ways.
func NewNWaysCache[K constraints.Integer, V any](capacity, ways int) *NWaysCache[K, V] {
	numsets := int(math.Ceil(float64(capacity) / float64(ways)))
	return &NWaysCache[K, V]{
		items:   make([]nWaysCacheEntry[K, V], numsets*ways), // adjust capacity by rounding up
		nways:   uint(ways),
		numsets: uint(numsets),
	}
}

func (c *NWaysCache[K, V]) Get(key K) (V, bool) {
	c.ticker++
	// find first position of the sat
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	for i := position; i < position+c.nways; i++ {
		if c.items[i].used > 0 && c.items[i].key == key {
			c.items[i].used = c.ticker
			return c.items[i].value, true
		}
	}

	var v V
	return v, false
}

func (c *NWaysCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	c.ticker++
	var oldestIndex uint
	// find first free position
	position := uint(key) % c.numsets * c.nways
	// try to find the key by iterating the set from its starting position
	oldest := c.ticker
	for i := position; i < position+c.nways; i++ {
		// either empty position or replacing the same key
		if c.items[i].used == 0 || c.items[i].key == key {
			evictedKey = c.items[i].key
			evictedValue = c.items[i].value
			c.items[i].key = key
			c.items[i].value = val
			c.items[i].used = c.ticker
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
	c.items[oldestIndex].used = c.ticker
	return evictedKey, evictedValue, true
}

func (c *NWaysCache[K, V]) Remove(key K) (original V, exists bool) {
	// find first position of the sat
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

type nWaysCacheEntry[K constraints.Integer, V any] struct {
	key   K
	value V
	used  uint64
}
