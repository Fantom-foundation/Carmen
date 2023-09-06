package common

import (
	"fmt"
	"sync"
	"testing"
)

func TestEmpty(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {
			_, exists := c.Get(1)
			if exists {
				t.Errorf("Item should not exist")
			}

			_, exists = c.Get(2)
			if exists {
				t.Errorf("Item should not exist")
			}
		})
	}
}

func TestItemExist(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 33)
			val, exists := c.Get(1)
			if exists == false || val != 33 {
				t.Errorf("Item 33 should exist")
			}

			_, exists = c.Get(2)
			if exists {
				t.Errorf("Item should not exist")
			}
		})
	}
}

func TestCacheRemove(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 11)

			if removed, exists := c.Remove(1); !exists || removed != 11 {
				t.Errorf("Item not removed: %v", removed)
			}

			if removed, exists := c.Get(1); exists {
				t.Errorf("Item not removed: %v", removed)
			}

			c.Set(4, 44)

			if actual, exists := c.Get(4); !exists {
				t.Errorf("Item not present: %v", actual)
			}
		})
	}
}

func TestSettingExisting(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 11)
			c.Set(1, 67)

			value, exists := c.Get(1)
			if !exists || value != 67 {
				t.Errorf("Item value invalid")
			}
		})
	}
}

func TestPrintNumberOfEvictions(t *testing.T) {
	if !testing.Verbose() {
		return
	}
	ExamplePrintNumberOfEvictions()
}

func ExamplePrintNumberOfEvictions() {
	const N = 15_000
	const CAPACITY = 10_000
	keys := generateRandomKeys(N)

	evictions := make(map[string]int)

	for name, c := range initCaches(CAPACITY) {
		evictions[name] = 0
		for i := 0; i < N; i++ {
			if _, _, evicted := c.Set(keys[i], i); evicted {
				evictions[name]++
			}
		}
	}
	for name, count := range evictions {
		fmt.Printf("Cache: %s, evictions: %2.2f%%\n", name, float32(count)/float32(N)*100)
	}
}

// Cache implements a memory overlay for the key-value pair.
// The keys can be set and obtained from the cache. The keys
// accumulate in the cache until the cache is full, i.e. it reaches its capacity.
// When this happens, a new key causes eviction of another key.
type cache[K any, V any] interface {
	// Get returns a value from the cache or false. If the value exists, its number of use is updated
	Get(key K) (V, bool)
	// Set associates a key to the cache.
	// If the key is already present, the value is updated and the key marked as
	// used. If the value is not present, a new entry is added to this
	// cache. This causes another entry to be removed if the cache size is exceeded.
	Set(key K, val V) (evictedKey K, evictedValue V, evicted bool)

	// Remove deletes the key from the map and returns the deleted value
	Remove(key K) (original V, exists bool)
}

func initCaches(capacity int) map[string]cache[int, int] {
	return map[string]cache[int, int]{
		"lruCache":        NewCache[int, int](capacity),
		"synced lruCache": NewSyncedCache[int, int](NewCache[int, int](capacity)),
		"2-ways Cache":    NewNWaysCache[int, int](capacity, 2),
		"4-ways Cache":    NewNWaysCache[int, int](capacity, 4),
		"8-ways Cache":    NewNWaysCache[int, int](capacity, 8),
		"16-ways Cache":   NewNWaysCache[int, int](capacity, 16),
		"32-ways Cache":   NewNWaysCache[int, int](capacity, 32),
	}
}

type SyncedCache[K any, V any] struct {
	cache[K, V]
	lock sync.Mutex
}

func NewSyncedCache[K any, V any](wrapped cache[K, V]) *SyncedCache[K, V] {
	return &SyncedCache[K, V]{
		cache: wrapped,
		lock:  sync.Mutex{},
	}
}

func (c *SyncedCache[K, V]) Get(key K) (V, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.Get(key)
}

func (c *SyncedCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.Set(key, val)
}

func (c *SyncedCache[K, V]) Remove(key K) (original V, exists bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.Remove(key)
}
