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
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func TestCache_Empty(t *testing.T) {
	for name, c := range initCaches(128) {
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

func TestCache_Get(t *testing.T) {
	for name, c := range initCaches(128) {
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

func TestCache_GetOrSet(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 33)
			val, exists, _, _, _ := c.GetOrSet(1, 333)
			if exists == false || val != 33 {
				t.Errorf("key 1 should exist")
			}

			val, exists, _, _, _ = c.GetOrSet(2, 22)
			if exists {
				t.Errorf("key 2 should not exist")
			}

			val, exists, _, _, _ = c.GetOrSet(2, 333)
			if exists == false || val != 22 {
				t.Errorf("key 1 should exist")
			}
		})
	}
}

func TestCache_Remove(t *testing.T) {
	for name, c := range initCaches(128) {
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

func Test_HitMissRatio_Report(t *testing.T) {
	for name, underlying := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {
			c := NewMissHitTrackingCache(underlying)
			c.Set(1, 11)
			c.Set(2, 22)

			c.Get(1) // hit
			c.Get(8) // miss
			c.Get(2) // hit
			c.Get(9) // miss

			report := c.getHitRatioReport()
			if !strings.HasSuffix(report, "(misses: 2, hits: 2, hitRatio: 0.500000)") {
				t.Errorf("unexpected memory footprint report: %s", report)
			}
		})
	}
}

func TestCache_Remove_NonExisting(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {
			if removed, exists := c.Remove(1); exists || removed != 0 {
				t.Errorf("item removed: %v", removed)
			}
		})
	}
}

func TestCache_Clear_FullCache(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {
			// insert test data
			inserted := make(map[int]int)
			for i := 0; i < 255; i++ {
				inserted[i] = i * 100
				c.Set(i, i*100)
			}

			c.Clear()

			// test
			for key := range inserted {
				if _, exists := c.Get(key); exists {
					t.Errorf("cache should be empty")
				}
			}

			c.Iterate(func(key int, val int) bool {
				t.Errorf("cache should be empty")
				return true
			})
		})
	}
}

func TestCache_Clear(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			keys := []int{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000}
			for _, key := range keys {
				c.Set(key, key+200)
			}

			c.Clear()

			for _, key := range keys {
				if val, exists := c.Get(key); exists {
					t.Errorf("key should not exist: %d -> %d", key, val)
				}
			}

		})
	}
}

func TestCache_Iterate(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			expected := map[int]int{
				1:  100,
				10: 300,
				30: 400,
			}

			for k, v := range expected {
				c.Set(k, v)
			}

			data := make(map[int]int)
			c.Iterate(func(key int, val int) bool {
				data[key] = val
				return true
			})

			if len(data) != len(expected) {
				t.Errorf("wrong number of keys iterated: %v", data)
			}

			for k, v := range expected {
				if got, want := data[k], v; got != want {
					t.Errorf("wrong value for key: %d -> %d != %d", 1, got, want)
				}
			}
		})
	}
}

func TestCache_IterateMutable(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			expected := map[int]int{
				1:  100,
				10: 300,
				30: 400,
			}

			for k, v := range expected {
				c.Set(k, v)
			}

			c.IterateMutable(func(key int, val *int) bool {
				*val = *val * 10 // modify each value
				return true
			})

			var counter int
			c.Iterate(func(key int, val int) bool {
				counter++
				if _, exists := expected[key]; !exists {
					t.Errorf("key was not inserted: %d", key)
				}
				if got, want := expected[key]*10, val; got != want {
					t.Errorf("wrong value for key: %d -> %d != %d", 1, got, want)
				}
				return true
			})

			if got, want := len(expected), counter; got != want {
				t.Errorf("wrong number of iterations: %d != %d", got, want)
			}
		})
	}
}

func TestCache_IterateMutable_Terminated(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			expected := map[int]int{
				1:  100,
				10: 300,
				30: 400,
			}

			for k, v := range expected {
				c.Set(k, v)
			}

			var modifiedKey int
			c.IterateMutable(func(key int, val *int) bool {
				modifiedKey = key
				*val = *val * 10 // modify each value
				return false     // terminate
			})

			c.Iterate(func(key int, val int) bool {
				if _, exists := expected[key]; !exists {
					t.Errorf("key was not inserted: %d", key)
				}
				multiplier := 1
				if key == modifiedKey {
					multiplier = 10
				}
				if got, want := expected[key]*multiplier, val; got != want {
					t.Errorf("wrong value for key: %d -> %d != %d", 1, got, want)
				}
				return true
			})
		})
	}
}

func TestCache_Iterate_Terminated(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			expected := map[int]int{
				1:  100,
				10: 300,
				30: 400,
			}

			for k, v := range expected {
				c.Set(k, v)
			}

			data := make(map[int]int)
			c.Iterate(func(key int, val int) bool {
				data[key] = val
				return false // terminate after one iteration
			})

			if got, want := len(data), 1; got != want {
				t.Errorf("wrong number of keys iterated: %v", data)
			}

			for k, v := range data {
				if got, want := v, expected[k]; got != want {
					t.Errorf("wrong value for key: %d -> %d != %d", 1, got, want)
				}
			}
		})
	}
}

func TestSettingExisting(t *testing.T) {
	for name, c := range initCaches(128) {
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

func TestCache_InsertDeleteInsertIterate(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			// this sequence of operations was detected problematic by fuzzing,
			// respective fuzzing test may be executed as
			// go test -run=FuzzNWays_RandomOps/4af37c7ba0fc5bd7 in this directory
			c.Set(1, 12336)
			c.Set(9, 12336)
			c.Remove(1)
			c.Set(9, 12337)

			value, _ := c.Get(9)
			if got, want := value, 12337; got != want {
				t.Errorf("values do not match: %d != %d", got, want)
			}

			c.Iterate(func(key int, value int) bool {
				if got, want := key, 9; got != want {
					t.Errorf("keys do not match: %d != %d", got, want)
				}

				if got, want := value, 12337; got != want {
					t.Errorf("values do not match: %d: %d != %d", key, got, want)
				}
				return true
			})
		})
	}
}

func TestCache_GetMemoryFootprint(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 12336)
			c.Set(9, 12336)

			var v int
			size := unsafe.Sizeof(v)
			fp := c.GetMemoryFootprint(size)
			if fp.value <= 0 {
				t.Errorf("wrong size provided: %d", fp.value)
			}
		})
	}
}

func TestCache_DynamicMemoryFootprint(t *testing.T) {
	for name, c := range initCaches(3) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {

			c.Set(1, 12336)
			c.Set(9, 12336)

			fp := c.GetDynamicMemoryFootprint(func(v int) uintptr {
				return unsafe.Sizeof(v)
			})
			if fp.value <= 0 {
				t.Errorf("wrong size provided: %d", fp.value)
			}
		})
	}
}

func TestCache_Iterate_FullCache(t *testing.T) {
	for name, c := range initCaches(128) {
		t.Run(fmt.Sprintf("cache %s", name), func(t *testing.T) {
			// insert test data
			expected := make(map[int]int)
			for i := 0; i < 255; i++ {
				expected[i] = i * 100
				evictedKey, _, evicted := c.Set(i, i*100)
				if evicted {
					delete(expected, evictedKey)
				}
			}

			// test
			c.Iterate(func(key int, val int) bool {
				want, exists := expected[key]
				if !exists {
					t.Errorf("iterated through the key that should not exist: %d", key)
				}

				if want != val {
					t.Errorf("iterated through an unexpected value: %d != %d", want, val)
				}
				return true
			})
		})
	}
}

func TestPrintNumberOfEvictions(t *testing.T) {
	if !testing.Verbose() {
		return
	}
	EvalPrintNumberOfEvictions()
}

func EvalPrintNumberOfEvictions() {
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
		fmt.Printf("LruCache: %s, evictions: %2.2f%%\n", name, float32(count)/float32(N)*100)
	}
}

func initCaches(capacity int) map[string]Cache[int, int] {
	return map[string]Cache[int, int]{
		"lruCache":           NewLruCache[int, int](capacity),
		"missHit LruCache":   NewMissHitTrackingCache[int, int](NewLruCache[int, int](capacity)),
		"missHit NWaysCache": NewMissHitTrackingCache[int, int](NewNWaysCache[int, int](capacity, 2)),
		"synced lruCache":    NewSyncedCache[int, int](NewLruCache[int, int](capacity)),
		"2-ways LruCache":    NewNWaysCache[int, int](capacity, 2),
		"4-ways LruCache":    NewNWaysCache[int, int](capacity, 4),
		"8-ways LruCache":    NewNWaysCache[int, int](capacity, 8),
		"16-ways LruCache":   NewNWaysCache[int, int](capacity, 16),
		"32-ways LruCache":   NewNWaysCache[int, int](capacity, 32),
	}
}

type SyncedCache[K any, V any] struct {
	cache Cache[K, V]
	lock  sync.Mutex
}

func NewSyncedCache[K any, V any](wrapped Cache[K, V]) *SyncedCache[K, V] {
	return &SyncedCache[K, V]{
		cache: wrapped,
		lock:  sync.Mutex{},
	}
}

func (c *SyncedCache[K, V]) Iterate(callback func(K, V) bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache.Iterate(callback)
}

func (c *SyncedCache[K, V]) IterateMutable(callback func(K, *V) bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache.IterateMutable(callback)
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

func (c *SyncedCache[K, V]) GetOrSet(key K, val V) (current V, present bool, evictedKey K, evictedValue V, evicted bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.GetOrSet(key, val)
}

func (c *SyncedCache[K, V]) Remove(key K) (original V, exists bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.Remove(key)
}

func (c *SyncedCache[K, V]) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache.Clear()
}

func (c *SyncedCache[K, V]) GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.GetMemoryFootprint(referencedValueSize)
}

func (c *SyncedCache[K, V]) GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.cache.GetDynamicMemoryFootprint(valueSizeProvider)
}
