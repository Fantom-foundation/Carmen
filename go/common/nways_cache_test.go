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
	"math/rand"
	"sync"
	"testing"
)

func TestNWaysCacheInitialisation(t *testing.T) {
	c := NewNWaysCache[int, int](31, 4)

	// capacity rounded up
	if got, want := len(c.items), 32; got != want {
		t.Errorf("sizes do not match: %d != %d", got, want)
	}

	if got, want := c.nways, uint(4); got != want {
		t.Errorf("number of ways do not match: %d != %d", got, want)
	}

	if got, want := c.numsets, uint(8); got != want {
		t.Errorf("number of sets do not match: %d != %d", got, want)
	}

}

func TestNWaysExceedCapacity(t *testing.T) {
	c := NewNWaysCache[int, int](3, 2)

	c.Set(1, 11)
	c.Set(3, 22)

	// value will get evicted from its slot even if the cache is not fully utilised
	evictedKey, evictedValue, evicted := c.Set(5, 55)
	if evictedKey != 1 || evictedValue != 11 || !evicted {
		t.Errorf("Incorrectly evicted items: %d/%d", evictedKey, evictedValue)
	}

	_, exists := c.Get(5) // one refreshed - first in the list now
	if exists == false {
		t.Errorf("Item should exist")
	}

	// fills the other set - no eviction
	evictedKey, evictedValue, evicted = c.Set(4, 44)
	if evictedKey != 0 || evictedValue != 0 || evicted {
		t.Errorf("No items should have been evicted yet")
	}
}

func TestNWaysCache_GetOrSet(t *testing.T) {
	c := NewNWaysCache[int, int](8, 4)

	if _, present, _, _, evicted := c.GetOrSet(1, 11); present || evicted {
		t.Errorf("value should neither present nor evicted")
	}

	if current, present, _, _, _ := c.GetOrSet(1, 12); !present || current != 11 {
		t.Errorf("previous value should be present")
	}

	// cause eviction
	c.Set(5, 5)
	c.Set(9, 9)
	c.Set(13, 13)

	if _, present, evictedKey, evictedValue, evicted := c.GetOrSet(17, 13); !evicted || present || evictedKey != 1 || evictedValue != 11 {
		t.Errorf("value should be evicted")
	}

	// no eviction - replacing
	if current, present, _, _, evicted := c.GetOrSet(9, 13); evicted || !present || current != 9 {
		t.Errorf("value should be evicted")
	}

}

func TestNWaysCache_Remove_Reinsert_Full_Slot(t *testing.T) {
	c := NewNWaysCache[int, int](8, 4)

	c.Set(0, 5)
	c.Set(1, 10)
	c.Set(2, 20)
	c.Set(3, 30)
	c.Set(4, 40)
	c.Set(5, 50)
	c.Set(6, 60)
	c.Set(7, 70)

	c.Remove(5)
	c.Set(6, 80)

	c.Iterate(func(key int, val int) bool {
		if key == 5 {
			t.Errorf("this key should have been removed: %d", key)
		}
		if key == 6 && val != 80 {
			t.Errorf("unexpected value: %d != %d", val, 80)
		}

		return true
	})

	c.Set(5, 55)
	c.Remove(5) // remove at the end of the slot - no swap will happen
	c.Set(9, 90)

	c.Iterate(func(key int, val int) bool {
		if key == 5 {
			t.Errorf("this key should have been removed: %d", key)
		}
		if key == 9 && val != 90 {
			t.Errorf("unexpected value: %d != %d", val, 80)
		}

		return true
	})
}

func TestNWaysLRUEvictedCapacity(t *testing.T) {
	c := NewNWaysCache[int, int](6, 3)

	c.Set(8, 8)
	c.Set(9, 9)
	c.Set(10, 10)
	c.Set(11, 11)
	c.Set(12, 12)
	c.Set(13, 13)

	// it will cause eviction
	if evictedKey, evictedVal, evicted := c.Set(14, 14); evictedKey != 8 || evictedVal != 8 || !evicted {
		t.Errorf("unexpected or unvecited entry: %d -> %d evicted: %v", evictedKey, evictedVal, evicted)
	}

	// touch keys from the same set as the key 14
	c.Get(10)
	c.Get(12)

	// 14 will be evicted now
	if evictedKey, evictedVal, evicted := c.Set(16, 15); evictedKey != 14 || evictedVal != 14 || !evicted {
		t.Errorf("unexpected or unvecited entry: %d -> %d evicted: %v", evictedKey, evictedVal, evicted)
	}
}

func TestNWaysCacheSetGetVariousConfigurations(t *testing.T) {
	capacity := 1024
	for ways := 2; ways < capacity; ways *= 2 {
		t.Run(fmt.Sprintf("ways: %d", ways), func(t *testing.T) {
			c := NewNWaysCache[int, int](capacity, ways)
			for i := 0; i < capacity; i++ {
				if evictedKey, evictedVal, evicted := c.Set(i, i); evictedKey != 0 || evictedVal != 0 || evicted {
					t.Errorf("No value should be evicted when adding: %d", i)
				}
			}

			for want := 0; want < capacity; want++ {
				got, exists := c.Get(want)
				if !exists {
					t.Errorf("key should exist: %d", want)
				}
				if got != want {
					t.Errorf("values do not match: %d != %d", got, want)
				}
			}

		})
	}
}

func TestNWaysCacheSetGetMissingValuesVariousConfigurations(t *testing.T) {
	capacity := 1024
	for ways := 2; ways < capacity; ways *= 2 {
		t.Run(fmt.Sprintf("ways: %d", ways), func(t *testing.T) {
			c := NewNWaysCache[int, int](capacity, ways)
			for i := 0; i < capacity; i++ {
				if evictedKey, evictedVal, evicted := c.Set(i, i); evictedKey != 0 || evictedVal != 0 || evicted {
					t.Errorf("No value should be evicted when adding: %d", i)
				}
			}

			offset := 5000
			for want := offset; want < offset+capacity; want++ {
				got, exists := c.Get(want)
				if exists {
					t.Errorf("key should NOT exist: %d", want)
				}
				if got != 0 {
					t.Errorf("value shuold be empty: %d", got)
				}
			}
		})
	}
}

func TestNWaysCacheSetGetRandomKeysVariousConfigurations(t *testing.T) {
	capacity := 1024

	keys := make(map[int]int, capacity)
	for i := 0; i < capacity/2; i++ {
		keys[rand.Int()] = i
	}

	for ways := 2; ways < capacity; ways *= 2 {
		t.Run(fmt.Sprintf("ways: %d", ways), func(t *testing.T) {
			c := NewNWaysCache[int, int](capacity, ways)

			evictedKeys := make([]int, 0, capacity)
			for key, val := range keys {
				if evictedKey, _, evicted := c.Set(key, val); evicted {
					evictedKeys = append(evictedKeys, evictedKey)
					delete(keys, evictedKey)
				}
			}

			// all non evicted keys must be present
			for key, value := range keys {
				got, exists := c.Get(key)
				if !exists {
					t.Errorf("key should exist: %d", key)
				}
				if got != value {
					t.Errorf("values do not match: %d != %d", got, key)
				}
			}

			// evicted keys must not be available
			for _, key := range evictedKeys {
				got, exists := c.Get(key)
				if exists {
					t.Errorf("key should not exist: %d", key)
				}
				if got != 0 {
					t.Errorf("value should not be set: %d", got)
				}
			}

		})
	}
}

func TestNWaysCache_Concurrent_ReadsWrites(t *testing.T) {
	loops := 1000
	c := NewNWaysCache[int, int](1024, 16)

	// generate keys
	keys := make(map[int]bool, loops)
	for i := 0; i < loops; i++ {
		key := rand.Intn(2048)
		keys[key] = true
	}

	// multi-producer
	evictedKeys := make(map[int]bool, loops)
	var wg sync.WaitGroup
	var lock sync.Mutex
	for key := range keys {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			if evictedKey, _, evicted := c.Set(key, 1); evicted {
				lock.Lock()
				evictedKeys[evictedKey] = false
				lock.Unlock()
			}
		}(key)
	}

	wg.Wait()

	// multi consumer
	for key := range keys {
		go func(key int) {
			lock.Lock()
			_, evicted := evictedKeys[key]
			lock.Unlock()
			if _, exists := c.Get(key); !exists && !evicted {
				t.Errorf("key %d does not exist", key)
			}
		}(key)
	}
}

func TestNWaysCache_Concurrent_Sequence(t *testing.T) {
	const loops = 1000

	for _, withIter := range []bool{true, false} {
		t.Run(fmt.Sprintf("iter %v", withIter), func(t *testing.T) {
			c := NewNWaysCache[int, int](1024, 16)
			var wg sync.WaitGroup
			for i := 0; i < loops; i++ {
				wg.Add(1)
				go func(key, val int, withIter bool) {
					defer wg.Done()
					c.Set(key, val)
					// value does not have to exist, because it could have been evicted by another thread.
					// i.e. check only if the value exists
					if got, exists := c.Get(key); exists && val != got {
						t.Errorf("value inserted into cache does not match: %d != %d", got, val)
					}

					if got, present, _, _, _ := c.GetOrSet(key, val); present && val != got {
						t.Errorf("value inserted into cache does not match: %d != %d", got, val)
					}

					c.Remove(key)
					if _, exists := c.Get(key); exists {
						t.Errorf("removed value should not exist")
					}

					if withIter {
						c.Iterate(func(gotKey int, value int) bool {
							if key == gotKey {
								t.Errorf("removed value should not exist")
							}
							return true
						})
					}
				}(i, i*1000, withIter)
			}
			wg.Wait()
		})
	}

}
