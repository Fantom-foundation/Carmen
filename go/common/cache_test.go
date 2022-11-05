package common

import "testing"

var removedKey, removedValue int

func TestEmpty(t *testing.T) {
	c := initCache(3)

	_, exists := c.Get(1)
	if exists {
		t.Errorf("Item should not exist")
	}

	_, exists = c.Get(2)
	if exists {
		t.Errorf("Item should not exist")
	}
}

func TestItemExist(t *testing.T) {
	c := initCache(3)

	c.Set(1, 33)
	val, exists := c.Get(1)
	if exists == false || val != 33 {
		t.Errorf("Item 33 should exist")
	}

	_, exists = c.Get(2)
	if exists {
		t.Errorf("Item should not exist")
	}
}

func TestExceedCapacity(t *testing.T) {
	c := initCache(3)

	c.Set(1, 11)
	c.Set(2, 22)
	c.Set(3, 33)

	_, exists := c.Get(1) // one refreshed - first in the cache now
	if exists == false {
		t.Errorf("Item should exist")
	}

	if removedKey != 0 || removedValue != 0 {
		t.Errorf("No item should have been evicted yet")
	}

	c.Set(4, 44)
	_, exists = c.Get(2) // 2 is the oldest in the cache
	if exists {
		t.Errorf("Item should be evicted")
	}
	if removedKey != 2 || removedValue != 22 {
		t.Errorf("Incorrectly evicted item: %d/%d", removedKey, removedValue)
	}
}

// TestLRUOrder test correct ordering of the keys
func TestLRUOrder(t *testing.T) {
	c := initCache(3)

	c.Set(1, 11)
	c.Set(2, 22)
	c.Set(3, 33)

	_, _ = c.Get(1) // one refreshed - first in the cache now
	if c.head.key != 1 {
		t.Errorf("Item should be head")
	}
	if c.tail.key != 2 {
		t.Errorf("Item should be tail")
	}

	c.Set(2, 222) // two refreshed - first in the cache now
	if c.head.key != 2 {
		t.Errorf("Item should be head")
	}
	if c.tail.key != 3 {
		t.Errorf("Item should be tail")
	}

	// insert exceeding and check order
	c.Set(4, 44)
	if c.head.key != 4 || c.head.next.key != 2 || c.head.next.next.key != 1 {
		t.Errorf("wrong order")
	}
	if c.tail.key != 1 || c.tail.prev.key != 2 || c.tail.prev.prev.key != 4 {
		t.Errorf("wrong order")
	}

}

func initCache(capacity int) *Cache[int, int] {
	return NewCache[int, int](capacity, func(key int, value int) {
		removedKey = key
		removedValue = value
	})
}
