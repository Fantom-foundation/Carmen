package common

import (
	"testing"
)

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
	evictedKey, evictedValue, evicted := c.Set(3, 33)
	if evictedKey != 0 || evictedValue != 0 || evicted {
		t.Errorf("No items should have been evicted yet")
	}

	_, exists := c.Get(1) // one refreshed - first in the list now
	if exists == false {
		t.Errorf("Item should exist")
	}

	evictedKey, evictedValue, evicted = c.Set(4, 44)
	if evictedKey != 2 || evictedValue != 22 || evicted == false {
		t.Errorf("Incorrectly evicted items: %d/%d", evictedKey, evictedValue)
	}
	_, exists = c.Get(2) // 2 is the oldest in the table
	if exists {
		t.Errorf("Item should be evicted")
	}
}

// TestLRUOrder test correct ordering of the keys
func TestLRUOrder(t *testing.T) {
	c := initCache(3)

	c.Set(1, 11)
	c.Set(2, 22)
	c.Set(3, 33)

	_, _ = c.Get(1) // one refreshed - first in the list now
	if c.head.key != 1 {
		t.Errorf("Item should be head")
	}
	if c.tail.key != 2 {
		t.Errorf("Item should be tail")
	}

	c.Set(2, 222) // two refreshed - first in the list now
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

func TestCacheRemove(t *testing.T) {
	c := initCache(3)

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
}

func TestSettingExisting(t *testing.T) {
	c := initCache(3)

	c.Set(1, 11)
	c.Set(1, 67)

	value, exists := c.Get(1)
	if !exists || value != 67 {
		t.Errorf("Item value invalid")
	}
}

func TestHitRatio(t *testing.T) {
	if !MissHitMeasuring {
		t.Skip("MissHitMeasuring is disabled - skipping the test")
	}

	c := initCache(3)
	c.Set(1, 11)
	c.Set(2, 22)

	c.Get(1) // hit
	c.Get(8) // miss
	c.Get(2) // hit
	c.Get(9) // miss

	report := c.getHitRatioReport()
	if report != "(misses: 2, hits: 2, hitRatio: 0.500000)" {
		t.Errorf("unexpected memory footprint report: %s", report)
	}
}

func initCache(capacity int) *Cache[int, int] {
	return NewCache[int, int](capacity)
}
