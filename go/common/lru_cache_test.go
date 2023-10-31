package common

import (
	"testing"
)

func TestLruExceedCapacity(t *testing.T) {
	c := NewCache[int, int](3)

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

	evictedKey, evictedValue, evicted = c.Set(5, 44)
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
	c := NewCache[int, int](3)

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

func TestHitRatio(t *testing.T) {
	if !MissHitMeasuring {
		t.Skip("MissHitMeasuring is disabled - skipping the test")
	}

	c := NewCache[int, int](3)
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

func TestLRUCache_GetOrSet(t *testing.T) {
	c := NewCache[int, int](4)

	if _, present, _, _, evicted := c.GetOrSet(1, 11); present || evicted {
		t.Errorf("value should be neither present nor evicted")
	}

	if current, present, _, _, _ := c.GetOrSet(1, 12); !present || current != 11 {
		t.Errorf("previous value should be present")
	}

	// cause eviction
	c.Set(5, 5)
	c.Set(9, 9)
	c.Set(13, 13)

	if _, present, evictedKey, evictedValue, evicted := c.GetOrSet(17, 13); !evicted || present || evictedKey != 1 || evictedValue != 11 {
		t.Errorf("value should be evicted: %d != 1 || %d != 11", evictedKey, evictedValue)
	}

	// no eviction - replacing
	if current, present, _, _, evicted := c.GetOrSet(9, 13); evicted || !present || current != 9 {
		t.Errorf("value should be evicted: %d != 9", current)
	}

}
