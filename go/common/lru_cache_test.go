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
	"testing"
)

func TestLruExceedCapacity(t *testing.T) {
	c := NewLruCache[int, int](3)

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
	c := NewLruCache[int, int](3)

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

func TestLRUCache_GetOrSet(t *testing.T) {
	c := NewLruCache[int, int](4)

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

func TestCache_Entry_String(t *testing.T) {
	e := entry[int, int]{10, 20, nil, nil}

	if got, want := e.String(), "Entry: 10 -> 20"; got != want {
		t.Errorf("provided string does not match: %s != %s", got, want)
	}
}
