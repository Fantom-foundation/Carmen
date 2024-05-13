// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package btree

import (
	"slices"
	"testing"
)

func TestLeafNodeInsertWithinCapacity(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)
	n.insert(2)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestLeafNodeInsertDuplicities(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)
	n.insert(2)

	// will do nothing
	n.insert(4)
	n.insert(1)
	n.insert(3)
	n.insert(2)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestLeafNodeInsertOverflowCapacity(t *testing.T) {
	n := newLeafNode[uint32](3, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)
	right, middle, split := n.insert(2) // this will overflow

	if !split {
		t.Errorf("node has not split.")
	}

	// check left
	if got, want := getKeys(n), []uint32{1, 2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// check right
	if got, want := getKeys(right), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// check middle
	if middle != 3 {
		t.Errorf("middle value wrong: %v != %v", middle, 3)
	}
}

func TestLeafNodeInsertOverflowCapacityBinaryTree(t *testing.T) {
	n := newLeafNode[uint32](2, comparator)

	n.insert(2)
	n.insert(1)
	right, middle, split := n.insert(3) // this will overflow

	if !split {
		t.Errorf("node has not split.")
	}

	// check left
	if got, want := getKeys(n), []uint32{1}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// check right
	if got, want := getKeys(right), []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// check middle
	if middle != 2 {
		t.Errorf("middle value wrong: %v != %v", middle, 3)
	}
}

func TestLeafNodeContains(t *testing.T) {
	n := newLeafNode[uint32](3, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)

	if exists := n.contains(1); !exists {
		t.Errorf("key should be found")
	}

	if exists := n.contains(10); exists {
		t.Errorf("key should not be found")
	}
}

func TestLeafNodeGetRange(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(7)
	n.insert(1)
	n.insert(5)
	n.insert(3)
	n.insert(4)
	n.insert(6)
	n.insert(2)

	if got, want := getNodeRange(n, 1, 5), []uint32{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getNodeRange(n, 1, 100), []uint32{1, 2, 3, 4, 5, 6, 7}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// sub-range
	if got, want := getNodeRange(n, 2, 7), []uint32{2, 3, 4, 5, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// not found
	if got, want := getNodeRange(n, 10, 100), []uint32{}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestLeafNodeNonConsecutiveGetRange(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(7)
	n.insert(3)
	n.insert(6)
	n.insert(2)

	// - B C - - F G;  H I J
	if got, want := getNodeRange(n, 1, 5), []uint32{2, 3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getNodeRange(n, 1, 100), []uint32{2, 3, 6, 7}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// sub-range
	if got, want := getNodeRange(n, 2, 7), []uint32{2, 3, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// not found
	if got, want := getNodeRange(n, 10, 100), []uint32{}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestLeafNodeRemove(t *testing.T) {
	n := newLeafNode[uint32](3, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)

	n.remove(1)
	if n.contains(1) {
		t.Errorf("key should not exist")
	}

	// check order preserved
	if got, want := getNodeRange(n, 1, 5), []uint32{3, 4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	n.remove(4)
	if n.contains(4) {
		t.Errorf("key should not exist")
	}

	n.remove(3)
	if n.contains(3) {
		t.Errorf("key should not exist")
	}

}
