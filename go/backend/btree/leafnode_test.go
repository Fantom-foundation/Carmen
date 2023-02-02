package btree

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestLeafNodeInsertWithinCapacity(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(4)
	n.insert(1)
	n.insert(3)
	n.insert(2)

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getKeys(n))
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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getKeys(n))
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
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getKeys(n))

	// check right
	common.AssertArraysEqual[uint32](t, []uint32{4}, getKeys(right))

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
	common.AssertArraysEqual[uint32](t, []uint32{1}, getKeys(n))

	// check right
	common.AssertArraysEqual[uint32](t, []uint32{3}, getKeys(right))

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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getNodeRange(n, 1, 5))

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5, 6, 7}, getNodeRange(n, 1, 100)) // above range

	// sub-range
	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 4, 5, 6}, getNodeRange(n, 2, 7))

	// not found
	common.AssertArraysEqual[uint32](t, []uint32{}, getNodeRange(n, 10, 100))
}

func TestLeafNodeNonConsecutiveGetRange(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.insert(7)
	n.insert(3)
	n.insert(6)
	n.insert(2)

	// - B C - - F G;  H I J
	common.AssertArraysEqual[uint32](t, []uint32{2, 3}, getNodeRange(n, 1, 5))

	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6, 7}, getNodeRange(n, 1, 100)) // above range

	// sub-range
	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6}, getNodeRange(n, 2, 7))

	// not found
	common.AssertArraysEqual[uint32](t, []uint32{}, getNodeRange(n, 10, 100))
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
	common.AssertArraysEqual[uint32](t, []uint32{3, 4}, getNodeRange(n, 1, 5))

	n.remove(4)
	if n.contains(4) {
		t.Errorf("key should not exist")
	}

	n.remove(3)
	if n.contains(3) {
		t.Errorf("key should not exist")
	}

}
