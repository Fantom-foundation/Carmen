package btree

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestInsertWithinCapacity(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.Insert(4)
	n.Insert(1)
	n.Insert(3)
	n.Insert(2)

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getKeys(n))
}

func TestInsertDuplicities(t *testing.T) {
	n := newLeafNode[uint32](10, comparator)

	n.Insert(4)
	n.Insert(1)
	n.Insert(3)
	n.Insert(2)

	// will do nothing
	n.Insert(4)
	n.Insert(1)
	n.Insert(3)
	n.Insert(2)

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getKeys(n))
}

func TestInsertOverflowCapacity(t *testing.T) {
	n := newLeafNode[uint32](3, comparator)

	n.Insert(4)
	n.Insert(1)
	n.Insert(3)
	right, middle, split := n.Insert(2) // this will overflow

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

func TestInsertOverflowCapacityBinaryTree(t *testing.T) {
	n := newLeafNode[uint32](2, comparator)

	n.Insert(2)
	n.Insert(1)
	right, middle, split := n.Insert(3) // this will overflow

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

	n.Insert(4)
	n.Insert(1)
	n.Insert(3)

	if exists := n.Contains(1); !exists {
		t.Errorf("key should be found")
	}

	if exists := n.Contains(10); exists {
		t.Errorf("key should not be found")
	}
}
