package btree

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestNodeInsertWithinCapacity(t *testing.T) {

	left := newLeafNode[uint32](10, comparator)
	left.Insert(4)
	left.Insert(1)
	left.Insert(3)
	left.Insert(2)

	n := initNewInnerNode[uint32](left, nil, 5, 10, comparator)

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5}, getKeys(n))
}

func TestNodeInsertInnerNodeTwoLeaf(t *testing.T) {

	// it will create a right leaf that is split to this inner node
	left := newLeafNode[uint32](2, comparator)
	left.Insert(4)
	left.Insert(2)

	right := newLeafNode[uint32](2, comparator)
	n := initNewInnerNode[uint32](left, right, 5, 2, comparator)
	n.Insert(3)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	// insert at the end - it has to create end up in a leaf node
	n.Insert(6)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{6}, getLeafKeys(n.children[2]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	// the new right leaf node overflows into the parent inner node
	n.Insert(7)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{6, 7}, getLeafKeys(n.children[2]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	// iterate the whole tree
	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 4, 5, 6, 7}, getKeys(n))
}

func TestNodeInsertMultiLevelsTree(t *testing.T) {
	left := newLeafNode[uint32](2, comparator)
	left.Insert(1)
	left.Insert(2)

	right := newLeafNode[uint32](2, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 2, comparator)
	n.Insert(4)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3}, getInnerKeys(n))

	n.Insert(6)
	n.Insert(5)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{6}, getLeafKeys(n.children[2]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	n.Insert(7)

	upperRight, middle, split := n.Insert(8) // we have got a new left here

	if !split {
		t.Errorf("missing split")
	}

	// left subtree - original node
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getLeafKeys(n.children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{3}, getInnerKeys(n))

	// right subtree - the new node
	common.AssertArraysEqual[uint32](t, []uint32{6}, getLeafKeys(upperRight.(*InnerNode[uint32]).children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{8}, getLeafKeys(upperRight.(*InnerNode[uint32]).children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{7}, getInnerKeys(upperRight))

	// new left value
	if middle != 5 {
		t.Errorf("wrong middle value: %d", middle)
	}

	// test thw new sub-trees
	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4}, getKeys(n))
	common.AssertArraysEqual[uint32](t, []uint32{6, 7, 8}, getKeys(upperRight))
}

func TestInnerNodeContains(t *testing.T) {
	// fully fill
	left := newLeafNode[uint32](3, comparator)
	left.Insert(1)
	left.Insert(2)

	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)
	n.Insert(3)
	n.Insert(4)
	n.Insert(5)
	n.Insert(6)
	n.Insert(7)

	if exists := n.Contains(1); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(2); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(3); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(4); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(5); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(6); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.Contains(7); !exists {
		t.Errorf("key should be found")
	}

	if exists := n.Contains(10); exists {
		t.Errorf("key should not be found")
	}
}

func getLeafKeys(n Node[uint32]) []uint32 {
	leaf := n.(*LeafNode[uint32])
	return leaf.keys
}
func getInnerKeys(n Node[uint32]) []uint32 {
	inner := n.(*InnerNode[uint32])
	return inner.keys
}
