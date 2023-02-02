package btree

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestNodeInsertWithinCapacity(t *testing.T) {

	left := newLeafNode[uint32](10, comparator)
	left.insert(4)
	left.insert(1)
	left.insert(3)
	left.insert(2)

	n := initNewInnerNode[uint32](left, nil, 5, 10, comparator)

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5}, getKeys(n))
}

func TestNodeInsertInnerNodeTwoLeaf(t *testing.T) {

	// it will create a right leaf that is split to this inner node
	left := newLeafNode[uint32](2, comparator)
	left.insert(4)
	left.insert(2)

	right := newLeafNode[uint32](2, comparator)
	n := initNewInnerNode[uint32](left, right, 5, 2, comparator)
	n.insert(3)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	// insert at the end - it has to create end up in a leaf node
	n.insert(6)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{6}, getLeafKeys(n.children[2]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	// the new right leaf node overflows into the parent inner node
	n.insert(7)

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
	left.insert(1)
	left.insert(2)

	right := newLeafNode[uint32](2, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 2, comparator)
	n.insert(4)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3}, getInnerKeys(n))

	n.insert(6)
	n.insert(5)

	// left child
	common.AssertArraysEqual[uint32](t, []uint32{1, 2}, getLeafKeys(n.children[0]))

	// right child
	common.AssertArraysEqual[uint32](t, []uint32{4}, getLeafKeys(n.children[1]))
	common.AssertArraysEqual[uint32](t, []uint32{6}, getLeafKeys(n.children[2]))

	// parent
	common.AssertArraysEqual[uint32](t, []uint32{3, 5}, getInnerKeys(n))

	n.insert(7)

	upperRight, middle, split := n.insert(8) // we have got a new left here

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
	left.insert(1)
	left.insert(2)

	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)
	n.insert(3)
	n.insert(4)
	n.insert(5)
	n.insert(6)
	n.insert(7)

	if exists := n.contains(1); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(2); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(3); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(4); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(5); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(6); !exists {
		t.Errorf("key should be found")
	}
	if exists := n.contains(7); !exists {
		t.Errorf("key should be found")
	}

	if exists := n.contains(10); exists {
		t.Errorf("key should not be found")
	}
}

func TestInnerNodeIterator(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

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

func TestInnerNodeIteratorNextCalledOnly(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(7)
	n.insert(1)
	n.insert(5)
	n.insert(3)
	n.insert(4)
	n.insert(6)
	n.insert(2)

	it := newIterator[uint32](1, 5, n)

	expected := []uint32{1, 2, 3, 4}
	for _, expectedKey := range expected {
		if actualKey := it.Next(); expectedKey != actualKey {
			t.Errorf("Expected key does not match: %v != %v", expectedKey, actualKey)
		}
	}
}

func TestInnerNodeIteratorHasNextStable(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(7)
	n.insert(1)
	n.insert(5)
	n.insert(3)
	n.insert(4)
	n.insert(6)
	n.insert(2)

	it := newIterator[uint32](1, 5, n)

	expected := []uint32{1, 2, 3, 4}
	for _, expectedKey := range expected {
		// run hasNext a few times
		for i := 0; i < 10; i++ {
			if exists := it.HasNext(); !exists {
				t.Errorf("Key should exist")
			}
		}

		if actualKey := it.Next(); expectedKey != actualKey {
			t.Errorf("Expected key does not match: %v != %v", expectedKey, actualKey)
		}
	}
}

func TestInnerNodeIteratorHasNext(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(7)
	n.insert(1)
	n.insert(5)
	n.insert(3)
	n.insert(4)
	n.insert(6)
	n.insert(2)

	it := newIterator[uint32](1, 5, n)

	// HasNext() work
	if exists := it.HasNext(); !exists {
		t.Errorf("Next key should exist")
	}

	// iterate all
	for it.HasNext() {
		it.Next()
	}

	// HasNext() work
	if exists := it.HasNext(); exists {
		t.Errorf("Next key should not exist")
	}
}

func TestInnerNodeNonConsecutiveGetRange(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(7)
	n.insert(3)
	n.insert(6)
	n.insert(8)
	n.insert(2)

	common.AssertArraysEqual[uint32](t, []uint32{2, 3}, getNodeRange(n, 1, 5))

	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6, 7, 8}, getNodeRange(n, 1, 100)) // above range

	// sub-range
	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6}, getNodeRange(n, 2, 7))

	common.AssertArraysEqual[uint32](t, []uint32{3, 6, 7, 8}, getNodeRange(n, 3, 100))

	// not found
	common.AssertArraysEqual[uint32](t, []uint32{}, getNodeRange(n, 10, 100))
}

func TestInnerNodeRemovePred(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(2)
	n.insert(3)
	n.insert(4)
	n.insert(1)

	n.remove(3)
	common.AssertArraysEqual[uint32](t, []uint32{2}, n.keys)
	common.AssertArraysEqual[uint32](t, []uint32{1}, getKeys(n.children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{4}, getKeys(n.children[1]))
}

func TestInnerNodeRemoveSuc(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 2, 3, comparator)

	n.insert(1)
	n.insert(2)
	n.insert(3)
	n.insert(4)

	n.remove(2)
	common.AssertArraysEqual[uint32](t, []uint32{3}, n.keys)
	common.AssertArraysEqual[uint32](t, []uint32{1}, getKeys(n.children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{4}, getKeys(n.children[1]))
}

func TestInnerNodeRemoveRotateR(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 3, comparator)

	n.insert(2)
	n.insert(3)
	n.insert(4)
	n.insert(1)

	n.remove(4)
	common.AssertArraysEqual[uint32](t, []uint32{2}, n.keys)
	common.AssertArraysEqual[uint32](t, []uint32{1}, getKeys(n.children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{3}, getKeys(n.children[1]))
}

func TestInnerNodeRemoveRotateL(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 2, 3, comparator)

	n.insert(1)
	n.insert(2)
	n.insert(3)
	n.insert(4)

	n.remove(1)
	common.AssertArraysEqual[uint32](t, []uint32{3}, n.keys)
	common.AssertArraysEqual[uint32](t, []uint32{2}, getKeys(n.children[0]))
	common.AssertArraysEqual[uint32](t, []uint32{4}, getKeys(n.children[1]))
}

func TestInnerNodeRemoveMerge(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 2, 3, comparator)

	n.insert(1)
	n.insert(2)
	n.insert(3)

	n.remove(1)
	common.AssertArraysEqual[uint32](t, []uint32{}, n.keys)
	common.AssertArraysEqual[uint32](t, []uint32{2, 3}, getKeys(n.children[0]))
	// one child removed
	if len(n.children) != 1 {
		t.Errorf("wrong number of children: %d", len(n.children))
	}
}

func TestInnerNodeRemove(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 2, 3, comparator)

	n.insert(1)
	n.insert(2)
	n.insert(3)
	n.insert(4)
	n.insert(5)
	n.insert(6)
	n.insert(7)

	// keys are all there
	expected := []uint32{1, 2, 3, 4, 5, 6, 7}
	common.AssertArraysEqual[uint32](t, expected, getNodeRange(n, 1, 50))

	var root node[uint32] = n
	for _, key := range expected {
		root = root.remove(key)
		if exists := root.contains(key); exists {
			t.Errorf("key should not be found")
		}
	}
}

func TestInnerNodeThreeLevelsRemove(t *testing.T) {
	left := initNewInnerNode[uint32](newLeafNode[uint32](3, comparator), newLeafNode[uint32](3, comparator), 7, 3, comparator)
	right := initNewInnerNode[uint32](newLeafNode[uint32](3, comparator), newLeafNode[uint32](3, comparator), 52, 3, comparator)
	n := initNewInnerNode[uint32](left, right, 27, 3, comparator)

	n.insert(1)
	n.insert(4)
	n.insert(7)
	n.insert(9)
	n.insert(13)
	n.insert(15)
	n.insert(21)
	n.insert(26)
	n.insert(27)
	n.insert(29)
	n.insert(52)
	n.insert(51)
	n.insert(54)

	expected := []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54}
	common.AssertArraysEqual[uint32](t, expected, getNodeRange(n, 1, 500))

	n.remove(54) // rotate right middle level: 15 -> 27 -> 52 + rotate right: 51 -> 52 -> 54, and delete 54

	if str := n.String(); str != "[[[1, 4], 7, [9, 13]], 15, [[21, 26], 27, [29], 51, [52]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.remove(4)
	n.remove(1) // rotate right 9 -> 7 -> 1

	if str := n.String(); str != "[[[7], 9, [13], 15, [21, 26]], 27, [[29], 51, [52]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.remove(52) // rotate right middle level: 15 -> 27 -> 51 + merge 29, 51, 52, and delete 51

	if str := n.String(); str != "[[[7], 9, [13]], 15, [[21, 26], 27, [29, 51]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n = n.remove(26).(*InnerNode[uint32]) // merge 9, 15, 27 - it creates a new root

	if str := n.String(); str != "[[7], 9, [13], 15, [21], 27, [29, 51]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.remove(15) // merge 16, 15, 21

	if str := n.String(); str != "[[7], 9, [13, 21], 27, [29, 51]]" {
		t.Errorf("tree structure does not match: %v", str)
	}
}

func getLeafKeys(n node[uint32]) []uint32 {
	leaf := n.(*LeafNode[uint32])
	return leaf.keys
}
func getInnerKeys(n node[uint32]) []uint32 {
	inner := n.(*InnerNode[uint32])
	return inner.keys
}

func getNodeRange(n node[uint32], start, end uint32) []uint32 {
	keys := make([]uint32, 0, 10)
	it := newIterator[uint32](start, end, n)
	for it.HasNext() {
		keys = append(keys, it.Next())
	}

	return keys
}
