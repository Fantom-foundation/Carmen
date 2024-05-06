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

func TestNodeInsertWithinCapacity(t *testing.T) {

	left := newLeafNode[uint32](10, comparator)
	left.insert(4)
	left.insert(1)
	left.insert(3)
	left.insert(2)

	n := initNewInnerNode[uint32](left, nil, 5, 10, comparator)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := getLeafKeys(n.children[0]), []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right child
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// parent
	if got, want := getInnerKeys(n), []uint32{3, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// insert at the end - it has to create end up in a leaf node
	n.insert(6)

	// left child
	if got, want := getLeafKeys(n.children[0]), []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right child
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getLeafKeys(n.children[2]), []uint32{6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// parent
	if got, want := getInnerKeys(n), []uint32{3, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// the new right leaf node overflows into the parent inner node
	n.insert(7)

	// left child
	if got, want := getLeafKeys(n.children[0]), []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right child
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getLeafKeys(n.children[2]), []uint32{6, 7}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// parent
	if got, want := getInnerKeys(n), []uint32{3, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// iterate the whole tree
	if got, want := getKeys(n), []uint32{2, 3, 4, 5, 6, 7}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestNodeInsertMultiLevelsTree(t *testing.T) {
	left := newLeafNode[uint32](2, comparator)
	left.insert(1)
	left.insert(2)

	right := newLeafNode[uint32](2, comparator)
	n := initNewInnerNode[uint32](left, right, 3, 2, comparator)
	n.insert(4)

	// left child
	if got, want := getLeafKeys(n.children[0]), []uint32{1, 2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right child
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// parent
	if got, want := getInnerKeys(n), []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	n.insert(6)
	n.insert(5)

	// left child
	if got, want := getLeafKeys(n.children[0]), []uint32{1, 2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right child
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getLeafKeys(n.children[2]), []uint32{6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// parent
	if got, want := getInnerKeys(n), []uint32{3, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	n.insert(7)

	upperRight, middle, split := n.insert(8) // we have got a new left here

	if !split {
		t.Errorf("missing split")
	}

	// left subtree - original node
	if got, want := getLeafKeys(n.children[0]), []uint32{1, 2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getLeafKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getInnerKeys(n), []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right subtree - the new node
	if got, want := getLeafKeys(upperRight.(*InnerNode[uint32]).children[0]), []uint32{6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getLeafKeys(upperRight.(*InnerNode[uint32]).children[1]), []uint32{8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getInnerKeys(upperRight), []uint32{7}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// new left value
	if middle != 5 {
		t.Errorf("wrong middle value: %d", middle)
	}

	// test thw new sub-trees
	if got, want := getKeys(n), []uint32{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(upperRight), []uint32{6, 7, 8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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

	if got, want := getNodeRange(n, 1, 5), []uint32{2, 3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getNodeRange(n, 1, 100), []uint32{2, 3, 6, 7, 8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// sub-range
	if got, want := getNodeRange(n, 2, 7), []uint32{2, 3, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getNodeRange(n, 3, 100), []uint32{3, 6, 7, 8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// not found
	if got, want := getNodeRange(n, 10, 100), []uint32{}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := n.keys, []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[0]), []uint32{1}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := n.keys, []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[0]), []uint32{1}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := n.keys, []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[0]), []uint32{1}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[1]), []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := n.keys, []uint32{3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[0]), []uint32{2}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := getKeys(n.children[1]), []uint32{4}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestInnerNodeRemoveMerge(t *testing.T) {
	left := newLeafNode[uint32](3, comparator)
	right := newLeafNode[uint32](3, comparator)
	n := initNewInnerNode[uint32](left, right, 2, 3, comparator)

	n.insert(1)
	n.insert(2)
	n.insert(3)

	n.remove(1)
	if got, want := []uint32{}, n.keys; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
	if got, want := []uint32{2, 3}, getKeys(n.children[0]); !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
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
	if got, want := getNodeRange(n, 1, 50), expected; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

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

	if got, want := getNodeRange(n, 1, 500), []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

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
