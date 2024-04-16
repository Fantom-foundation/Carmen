//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package btree

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

var (
	comparator = common.Uint32Comparator{}
)

const numKeys = 1000

func TestBTreeInsert(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(1)
	n.Insert(2)
	n.Insert(3)
	n.Insert(4)
	n.Insert(5)
	n.Insert(6)
	n.Insert(7)
	n.Insert(8)
	n.Insert(9)
	n.Insert(10)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	for _, key := range []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		if !n.Contains(key) {
			t.Errorf("key %d should be present", key)
		}
	}
}

func TestBTreeInsertUnsorted(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(8)
	n.Insert(3)
	n.Insert(5)
	n.Insert(6)
	n.Insert(7)
	n.Insert(4)
	n.Insert(9)
	n.Insert(10)
	n.Insert(2)
	n.Insert(1)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	for _, key := range []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		if !n.Contains(key) {
			t.Errorf("key %d should be present", key)
		}
	}
}

func TestBTreeInsertSplitSmaller(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(5)
	n.Insert(3)
	n.Insert(4)
	n.Insert(6)
	n.Insert(1)
	n.Insert(2)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	for _, key := range []uint32{1, 2, 3, 4, 5, 6} {
		if !n.Contains(key) {
			t.Errorf("key %d should be present", key)
		}
	}
}

func TestBTreeNonConsecutiveGetRange(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(7)
	n.Insert(3)
	n.Insert(6)
	n.Insert(8)
	n.Insert(2)

	if got, want := getTreeRange(n, 2, 4), []uint32{2, 3}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getTreeRange(n, 1, 100), []uint32{2, 3, 6, 7, 8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// sub-range
	if got, want := getTreeRange(n, 2, 7), []uint32{2, 3, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getTreeRange(n, 3, 100), []uint32{3, 6, 7, 8}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// not found
	if got, want := getTreeRange(n, 10, 100), []uint32{}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestBTreeMultiLevelGetRange(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(1)
	n.Insert(4)
	n.Insert(7)
	n.Insert(9)
	n.Insert(13)
	n.Insert(15)
	n.Insert(21)
	n.Insert(26)
	n.Insert(27)
	n.Insert(29)
	n.Insert(51)
	n.Insert(52)
	n.Insert(54)
	n.Insert(55)
	n.Insert(86)

	if got, want := getTreeRange(n, 9, 26), []uint32{9, 13, 15, 21}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	if got, want := getTreeRange(n, 1, 100), []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54, 55, 86}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// left subtree
	if got, want := getTreeRange(n, 1, 26), []uint32{1, 4, 7, 9, 13, 15, 21}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// right subtree
	if got, want := getTreeRange(n, 26, 87), []uint32{26, 27, 29, 51, 52, 54, 55, 86}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// not found
	if got, want := getTreeRange(n, 100, 200), []uint32{}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestBTreeMultiLevelHasNext(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(1)
	n.Insert(4)
	n.Insert(7)
	n.Insert(9)
	n.Insert(13)
	n.Insert(15)
	n.Insert(21)
	n.Insert(26)
	n.Insert(27)
	n.Insert(29)
	n.Insert(51)
	n.Insert(52)
	n.Insert(54)
	n.Insert(55)
	n.Insert(86)

	// left subtree
	it := n.NewIterator(1, 26)
	expected := []uint32{1, 4, 7, 9, 13, 15, 21}
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
	if exists := it.HasNext(); exists {
		t.Errorf("Key should not exist")
	}

	// right subtree
	it = n.NewIterator(26, 87)
	expected = []uint32{26, 27, 29, 51, 52, 54, 55, 86}
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
	if exists := it.HasNext(); exists {
		t.Errorf("Key should not exist")
	}
}

func TestBTreeThreeLevelsRemove(t *testing.T) {
	n := NewBTree[uint32](3, common.Uint32Comparator{})

	n.Insert(1)
	n.Insert(4)
	n.Insert(7)
	n.Insert(9)
	n.Insert(13)
	n.Insert(15)
	n.Insert(21)
	n.Insert(26)
	n.Insert(27)
	n.Insert(29)
	n.Insert(52)
	n.Insert(51)
	n.Insert(54)

	if str := n.String(); str != "[[[1, 4], 7, [9, 13], 15, [21, 26]], 27, [[29, 51], 52, [54]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	if got, want := getTreeRange(n, 1, 500), []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	n.Remove(54) // rotate right middle level: 15 -> 27 -> 52 + rotate right: 51 -> 52 -> 54, and delete 54

	if str := n.String(); str != "[[[1, 4], 7, [9, 13]], 15, [[21, 26], 27, [29], 51, [52]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(4)
	n.Remove(1) // rotate right 9 -> 7 -> 1

	if str := n.String(); str != "[[[7], 9, [13], 15, [21, 26]], 27, [[29], 51, [52]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(52) // rotate right middle level: 15 -> 27 -> 51 + merge 29, 51, 52, and delete 51

	if str := n.String(); str != "[[[7], 9, [13]], 15, [[21, 26], 27, [29, 51]]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(26) // merge 9, 15, 27 - it creates a new root

	if str := n.String(); str != "[[7], 9, [13], 15, [21], 27, [29, 51]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(15) // merge 13, 15, 21

	if str := n.String(); str != "[[7], 9, [13, 21], 27, [29, 51]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(21)
	n.Remove(51)
	n.Remove(9) // merge 7, 9, 13

	if str := n.String(); str != "[[7, 13], 27, [29]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(27) // use predecessor 27 -> 13

	if str := n.String(); str != "[[7], 13, [29]]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	n.Remove(7) // new root - one leaf

	if str := n.String(); str != "[13, 29]" {
		t.Errorf("tree structure does not match: %v", str)
	}

	// empty tree
	n.Remove(13)
	n.Remove(29)

	if str := n.String(); str != "[]" {
		t.Errorf("tree structure does not match: %v", str)
	}
}

func TestBTreeRemoveNonExisting(t *testing.T) {
	n := NewBTree[uint32](4, common.Uint32Comparator{})

	// fill-in leafs at min capacity
	n.Insert(1)
	n.Insert(2)
	n.Insert(3)
	n.Insert(4)
	n.Insert(5)

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}

	// removal of non-existing cannot break properties
	n.Remove(999)
	if err := n.checkProperties(); err != nil {
		t.Errorf("%e", err)
	}

	n.Insert(6)
	if err := n.checkProperties(); err != nil {
		t.Errorf("%e", err)
	}

	if got, want := getKeys(n), []uint32{1, 2, 3, 4, 5, 6}; !slices.Equal(got, want) {
		t.Errorf("slices are not equal: got: %v != want: %v", got, want)
	}
}

func TestBTreeInsertRemoveOrdered(t *testing.T) {
	widths := []int{3, 1 << 3, 1 << 5, 1 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity %d, items %d", width, numKeys), func(t *testing.T) {
			n := NewBTree[uint32](width, common.Uint32Comparator{})

			data := make([]uint32, 0, numKeys)
			for i := 0; i < numKeys; i++ {
				key := uint32(i)
				data = append(data, key)
				n.Insert(key)
			}

			if err := n.checkProperties(); err != nil {
				t.Errorf("tree properties check do not pass: %e", err)
			}

			// check all data inserted and sorted
			if !slices.IsSortedFunc(getKeys(n), func(a, b uint32) int {
				return comparator.Compare(&a, &b)
			}) {
				t.Errorf("array is not sorted: %v", getKeys(n))
			}

			for _, key := range data {
				if !n.Contains(key) {
					t.Errorf("key %d should be present", key)
				}

				// remove and check properties
				n.Remove(key)
				if err := n.checkProperties(); err != nil {
					t.Errorf("tree properties check do not pass: %e", err)
				}

			}

		})
	}
}

func TestBTreeGetRangeManyElements(t *testing.T) {
	widths := []int{2, 3, 1 << 3, 1 << 5, 1 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity %d, items %d", width, numKeys), func(t *testing.T) {
			n, data := initBTreeNonRepeatRandomKeys(width, numKeys)
			sort.Slice(data, func(i, j int) bool {
				return data[i] < data[j]
			})
			// test various intervals
			for i := 0; i < len(data); i += 10 {
				start := rand.Intn(i + 1)
				end := rand.Intn(len(data)-i) + i
				keys := getTreeRange(n, data[start], data[end])
				if got, want := data[start:end], keys; !slices.Equal(got, want) {
					t.Errorf("arrays not equal: got: %v != want: %v", got, want)
				}
			}

		})
	}
}

func TestBTreeLoadTest(t *testing.T) {
	widths := []int{2, 3, 1 << 3, 1 << 5, 1 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity %d, items %d", width, numKeys), func(t *testing.T) {
			n, data := initBTreeRandomKeys(width, numKeys)
			if err := n.checkProperties(); err != nil {
				t.Errorf("tree properties check do not pass: %e", err)
			}

			// check all data inserted and sorted
			if !slices.IsSortedFunc(getKeys(n), func(a, b uint32) int {
				return comparator.Compare(&a, &b)
			}) {
				t.Errorf("array is not sorted: %v", getKeys(n))
			}
			for _, key := range data {
				if !n.Contains(key) {
					t.Errorf("key %d should be present", key)
				}
			}
		})
	}
}

func TestBTreeRemove(t *testing.T) {
	widths := []int{3, 1 << 3, 1 << 5, 1 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity %d, items %d", width, numKeys), func(t *testing.T) {
			n, data := initBTreeNonRepeatRandomKeys(width, numKeys)

			if err := n.checkProperties(); err != nil {
				t.Errorf("tree properties check do not pass: %e", err)
			}

			// pick-up randomly keys from the input and delete until all keys are removed
			for len(data) > 0 {
				i := uint32(rand.Intn(len(data)))
				key := data[i]

				if !n.Contains(key) {
					t.Errorf("key %d should exist in the tree", i)
				}

				n.Remove(key)

				if n.Contains(key) {
					t.Errorf("key %d should not exist in the tree", i)
				}

				data = append(data[:i], data[i+1:]...)

				if err := n.checkProperties(); err != nil {
					t.Errorf("tree properties check do not pass: %e", err)
				}
			}

			// the remaining node must be an empty leaf
			if len(n.root.(*LeafNode[uint32]).keys) != 0 {
				t.Errorf("tree is not empty")
			}
		})
	}
}

// initBTreeRandomKeys creates a BTree with the given width of nodes.
// It initializes it with random numbers, with the size of numKeys
func initBTreeRandomKeys(width, numKeys int) (*BTree[uint32], []uint32) {
	n := NewBTree[uint32](width, common.Uint32Comparator{})

	data := make([]uint32, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		key := uint32(rand.Intn(10 * numKeys))
		data = append(data, key)
		n.Insert(key)
	}

	return n, data
}

// initBTreeNonRepeatRandomKeys creates a BTree with the given width of nodes.
// It initializes it with random numbers, with the size of numKeys.
// The random numbers do not repeat.
func initBTreeNonRepeatRandomKeys(width, numKeys int) (*BTree[uint32], []uint32) {
	n := NewBTree[uint32](width, common.Uint32Comparator{})

	data := make([]uint32, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		key := uint32(i*10 + rand.Intn(10)) // rand keys in tenth intervals
		data = append(data, key)
	}

	rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })

	for _, key := range data {
		n.Insert(key)
	}

	return n, data
}

func getKeys(n ForEacher[uint32]) []uint32 {
	keys := make([]uint32, 0, 10)
	n.ForEach(func(k uint32) {
		keys = append(keys, k)
	})

	return keys
}

func getTreeRange(n *BTree[uint32], start, end uint32) []uint32 {
	keys := make([]uint32, 0, 10)
	it := n.NewIterator(start, end)
	for it.HasNext() {
		keys = append(keys, it.Next())
	}

	return keys
}
