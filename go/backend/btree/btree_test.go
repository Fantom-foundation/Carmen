package btree

import (
	"fmt"
	"math/rand"
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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, getKeys(n))

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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, getKeys(n))

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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5, 6}, getKeys(n))

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

	common.AssertArraysEqual[uint32](t, []uint32{2, 3}, getTreeRange(n, 2, 4))

	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6, 7, 8}, getTreeRange(n, 1, 100)) // above range

	// sub-range
	common.AssertArraysEqual[uint32](t, []uint32{2, 3, 6}, getTreeRange(n, 2, 7))

	common.AssertArraysEqual[uint32](t, []uint32{3, 6, 7, 8}, getTreeRange(n, 3, 100))

	// not found
	common.AssertArraysEqual[uint32](t, []uint32{}, getTreeRange(n, 10, 100))
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

	common.AssertArraysEqual[uint32](t, []uint32{9, 13, 15, 21}, getTreeRange(n, 9, 26))

	common.AssertArraysEqual[uint32](t, []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54, 55, 86}, getTreeRange(n, 1, 100)) // above range

	// left subtree
	common.AssertArraysEqual[uint32](t, []uint32{1, 4, 7, 9, 13, 15, 21}, getTreeRange(n, 1, 26))

	// right subtree
	common.AssertArraysEqual[uint32](t, []uint32{26, 27, 29, 51, 52, 54, 55, 86}, getTreeRange(n, 26, 87))

	// not found
	common.AssertArraysEqual[uint32](t, []uint32{}, getTreeRange(n, 100, 200))
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

	expected := []uint32{1, 4, 7, 9, 13, 15, 21, 26, 27, 29, 51, 52, 54}
	common.AssertArraysEqual[uint32](t, expected, getTreeRange(n, 1, 500))

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

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5}, getKeys(n))

	// removal of non-existing cannot break properties
	n.Remove(999)
	if err := n.checkProperties(); err != nil {
		t.Errorf("%e", err)
	}

	n.Insert(6)
	if err := n.checkProperties(); err != nil {
		t.Errorf("%e", err)
	}

	common.AssertArraysEqual[uint32](t, []uint32{1, 2, 3, 4, 5, 6}, getKeys(n))
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
			common.AssertArraySorted[uint32](t, getKeys(n), comparator)
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
				common.AssertArraysEqual[uint32](t, data[start:end], keys)
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
			common.AssertArraySorted[uint32](t, getKeys(n), comparator)
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
