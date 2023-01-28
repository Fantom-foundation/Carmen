package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

var (
	comparator = common.Uint32Comparator{}
	numKeys    = 10000
)

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

func TestBTreeGetRangeManyElements(t *testing.T) {
	widths := []int{2, 3, 2 << 3, 2 << 5, 2 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity: %d, items: %d", width, numKeys), func(t *testing.T) {
			n := NewBTree[uint32](width, common.Uint32Comparator{})

			data := make([]uint32, 0, numKeys)
			for i := 0; i < numKeys; i++ {
				key := uint32(i*10 + rand.Intn(10)) // rand keys in tenth intervals
				data = append(data, key)
				n.Insert(key)
			}

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
	widths := []int{2, 3, 2 << 3, 2 << 5, 2 << 7}

	for _, width := range widths {
		t.Run(fmt.Sprintf("btree, capacity: %d, items: %d", width, numKeys), func(t *testing.T) {
			n := NewBTree[uint32](width, common.Uint32Comparator{})

			data := make([]uint32, 0, numKeys)
			for i := 0; i < numKeys; i++ {
				key := uint32(rand.Intn(10 * numKeys))
				data = append(data, key)
				n.Insert(key)
			}

			//fmt.Printf("Tree: %d\n%v\n\n", width, n)

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
