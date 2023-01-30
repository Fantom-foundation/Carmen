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

func getKeys(n forEacher[uint32]) []uint32 {
	keys := make([]uint32, 0, 10)
	n.ForEach(func(k uint32) {
		keys = append(keys, k)
	})

	return keys
}

type forEacher[K any] interface {
	ForEach(callback func(k K))
}
