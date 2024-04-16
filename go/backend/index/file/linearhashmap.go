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

package file

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// LinearHashMap is a structure mapping a list of key/value pairs.
// It stores keys in buckets based on the hash computed ouf of the keys. The keys associate the values.
// The number of buckets increases on insert when the number of stored keys overflows the capacity of this map.
// In contrast to simple HashMaps, this structure grows by splitting one bucket into two when the capacity is exceeded,
// so the map does not have to be fully copied to a new bigger structure.
// The capacity is verified on each insert and potentially the split is triggered.
// It is inspired by: https://hackthology.com/linear-hashing.html#fn-5
type LinearHashMap[K comparable, V comparable] struct {
	pagePool *pagepool.PagePool[PageId, *IndexPage[K, V]]

	size         uint // total number of keys in the whole map
	pageCapacity int  //maximal number of elements per block

	comparator common.Comparator[K]
	common.LinearHashBase[K, V]
}

// NewLinearHashMap creates a new instance with the initial number of buckets and constant bucket size.
// The number of buckets will grow as this table grows.
func NewLinearHashMap[K comparable, V comparable](pageCapacity, numBuckets, size int, pagePool *pagepool.PagePool[PageId, *IndexPage[K, V]], hasher common.Hasher[K], comparator common.Comparator[K]) *LinearHashMap[K, V] {
	return &LinearHashMap[K, V]{
		pagePool:       pagePool,
		size:           uint(size),
		pageCapacity:   pageCapacity,
		comparator:     comparator,
		LinearHashBase: common.NewLinearHashBase[K, V](numBuckets, hasher, comparator),
	}
}

// Put assigns the value to the input key.
func (h *LinearHashMap[K, V]) Put(key K, value V) error {
	bucketId := h.GetBucketId(&key)
	bucket := h.pageList(bucketId)
	beforeSize := bucket.Size()

	if err := bucket.Put(key, value); err != nil {
		return err
	}

	if beforeSize < bucket.Size() {
		h.size += 1
	}

	// when the number of buckets overflows, split one bucket into two
	return h.checkSplit()
}

// Get returns value associated to the input key.
func (h *LinearHashMap[K, V]) Get(key K) (value V, exists bool, err error) {
	bucketId := h.GetBucketId(&key)
	bucket := h.pageList(bucketId)
	value, exists, err = bucket.Get(key)
	return
}

// GetOrAdd either returns a value stored under input key, or it associates the input value
// when the key is not stored yet.
// It returns true if the key was present, or false otherwise.
func (h *LinearHashMap[K, V]) GetOrAdd(key K, val V) (value V, exists bool, err error) {
	bucketId := h.GetBucketId(&key)
	bucket := h.pageList(bucketId)
	value, exists, err = bucket.GetOrAdd(key, val)
	if err != nil {
		return
	}
	if !exists {
		h.size += 1
		return value, exists, h.checkSplit()
	}
	return
}

// ForEach iterates all stored key/value pairs.
func (h *LinearHashMap[K, V]) ForEach(callback func(K, V)) error {
	for i := uint(0); i < h.GetNumBuckets(); i++ {
		bucket := h.pageList(i)
		if err := bucket.ForEach(callback); err != nil {
			return err
		}
	}

	return nil
}

// Remove deletes the key from the map and returns whether an element was removed.
func (h *LinearHashMap[K, V]) Remove(key K) (bool, error) {
	bucketId := h.GetBucketId(&key)
	bucket := h.pageList(bucketId)
	exists, err := bucket.Remove(key)
	if err != nil {
		return exists, err
	}
	if exists {
		h.size -= 1
	}

	return exists, nil
}

func (h *LinearHashMap[K, V]) Size() int {
	return int(h.size)
}

func (h *LinearHashMap[K, V]) Clear() error {
	h.size = 0
	for i := uint(0); i < h.GetNumBuckets(); i++ {
		bucket := h.pageList(i)
		if err := bucket.Clear(); err != nil {
			return err
		}
	}

	return nil
}

func (h *LinearHashMap[K, V]) checkSplit() (err error) {
	// when the number of buckets overflows, split one bucket into two
	if h.size > h.GetNumBuckets()*uint(h.pageCapacity) {
		err = h.split()
	}
	return err
}

// split creates a new bucket and extends the total number of buckets.
// It locates a bucket to split, extends the bit mask by adding one more bit
// and re-distribute keys between the old bucket and the new bucket.
func (h *LinearHashMap[K, V]) split() error {
	bucketId := h.NextBucketId()
	oldBucket := h.pageList(bucketId)

	oldEntries, err := oldBucket.GetEntries()
	if err != nil {
		return err
	}

	// release resources
	err = oldBucket.Clear()
	if err != nil {
		return err
	}

	entriesA, entriesB := h.SplitEntries(bucketId, oldEntries)

	err = InitPageList[K, V](int(bucketId), h.pageCapacity, h.pagePool, entriesA)
	if err != nil {
		return err
	}

	err = InitPageList[K, V](int(h.GetNumBuckets()-1), h.pageCapacity, h.pagePool, entriesB)
	if err != nil {
		return err
	}

	return nil
}

func (h *LinearHashMap[K, V]) PrintDump() {
	for i := uint(0); i < h.GetNumBuckets(); i++ {
		bucket := h.pageList(i)
		fmt.Printf("Bucket: %d\n", i)
		err := bucket.ForEach(func(k K, v V) {
			fmt.Printf("%s \n", h.ToString(k, v))
		})
		if err != nil {
			fmt.Printf("error: %s", err)
		}
	}
}

func (h *LinearHashMap[K, V]) pageList(bucket uint) PageList[K, V] {
	return NewPageList[K, V](int(bucket), h.pageCapacity, h.pagePool)
}

func (h *LinearHashMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	var entrySize uintptr
	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("buckets", common.NewMemoryFootprint(entrySize))
	footprint.AddChild("pagePool", h.pagePool.GetMemoryFootprint())
	return footprint
}
