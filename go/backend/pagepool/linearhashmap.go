package pagepool

import (
	"fmt"
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
	list     []*PageMapList[K, V]
	pagePool *PagePool[*KVPage[K, V]]

	records       uint // current total number of records in the whole table
	blockCapacity int  //maximal number of elements per block

	comparator common.Comparator[K]
	helper     common.LinearHashHelper[K, V]
}

// NewLinearHashMap creates a new instance with the initial number of buckets and constant bucket size.
// The number of buckets will grow as this table grows
func NewLinearHashMap[K comparable, V comparable](blockItems, numBuckets int, pagePool *PagePool[*KVPage[K, V]], hasher common.Hasher[K], comparator common.Comparator[K]) *LinearHashMap[K, V] {
	list := make([]*PageMapList[K, V], numBuckets)
	for i := 0; i < numBuckets; i++ {
		list[i] = NewPageMapList[K, V](i, blockItems, pagePool)
	}
	return &LinearHashMap[K, V]{
		list:          list,
		pagePool:      pagePool,
		blockCapacity: blockItems,
		comparator:    comparator,
		helper:        common.NewLinearHashHelper[K, V](numBuckets, hasher, comparator),
	}
}

// Put assigns the value to the input key.
func (h *LinearHashMap[K, V]) Put(key K, value V) error {
	bucketId := h.helper.GetBucketId(key, uint(len(h.list)))
	bucket := h.list[bucketId]
	beforeSize := bucket.Size()

	if err := bucket.Put(key, value); err != nil {
		return err
	}

	if beforeSize < bucket.Size() {
		h.records += 1
	}

	// when the number of buckets overflows, split one bucket into two
	return h.checkSplit()
}

// Get returns value associated to the input key
func (h *LinearHashMap[K, V]) Get(key K) (value V, exists bool, err error) {
	bucket := h.helper.GetBucketId(key, uint(len(h.list)))
	value, exists, err = h.list[bucket].Get(key)
	return
}

// GetOrAdd either returns a value stored under input key, or it associates the input value
// when the key is not stored yet.
// It returns true if the key was present, or false otherwise.
func (h *LinearHashMap[K, V]) GetOrAdd(key K, val V) (value V, exists bool, err error) {
	bucket := h.helper.GetBucketId(key, uint(len(h.list)))
	value, exists, err = h.list[bucket].GetOrAdd(key, val)
	if err != nil {
		return
	}
	if !exists {
		h.records += 1
		return value, exists, h.checkSplit()
	}
	return
}

// ForEach iterates all stored key/value pairs
func (h *LinearHashMap[K, V]) ForEach(callback func(K, V)) error {
	for _, v := range h.list {
		if err := v.ForEach(callback); err != nil {
			return err
		}
	}

	return nil
}

// Remove deletes the key from the map and returns whether an element was removed.
func (h *LinearHashMap[K, V]) Remove(key K) (bool, error) {
	bucket := h.helper.GetBucketId(key, uint(len(h.list)))
	exists, err := h.list[bucket].Remove(key)
	if err != nil {
		return exists, err
	}
	if exists {
		h.records -= 1
	}

	return exists, nil
}

func (h *LinearHashMap[K, V]) Size() int {
	return int(h.records)
}

func (h *LinearHashMap[K, V]) Clear() error {
	h.records = 0
	for _, v := range h.list {
		if err := v.Clear(); err != nil {
			return err
		}
	}

	return nil
}

// GetBuckets returns the number of buckets
func (h *LinearHashMap[K, V]) GetBuckets() int {
	return len(h.list)
}

func (h *LinearHashMap[K, V]) checkSplit() (err error) {
	// when the number of buckets overflows, split one bucket into two
	if h.records > uint(len(h.list))*uint(h.blockCapacity) {
		err = h.split()
	}
	return err
}

// split creates a new bucket and extends the total number of buckets.
// It locates a bucket to split, extends the bit mask by adding one more bit
// and re-distribute keys between the old bucket and the new bucket.
func (h *LinearHashMap[K, V]) split() error {
	bucketId := h.helper.NextBucketId(len(h.list))
	oldBucket := h.list[bucketId]

	oldEntries, err := oldBucket.GetEntries()
	if err != nil {
		return err
	}

	// release resources
	err = oldBucket.Clear()
	if err != nil {
		return err
	}

	entriesA, entriesB := h.helper.SplitEntries(len(h.list), bucketId, oldEntries)

	bucketA, err := InitPageMapList[K, V](int(bucketId), h.blockCapacity, h.pagePool, entriesA)
	if err != nil {
		return err
	}

	bucketB, err := InitPageMapList[K, V](len(h.list), h.blockCapacity, h.pagePool, entriesB)
	if err != nil {
		return err
	}

	// append the new one
	h.list[bucketId] = bucketA
	h.list = append(h.list, bucketB)

	return nil
}

func (h *LinearHashMap[K, V]) PrintDump() {
	for i, v := range h.list {
		fmt.Printf("Bucket: %d\n", i)
		err := v.ForEach(func(k K, v V) {
			fmt.Printf("%s \n", h.helper.ToString(k, v))
		})
		if err != nil {
			fmt.Printf("error: %s", err)
		}
	}
}

func (h *LinearHashMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	var entrySize uintptr
	for _, item := range h.list {
		entrySize += item.GetMemoryFootprint().Value()
	}
	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("buckets", common.NewMemoryFootprint(entrySize))
	footprint.AddChild("pagePool", h.pagePool.GetMemoryFootprint())
	return footprint
}