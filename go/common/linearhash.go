package common

import (
	"fmt"
	"math"
	"sort"
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
	list []BulkInsertMap[K, V]

	records       uint // current total number of records in the whole table
	bits          uint // number of bits in current hash mask
	blockCapacity int  //maximal number of elements per block

	comparator Comparator[K]
	hasher     Hasher[K]
	factory    BulkInsertMapFactory[K, V] // factory creates a new bucket
}

// NewLinearHashMap creates a new instance with the initial number of buckets and constant bucket size.
// The number of buckets will grow as this table grows
func NewLinearHashMap[K comparable, V comparable](blockItems, numBuckets int, hasher Hasher[K], comparator Comparator[K], factory BulkInsertMapFactory[K, V]) *LinearHashMap[K, V] {
	list := make([]BulkInsertMap[K, V], numBuckets)
	for i := 0; i < numBuckets; i++ {
		list[i] = factory(i, blockItems)
	}
	bits := log2(numBuckets)

	return &LinearHashMap[K, V]{
		list:          list,
		bits:          bits,
		blockCapacity: blockItems,
		hasher:        hasher,
		comparator:    comparator,
		factory:       factory,
	}
}

// Put assigns the value to the input key.
func (h *LinearHashMap[K, V]) Put(key K, value V) error {
	bucketId := h.bucket(key, uint(len(h.list)))
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

func (h *LinearHashMap[K, V]) Add(key K, value V) error {
	bucketId := h.bucket(key, uint(len(h.list)))
	bucket := h.list[bucketId]
	beforeSize := bucket.Size()

	if err := bucket.Add(key, value); err != nil {
		return err
	}

	if beforeSize < bucket.Size() {
		h.records += 1
	}

	return h.checkSplit()
}

// Get returns value associated to the input key
func (h *LinearHashMap[K, V]) Get(key K) (value V, exists bool, err error) {
	bucket := h.bucket(key, uint(len(h.list)))
	value, exists, err = h.list[bucket].Get(key)
	return
}

func (h *LinearHashMap[K, V]) GetOrAdd(key K, val V) (value V, exists bool, err error) {
	bucket := h.bucket(key, uint(len(h.list)))
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

func (h *LinearHashMap[K, V]) GetAll(key K) ([]V, error) {
	bucket := h.bucket(key, uint(len(h.list)))
	return h.list[bucket].GetAll(key)
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
	bucket := h.bucket(key, uint(len(h.list)))
	exists, err := h.list[bucket].Remove(key)
	if err != nil {
		return exists, err
	}
	if exists {
		h.records -= 1
	}

	return exists, nil
}

func (h *LinearHashMap[K, V]) RemoveAll(key K) error {
	bucketId := h.bucket(key, uint(len(h.list)))
	bucket := h.list[bucketId]
	beforeSize := bucket.Size()

	if err := bucket.RemoveAll(key); err != nil {
		return err
	}

	// modify the number of records from the size diff in the bucket
	if beforeSize > bucket.Size() {
		h.records -= uint(beforeSize - bucket.Size())
	}

	return nil
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

func (h *LinearHashMap[K, V]) bucket(key K, numBuckets uint) uint {
	// get last bits of hash
	hashedKey := h.hasher.Hash(&key)
	m := uint(hashedKey & ((1 << h.bits) - 1))
	if m < numBuckets {
		return m
	} else {
		// unset the top bit when buckets overflow, i.e. do modulo
		return m ^ (1 << (h.bits - 1))
	}
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
	bucketId := uint(len(h.list)) % (1 << (h.bits - 1))
	oldBucket := h.list[bucketId]

	// the number of buckets exceeds current bit mask, extend the mask
	nextNumBucket := uint(len(h.list) + 1)
	if nextNumBucket > (1 << h.bits) {
		h.bits += 1
	}

	bucketA := h.factory(int(bucketId), h.blockCapacity)
	bucketB := h.factory(len(h.list), h.blockCapacity)

	// copy key-values pair to use in the new bucket
	entriesA := make([]MapEntry[K, V], 0, oldBucket.Size())
	entriesB := make([]MapEntry[K, V], 0, oldBucket.Size())

	oldEntries, err := oldBucket.GetEntries()
	if err != nil {
		return err
	}
	for _, entry := range oldEntries {
		if h.bucket(entry.Key, nextNumBucket) == bucketId {
			entriesA = append(entriesA, entry)
		} else {
			entriesB = append(entriesB, entry)
		}
	}

	// release resources
	if err := oldBucket.Clear(); err != nil {
		return err
	}

	sort.Slice(entriesA, func(i, j int) bool { return h.comparator.Compare(&entriesA[i].Key, &entriesA[j].Key) < 0 })
	sort.Slice(entriesB, func(i, j int) bool { return h.comparator.Compare(&entriesB[i].Key, &entriesB[j].Key) < 0 })

	if err := bucketA.BulkInsert(entriesA); err != nil {
		return err
	}

	if err := bucketB.BulkInsert(entriesB); err != nil {
		return err
	}

	// append the new one
	h.list[bucketId] = bucketA
	h.list = append(h.list, bucketB)

	return nil
}

func log2(x int) (y uint) {
	return uint(math.Ceil(math.Log2(float64(x))))
}

func (h *LinearHashMap[K, V]) PrintDump() {
	for i, v := range h.list {
		fmt.Printf("Bucket: %d\n", i)
		err := v.ForEach(func(k K, v V) {
			hash := h.hasher.Hash(&k)
			mask := ""
			for i := uint(0); i < h.bits; i++ {
				bit := (hash & (1 << i)) >> i
				if bit == 1 {
					mask = "1" + mask
				} else {
					mask = "0" + mask
				}
			}
			fmt.Printf("  %2v -> %3v hash: %64b, mask: %s \n", k, v, hash, mask)
		})
		if err != nil {
			fmt.Printf("error: %s", err)
		}
	}
}

func (h *LinearHashMap[K, V]) GetMemoryFootprint() *MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	var entrySize uintptr
	for _, item := range h.list {
		entrySize += item.GetMemoryFootprint().Value()
	}
	footprint := NewMemoryFootprint(selfSize)
	footprint.AddChild("buckets", NewMemoryFootprint(entrySize))
	return footprint
}
