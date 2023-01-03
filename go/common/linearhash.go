package common

import (
	"fmt"
	"math"
	"sort"
)

// LinearHashHelper contains common methods to implement LinearHash
type LinearHashHelper[K comparable, V any] struct {
	hasher     Hasher[K]
	comparator Comparator[K]

	bits uint // number of bits in current hash mask
}

func NewLinearHashHelper[K comparable, V any](numBuckets int, hasher Hasher[K], comparator Comparator[K]) LinearHashHelper[K, V] {
	return LinearHashHelper[K, V]{hasher, comparator, log2(numBuckets)}
}

// GetBucketId compute collision bucket index for the input key and current number of buckets
func (h *LinearHashHelper[K, V]) GetBucketId(key K, numBuckets uint) uint {
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

// NextBucketId returns next bucket to split.
func (h *LinearHashHelper[K, V]) NextBucketId(numBuckets int) uint {
	return uint(numBuckets) % (1 << (h.bits - 1))
}

// SplitEntries divides input entries into two sets based on computation of the entry key hash
// and computing if the hash belongs to the input entry set or in a new one.
// Two new sets are created, i.e. the input set can be discarded.
// This method also increases the number of bits if the length of the collision bucket list exceeds the address space
// of current bit mask.
func (h *LinearHashHelper[K, V]) SplitEntries(numBuckets int, bucketId uint, oldEntries []MapEntry[K, V]) (entriesA, entriesB []MapEntry[K, V]) {
	// copy key-values pair to use in the new bucket
	entriesA = make([]MapEntry[K, V], 0, len(oldEntries))
	entriesB = make([]MapEntry[K, V], 0, len(oldEntries))

	// the number of buckets exceeds current bit mask, extend the mask
	nextNumBucket := uint(numBuckets + 1)
	if nextNumBucket > (1 << h.bits) {
		h.bits += 1
	}

	for _, entry := range oldEntries {
		if h.GetBucketId(entry.Key, nextNumBucket) == bucketId {
			entriesA = append(entriesA, entry)
		} else {
			entriesB = append(entriesB, entry)
		}
	}

	sort.Slice(entriesA, func(i, j int) bool { return h.comparator.Compare(&entriesA[i].Key, &entriesA[j].Key) < 0 })
	sort.Slice(entriesB, func(i, j int) bool { return h.comparator.Compare(&entriesB[i].Key, &entriesB[j].Key) < 0 })

	return entriesA, entriesB
}

func (h *LinearHashHelper[K, V]) ToString(k K, v V) string {
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
	return fmt.Sprintf("  %2v -> %3v hash: %64b, mask: %s", k, v, hash, mask)
}

func log2(x int) (y uint) {
	return uint(math.Ceil(math.Log2(float64(x))))
}
