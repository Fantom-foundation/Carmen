// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"fmt"
	"math"
	"sort"
)

// LinearHashBase contains common methods to implement LinearHash
type LinearHashBase[K comparable, V any] struct {
	hasher     Hasher[K]
	comparator Comparator[K]

	bits, numBuckets uint // number of bits in current hash mask
}

func NewLinearHashBase[K comparable, V any](numBuckets int, hasher Hasher[K], comparator Comparator[K]) LinearHashBase[K, V] {
	return LinearHashBase[K, V]{hasher, comparator, IntLog2(numBuckets), uint(numBuckets)}
}

// GetBucketId compute collision bucket index for the input key and current number of buckets
func (h *LinearHashBase[K, V]) GetBucketId(key *K) uint {
	// get last bits of hash
	hashedKey := h.hasher.Hash(key)
	m := uint(hashedKey & ((1 << h.bits) - 1))
	if m < h.numBuckets {
		return m
	} else {
		// unset the top bit when buckets overflow, i.e. do modulo
		return m ^ (1 << (h.bits - 1))
	}
}

// NextBucketId returns next bucket to split.
func (h *LinearHashBase[K, V]) NextBucketId() uint {
	return uint(h.numBuckets) % (1 << (h.bits - 1))
}

// SplitEntries divides input entries into two sets based on computation of the entry key hash
// and computing if the hash belongs to the input entry set or in a new one.
// Two new sets are created, i.e. the input set can be discarded.
// This method also increases the number of bits if the length of the collision bucket list exceeds the address space
// of current bit mask.
func (h *LinearHashBase[K, V]) SplitEntries(bucketId uint, oldEntries []MapEntry[K, V]) (entriesA, entriesB []MapEntry[K, V]) {
	// copy key-values pair to use in the new bucket
	entriesA = make([]MapEntry[K, V], 0, len(oldEntries))
	entriesB = make([]MapEntry[K, V], 0, len(oldEntries))

	// the number of buckets exceeds current bit mask, extend the mask
	h.numBuckets += 1
	if h.numBuckets > (1 << h.bits) {
		h.bits += 1
	}

	for _, entry := range oldEntries {
		if h.GetBucketId(&entry.Key) == bucketId {
			entriesA = append(entriesA, entry)
		} else {
			entriesB = append(entriesB, entry)
		}
	}

	sort.Slice(entriesA, func(i, j int) bool { return h.comparator.Compare(&entriesA[i].Key, &entriesA[j].Key) < 0 })
	sort.Slice(entriesB, func(i, j int) bool { return h.comparator.Compare(&entriesB[i].Key, &entriesB[j].Key) < 0 })

	return entriesA, entriesB
}

func (h *LinearHashBase[K, V]) ToString(k K, v V) string {
	hash := h.hasher.Hash(&k)
	return fmt.Sprintf("  %2v -> %3v hash: %64b, mask: %b", k, v, hash, h.bits)
}

func (h *LinearHashBase[K, V]) GetBits() uint {
	return h.bits
}

func (h *LinearHashBase[K, V]) GetNumBuckets() uint {
	return h.numBuckets
}

func IntLog2(x int) (y uint) {
	return uint(math.Ceil(math.Log2(float64(x))))
}
