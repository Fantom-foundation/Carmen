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

import "testing"

func TestLinearHashBitMask(t *testing.T) {
	h := NewLinearHashBase[uint32, uint32](128, directHash{}, Uint32Comparator{})
	if h.GetBits() != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.GetBits(), 128)
	}

	// not exactly rounded
	h = NewLinearHashBase[uint32, uint32](120, directHash{}, Uint32Comparator{})
	if h.GetBits() != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.GetBits(), 120)
	}
}

func TestLinearHashGetBucketId(t *testing.T) {
	h := NewLinearHashBase[uint32, uint32](3, directHash{}, Uint32Comparator{})

	key0 := uint32(0)
	if bucket := h.GetBucketId(&key0); bucket != 0 {
		t.Errorf("wrong bucket: %d", bucket)
	}
	key1 := uint32(1)
	if bucket := h.GetBucketId(&key1); bucket != 1 {
		t.Errorf("wrong bucket: %d", bucket)
	}
	key2 := uint32(2)
	if bucket := h.GetBucketId(&key2); bucket != 2 {
		t.Errorf("wrong bucket: %d", bucket)
	}
	// 3 = (bin) 11, unset top bit -> 01
	key3 := uint32(3)
	if bucket := h.GetBucketId(&key3); bucket != 1 {
		t.Errorf("wrong bucket: %d", bucket)
	}
}

func TestLinearHashNextBucketId(t *testing.T) {
	h := NewLinearHashBase[uint32, uint32](3, directHash{}, Uint32Comparator{})
	// 3 % 2 = 1 (2 bits mask)
	if bucket := h.NextBucketId(); bucket != 1 {
		t.Errorf("wrong bucket: %d", bucket)
	}
	// 65 % 64 = 1  (7 bits mask)
	h = NewLinearHashBase[uint32, uint32](65, directHash{}, Uint32Comparator{})
	if bucket := h.NextBucketId(); bucket != 1 {
		t.Errorf("wrong bucket: %d", bucket)
	}
}

func TestLinearHashSplitEntries(t *testing.T) {
	h := NewLinearHashBase[uint32, uint32](1, directHash{}, Uint32Comparator{})
	entries := make([]MapEntry[uint32, uint32], 0, 3)

	entries = append(entries, MapEntry[uint32, uint32]{0, 0}) // 00 - new bucket A
	entries = append(entries, MapEntry[uint32, uint32]{1, 0}) // 01 - new bucket B
	entries = append(entries, MapEntry[uint32, uint32]{2, 0}) // 10 - new bucket A
	entries = append(entries, MapEntry[uint32, uint32]{3, 0}) // 11 - new bucket B

	entriesA, entriesB := h.SplitEntries(0, entries)

	// num buckets and the bit mask have extended
	if h.GetBits() != 1 {
		t.Errorf("wrong bit mask %d", h.GetBits())
	}
	// one more bucket 2 -> 3
	if h.numBuckets != 2 {
		t.Errorf("wrong num of buckets %d", h.numBuckets)
	}

	// entries are sorted
	if entriesA[0].Key != uint32(0) || entriesA[1].Key != uint32(2) {
		t.Errorf("wrong entries: %v", entriesA)
	}

	if entriesB[0].Key != uint32(1) || entriesB[1].Key != uint32(3) {
		t.Errorf("wrong entries: %v", entriesA)
	}
}

func TestLinearHash_ToString(t *testing.T) {
	h := NewLinearHashBase[uint32, uint32](3, directHash{}, Uint32Comparator{})

	if got, want := h.ToString(123, 456), "  123 -> 456 hash:                                                          1111011, mask: 10"; got != want {
		t.Errorf("strings do not match: got: %v != want: %v", got, want)
	}
}

// directHash hashes the input number just returning the same number as the input
// i.e. no hashing is happening.
type directHash struct{}

func (h directHash) Hash(val *uint32) uint64 {
	return uint64(*val)
}
