package common

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

const (
	BucketSize = 3
	NumBuckets = 2
)

var (
	A = Address{0xAA}
	B = Address{0xBB}
	C = Address{0xCC}
)

// mapFactory a factory for this hashmap
var mapFactory = func(bucket, capacity int) BulkInsertMap[Address, uint32] {
	return NewBlockList[Address, uint32](10, AddressComparator{})
}

func TestLinearHashIsMap(t *testing.T) {
	var instance LinearHashMap[Address, uint32]
	var _ ErrMap[Address, uint32] = &instance
}

func TestLinearHashStableHashing(t *testing.T) {
	var prev uint64
	for i := 0; i < 100; i++ {
		curr := AddressHasher{}.Hash(&Address{0xAA})
		if prev != 0 && prev != curr {
			t.Errorf("Hash is not stable: %x != %x\n", prev, curr)
		}
		prev = curr
	}
}

func TestLinearHashBitMask(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](10, 128, AddressHasher{}, AddressComparator{}, mapFactory)
	if h.bits != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.bits, 128)
	}

	// not exactly rounded
	h = NewLinearHashMap[Address, uint32](10, 120, AddressHasher{}, AddressComparator{}, mapFactory)
	if h.bits != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.bits, 120)
	}

}

func TestLinearHashGetSet(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{}, mapFactory)

	if _, exists, _ := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	_ = h.Put(A, 10)
	_ = h.Put(B, 20)

	if val, _, _ := h.Get(A); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _, _ := h.Get(B); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _, _ := h.Get(C); val != 0 {
		t.Errorf("Value is not correct")
	}
}

func TestLinearHashOverflow(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{}, mapFactory)

	// fill-in all pages we have
	for i := uint32(0); i < BucketSize*NumBuckets; i++ {
		address := toAddress(i + 1)
		_ = h.Put(address, i+1)
	}

	// check properties are correct
	if h.bits != log2(NumBuckets) {
		t.Errorf("Property is not correct %d", h.bits)
	}
	if h.records != BucketSize*NumBuckets {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets {
		t.Errorf("Property is not correct %d", len(h.list))
	}

	//h.PrintDump()

	// check values properly set
	for i := uint32(0); i < BucketSize*NumBuckets; i++ {
		address := toAddress(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != i+1 {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, AddressHasher{}.Hash(&address))
		}
	}

	// this will overflow!
	_ = h.Put(A, 9999)

	//check properties are correct - number of buckets have increased
	if h.bits != log2(NumBuckets+1) {
		t.Errorf("Property is not correct %d", h.bits)
	}
	if h.records != BucketSize*NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets+1 {
		t.Errorf("Property is not correct %d", len(h.list))
	}

	//h.PrintDump()

	// check values properly set
	for i := uint32(0); i < BucketSize*NumBuckets; i++ {
		address := toAddress(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != i+1 {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, AddressHasher{}.Hash(&address))
		}
	}

	if val, exists, _ := h.Get(A); !exists || val != 9999 {
		t.Errorf("Value is not correct")
	}
}

func TestLinearHashSize(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{}, mapFactory)

	n := rand.Intn(9999)
	for i := uint32(0); i < uint32(n); i++ {
		_ = h.Put(toAddress(i), i)
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestLinearHashRemove(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{}, mapFactory)

	if exists, _ := h.Remove(C); exists {
		t.Errorf("Remove failed ")
	}

	_ = h.Put(A, 10)
	if exists, _ := h.Remove(A); !exists {
		t.Errorf("Remove failed: %v ", A)
	}

	if size := h.Size(); size != 0 {
		t.Errorf("Size is wrong")
	}
}

func toAddress(num uint32) (address Address) {
	addr := binary.BigEndian.AppendUint32([]byte{}, num)
	copy(address[:], addr)
	return
}
