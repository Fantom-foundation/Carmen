package common

import (
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

func TestLinearHashIsMap(t *testing.T) {
	var instance LinearHashMap[Address, uint32]
	var _ Map[Address, uint32] = &instance
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
	h := NewLinearHashMap[Address, uint32](10, 128, AddressHasher{}, AddressComparator{})
	if h.helper.bits != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.helper.bits, 128)
	}

	// not exactly rounded
	h = NewLinearHashMap[Address, uint32](10, 120, AddressHasher{}, AddressComparator{})
	if h.helper.bits != 7 {
		t.Errorf("Num of bits %d is not Log2 of num of blocks %d", h.helper.bits, 120)
	}

}

func TestLinearHashGetSet(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(A, 10)
	h.Put(B, 20)

	if val, _ := h.Get(A); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(B); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(C); val != 0 {
		t.Errorf("Value is not correct")
	}
}

func TestLinearHashOverflow(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{})

	// fill-in all pages we have
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := AddressFromNumber(i + 1)
		h.Put(address, uint32(i+1))
	}

	// check properties are correct
	if h.helper.bits != log2(NumBuckets) {
		t.Errorf("Property is not correct %d", h.helper.bits)
	}
	if h.records != BucketSize*NumBuckets {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets {
		t.Errorf("Property is not correct %d", len(h.list))
	}

	//h.printDump()

	// check values properly set
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := AddressFromNumber(i + 1)
		if val, exists := h.Get(address); !exists || val != uint32(i+1) {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, AddressHasher{}.Hash(&address))
		}
	}

	// this will overflow!
	h.Put(A, 9999)

	//check properties are correct - number of buckets have increased
	if h.helper.bits != log2(NumBuckets+1) {
		t.Errorf("Property is not correct %d", h.helper.bits)
	}
	if h.records != BucketSize*NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets+1 {
		t.Errorf("Property is not correct %d", len(h.list))
	}

	//h.printDump()

	// check values properly set
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := AddressFromNumber(i + 1)
		if val, exists := h.Get(address); !exists || val != uint32(i+1) {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, AddressHasher{}.Hash(&address))
		}
	}

	if val, exists := h.Get(A); !exists || val != 9999 {
		t.Errorf("Value is not correct")
	}
}

func TestLinearHashGetOrAddOverflow(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{})

	// fill-in all pages we have
	for i := uint32(0); i < BucketSize*NumBuckets; i++ {
		address := AddressFromNumber(int(i + 1))
		h.GetOrAdd(address, i+1)
	}

	// check properties are correct
	if h.helper.bits != log2(NumBuckets) {
		t.Errorf("Property is not correct %d", h.helper.bits)
	}
	if h.records != BucketSize*NumBuckets {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets {
		t.Errorf("Property is not correct %d", len(h.list))
	}

	if size := h.Size(); size != BucketSize*NumBuckets {
		t.Errorf("Invalid size: %d", size)
	}

	// this will not overflow - key exists
	if val, exists := h.GetOrAdd(AddressFromNumber(1), 99999); !exists || val != 1 {
		t.Errorf("Wrong result: val: %d, exists: %v", val, exists)
	}

	// this will  overflow - a new key
	if val, exists := h.GetOrAdd(AddressFromNumber(9999), 99999); exists || val != 99999 {
		t.Errorf("Wrong result: val: %d, exists: %v", val, exists)
	}

	//check properties are correct - number of buckets have increased
	if h.helper.bits != log2(NumBuckets+1) {
		t.Errorf("Property is not correct %d", h.helper.bits)
	}
	if h.records != BucketSize*NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.records)
	}
	if len(h.list) != NumBuckets+1 {
		t.Errorf("Property is not correct %d", len(h.list))
	}
	if size := h.Size(); size != BucketSize*NumBuckets+1 {
		t.Errorf("Invalid size: %d", size)
	}
}

func TestLinearHashSize(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{})

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Put(AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("SizeBytes is not correct: %d != %d", size, n)
	}
}

func TestLinearHashRemove(t *testing.T) {
	h := NewLinearHashMap[Address, uint32](BucketSize, NumBuckets, AddressHasher{}, AddressComparator{})

	if exists := h.Remove(C); exists {
		t.Errorf("remove failed ")
	}

	h.Put(A, 10)
	if exists := h.Remove(A); !exists {
		t.Errorf("remove failed: %v ", A)
	}

	if size := h.Size(); size != 0 {
		t.Errorf("SizeBytes is wrong")
	}
}
