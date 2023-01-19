package file

import (
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

const (
	BucketSize = 3
	NumBuckets = 2
)

func TestLinearHashIsMap(t *testing.T) {
	var instance LinearHashMap[common.Address, uint32]
	var _ common.ErrMap[common.Address, uint32] = &instance
}

func TestLinearHashStableHashing(t *testing.T) {
	var prev uint64
	for i := 0; i < 100; i++ {
		curr := common.AddressHasher{}.Hash(&common.Address{0xAA})
		if prev != 0 && prev != curr {
			t.Errorf("Hash is not stable: %x != %x\n", prev, curr)
		}
		prev = curr
	}
}

func TestLinearHashGetSet(t *testing.T) {
	h := initLinearHashMap()

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
	h := initLinearHashMap()

	// fill-in all pages we have
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := common.AddressFromNumber(i + 1)
		_ = h.Put(address, uint32(i+1))
	}

	// check properties are correct
	if h.GetBits() != common.IntLog2(NumBuckets) {
		t.Errorf("Property is not correct %d", h.GetBits())
	}
	if h.size != BucketSize*NumBuckets {
		t.Errorf("Property is not correct %d", h.size)
	}
	if int(h.GetNumBuckets()) != NumBuckets {
		t.Errorf("Property is not correct %d", h.GetNumBuckets())
	}

	// check values properly set
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := common.AddressFromNumber(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != uint32(i+1) {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, common.AddressHasher{}.Hash(&address))
		}
	}

	//h.PrintDump()

	// this will overflow!
	_ = h.Put(A, 9999)

	//check properties are correct - number of buckets have increased
	if h.GetBits() != common.IntLog2(NumBuckets+1) {
		t.Errorf("Property is not correct %d", h.GetBits())
	}
	if h.size != BucketSize*NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.size)
	}
	if int(h.GetNumBuckets()) != NumBuckets+1 {
		t.Errorf("Property is not correct %d", int(h.GetNumBuckets()))
	}

	//h.PrintDump()

	// check values properly set
	for i := 0; i < BucketSize*NumBuckets; i++ {
		address := common.AddressFromNumber(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != uint32(i+1) {
			t.Errorf("Value incorrect: %v -> %d != %d  (hash: %x)", address, val, uint32(i+1), common.AddressHasher{}.Hash(&address))
		}
	}

	if val, exists, _ := h.Get(A); !exists || val != 9999 {
		t.Errorf("Value is not correct")
	}
}

func TestLinearHashGetOrAddOverflow(t *testing.T) {
	h := initLinearHashMap()

	// fill-in all pages we have
	for i := uint32(0); i < BucketSize*NumBuckets; i++ {
		address := common.AddressFromNumber(int(i + 1))
		_, _, _ = h.GetOrAdd(address, i+1)
	}

	// check properties are correct
	if h.GetBits() != common.IntLog2(NumBuckets) {
		t.Errorf("Property is not correct %d", h.GetBits())
	}
	if h.size != BucketSize*NumBuckets {
		t.Errorf("Property is not correct %d", h.size)
	}
	if int(h.GetNumBuckets()) != NumBuckets {
		t.Errorf("Property is not correct %d", h.GetNumBuckets())
	}

	if size := h.Size(); size != BucketSize*NumBuckets {
		t.Errorf("Invalid size: %d", size)
	}

	// this will not overflow - key exists
	if val, exists, _ := h.GetOrAdd(common.AddressFromNumber(1), 99999); !exists || val != 1 {
		t.Errorf("Wrong result: val: %d, exists: %v", val, exists)
	}

	// this will  overflow - a new key
	if val, exists, _ := h.GetOrAdd(common.AddressFromNumber(9999), 99999); exists || val != 99999 {
		t.Errorf("Wrong result: val: %d, exists: %v", val, exists)
	}

	//check properties are correct - number of buckets have increased
	if h.GetBits() != common.IntLog2(NumBuckets+1) {
		t.Errorf("Property is not correct %d", h.GetBits())
	}
	if h.size != BucketSize*NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.size)
	}
	if int(h.GetNumBuckets()) != NumBuckets+1 {
		t.Errorf("Property is not correct %d", h.GetNumBuckets())
	}
	if size := h.Size(); size != BucketSize*NumBuckets+1 {
		t.Errorf("Invalid size: %d", size)
	}
}

func TestLinearHashSize(t *testing.T) {
	h := initLinearHashMap()

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		_ = h.Put(common.AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestLinearHashRemove(t *testing.T) {
	h := initLinearHashMap()

	if exists, _ := h.Remove(C); exists {
		t.Errorf("remove failed ")
	}

	_ = h.Put(A, 10)
	if exists, _ := h.Remove(A); !exists {
		t.Errorf("remove failed: %v ", A)
	}

	if size := h.Size(); size != 0 {
		t.Errorf("Size is wrong")
	}
}

func initLinearHashMap() *LinearHashMap[common.Address, uint32] {
	pageFactory := PageNumKeysFactory[common.Address, uint32](maxItems, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	pagePool := pagepool.NewPagePool[*Page[common.Address, uint32]](pagePoolSize, nil, pagepool.NewMemoryPageStore(), pageFactory)
	return NewLinearHashMap[common.Address, uint32](maxItems, NumBuckets, 0, pagePool, common.AddressHasher{}, common.AddressComparator{})
}
