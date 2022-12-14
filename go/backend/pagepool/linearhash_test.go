package pagepool_test

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

const (
	BlockItems = 3
	NumBuckets = 2
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
)

func TestLinearHashGetSet(t *testing.T) {
	h := initLinearHashWithPagePool()

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
	h := initLinearHashWithPagePool()

	// fill-in all pages we have
	for i := uint32(0); i < BlockItems*NumBuckets; i++ {
		address := toAddress(i + 1)
		_ = h.Put(address, i+1)
	}

	// check values properly set
	for i := uint32(0); i < BlockItems*NumBuckets; i++ {
		address := toAddress(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != i+1 {
			t.Errorf("Value incorrect: %v -> %d  (hash: %x)", address, val, common.AddressHasher{}.Hash(&address))
		}
	}

	// this will overflow!
	_ = h.Put(A, 9999)

	// check values properly set
	for i := uint32(0); i < BlockItems*NumBuckets; i++ {
		address := toAddress(i + 1)
		if val, exists, _ := h.Get(address); !exists || val != i+1 {
			t.Errorf("Value incorrect: %v -> %d  (hash: %b)", address, val, common.AddressHasher{}.Hash(&address))
		}
	}

	if val, exists, _ := h.Get(A); !exists || val != 9999 {
		t.Errorf("Value is not correct: %v -> %d", A, val)
	}
}

func TestLinearHashSize(t *testing.T) {
	h := initLinearHashWithPagePool()

	n := rand.Intn(9999)
	for i := uint32(0); i < uint32(n); i++ {
		_ = h.Put(toAddress(i), i)
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func toAddress(num uint32) (address common.Address) {
	addr := binary.BigEndian.AppendUint32([]byte{}, num)
	copy(address[:], addr)
	return
}

func initLinearHashWithPagePool() *common.LinearHashMap[common.Address, uint32] {
	pageListFactory := func(bucket, capacity int) common.BulkInsertMap[common.Address, uint32] {
		// two pages in the pool, two items each
		pagePoolSize := 2
		pageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
		pagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, BlockItems, nil, pageStore, common.AddressComparator{})
		return pagepool.NewPageList[common.Address, uint32](bucket, capacity, pagePool)
	}

	return common.NewLinearHashMap[common.Address, uint32](NumBuckets, BlockItems, common.AddressHasher{}, common.AddressComparator{}, pageListFactory)
}
