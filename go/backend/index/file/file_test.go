package file

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
)

func TestFileIndexImplements(t *testing.T) {
	var inst Index[common.Address, uint32]
	var _ index.Index[common.Address, uint32] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestFileIndexGetAdd(t *testing.T) {
	inst, err := NewIndex[common.Address, uint32](t.TempDir(), common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	t.Cleanup(func() {
		_ = inst.Close()
	})
	if err != nil {
		t.Fatal("Cannot create instance", err)
	}

	if _, err := inst.Get(A); err != index.ErrNotFound {
		t.Errorf("Value must not exists")
	}

	if val, err := inst.GetOrAdd(A); err != nil || val != 0 {
		t.Errorf("Value must exists")
	}

	if val, err := inst.GetOrAdd(B); err != nil || val != 1 {
		t.Errorf("Value must exists")
	}

	if exists := inst.Contains(A); !exists {
		t.Errorf("Value must exists")
	}

	if exists := inst.Contains(B); !exists {
		t.Errorf("Value must exists")
	}
}

func TestFileHashIndexGetAddMany(t *testing.T) {
	inst, err := NewIndex[common.Address, uint32](t.TempDir(), common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	t.Cleanup(func() {
		_ = inst.Close()
	})
	if err != nil {
		t.Fatal("Cannot create instance", err)
	}

	data := make(map[common.Address]uint32)
	for i := 0; i < 999; i++ {
		n := rand.Intn(10000)
		key := common.AddressFromNumber(n)
		if _, err := inst.GetOrAdd(key); err != nil {
			t.Errorf("Value must exists")
		}

		// track unique values
		data[key], _ = inst.Get(key)
	}

	// check all vales present
	for expectKey, expectVal := range data {
		if actVal, err := inst.Get(expectKey); err != nil || actVal != expectVal {
			t.Errorf("Expected value does not match atual: %v -> %v != %v", expectKey, actVal, expectVal)
		}
	}

	// check +1 index
	key := common.AddressFromNumber(100009)
	expected := uint32(len(data))
	if idx, err := inst.GetOrAdd(key); err != nil || idx != expected {
		t.Errorf("Unexpected size: %d != %d", idx, expected)
	}
}

func TestFileHashIndexPersisted(t *testing.T) {
	numBuckets := 2 // only two init buckets to have many splits and evicted pages
	poolSize := 10
	dir := t.TempDir()

	inst, err := NewParamIndex[common.Address, uint32](dir, numBuckets, poolSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	t.Cleanup(func() {
		_ = inst.Close()
	})
	if err != nil {
		t.Fatal("Cannot create instance", err)
	}

	data := make(map[common.Address]uint32)
	max := 4096 * 10
	for i := 0; i < max; i++ {
		n := rand.Intn(10000)
		key := common.AddressFromNumber(n)
		if _, err := inst.GetOrAdd(key); err != nil {
			t.Errorf("Value must exists")
		}
		// track unique values
		data[key], _ = inst.Get(key)
	}

	// check all vales present
	for expectKey, expectVal := range data {
		if actVal, err := inst.Get(expectKey); err != nil || actVal != expectVal {
			t.Errorf("Expected value does not match atual: %v -> %v != %v", expectKey, actVal, expectVal)
		}
	}

	if size := inst.table.Size(); size != len(data) {
		t.Errorf("Size is not correct: %d != %d", size, len(data))
	}

	expectedNumBuckets := inst.table.GetNumBuckets()

	// re-open
	err = inst.Close()
	if err != nil {
		t.Fatal("Cannot close instance", err)
	}

	inst, err = NewIndex[common.Address, uint32](dir, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	t.Cleanup(func() {
		_ = inst.Close()
	})
	if err != nil {
		t.Fatal("Cannot create instance", err)
	}

	// test metadata propagated
	if size := inst.table.Size(); size != len(data) {
		t.Errorf("Size is not correct: %d != %d", size, len(data))
	}
	if actualBuckets := inst.table.GetNumBuckets(); actualBuckets != expectedNumBuckets {
		t.Errorf("Size is not correct: %d != %d", actualBuckets, len(data))
	}

	// check all vales present
	for expectKey, expectVal := range data {
		if actVal, err := inst.Get(expectKey); err != nil || actVal != expectVal {
			t.Errorf("Expected value does not match atual: %v -> %v != %v", expectKey, actVal, expectVal)
		}
	}

	// check +1 index
	key := common.AddressFromNumber(100009)
	expected := uint32(len(data))
	if idx, err := inst.GetOrAdd(key); err != nil || idx != expected {
		t.Errorf("Unexpected index: %d != %d", idx, expected)
	}

	// test metadata written
	hash, buckets, lastBucket, _, records, lastIndex, freeIds, err := readMetadata[uint32](dir, common.Identifier32Serializer{})
	if err != nil {
		t.Errorf("Cannot read metadata file: %s", err)
	}

	var emptyHash common.Hash
	if hash == emptyHash {
		t.Errorf("Hash is empty: %x", hash)
	}

	// default number of buckets
	if buckets != int(expectedNumBuckets) {
		t.Errorf("Wrong number of buckets: %d ", numBuckets)
	}

	if lastBucket == 0 {
		t.Errorf("No free Ids read")
	}

	if lastIndex != expected {
		t.Errorf("Last index wrong: %d |= %d", lastIndex, expected)
	}

	if len(freeIds) == 0 {
		t.Errorf("No free Ids read")
	}

	if records != len(data) {
		t.Errorf("Size is not correct: %d != %d", records, len(data))
	}
}

func TestFileHashMemoryFootprint(t *testing.T) {
	numBuckets := 2 // only two init buckets to have many splits and evicted pages
	poolSize := 10

	inst, err := NewParamIndex[common.Address, uint32](t.TempDir(), numBuckets, poolSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	t.Cleanup(func() {
		_ = inst.Close()
	})
	if err != nil {
		t.Fatal("Cannot create instance", err)
	}

	max := 4096 * 10
	for i := 0; i < max; i++ {
		n := rand.Intn(10000)
		key := common.AddressFromNumber(n)
		if _, err := inst.GetOrAdd(key); err != nil {
			t.Errorf("Value must exists")
		}
	}

	footPrint := inst.GetMemoryFootprint()
	if size := footPrint.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	hashIndex := footPrint.GetChild("hashIndex")
	if size := hashIndex.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	linearHash := footPrint.GetChild("linearHash")
	if size := linearHash.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	pagepool := footPrint.GetChild("pagePool")
	if size := pagepool.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	freeList := pagepool.GetChild("freeList")
	if freeList == nil {
		t.Errorf("Mem footprint wrong")
	}

	freeIds := pagepool.GetChild("freeIds")
	if freeIds == nil {
		t.Errorf("Mem footprint wrong")
	}

	pagePool := pagepool.GetChild("pagePool")
	if size := pagePool.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	pageStore := footPrint.GetChild("pageStore")
	if size := pageStore.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

	removedIds := pageStore.GetChild("removedIds")
	if size := removedIds.Value(); size == 0 {
		t.Errorf("Mem footprint wrong: %d", size)
	}

}
