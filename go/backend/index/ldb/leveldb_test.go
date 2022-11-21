package ldb

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0x01}
	B = common.Address{0x02}
	C = common.Address{0x03}
)

const (
	HashAB = "c28553369c52e217564d3f5a783e2643186064498d1b3071568408d49eae6cbe"
)

func TestImplements(t *testing.T) {
	var persistent Index[*common.Address, uint32]
	var _ index.Index[*common.Address, uint32] = &persistent
}

func TestBasicOperation(t *testing.T) {
	db, _ := openIndexTempDb(t)
	idx := createIndex(t, db)

	indexA, err := idx.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A; %s", err)
		return
	}
	if indexA != 0 {
		t.Errorf("first inserted is not 0")
		return
	}
	indexB, err := idx.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed add of address B; %s", err)
		return
	}
	if indexB != 1 {
		t.Errorf("second inserted is not 1")
		return
	}

	if !idx.Contains(A) {
		t.Errorf("persistent does not contains inserted A")
		return
	}
	if !idx.Contains(B) {
		t.Errorf("persistent does not contains inserted B")
		return
	}
	if idx.Contains(C) {
		t.Errorf("persistent claims it contains non-existing C")
		return
	}
	if _, err := idx.Get(C); err != index.ErrNotFound {
		t.Errorf("persistent returns wrong error when getting non-existing")
		return
	}
}

func TestMultipleAssigningOfOneIndex(t *testing.T) {
	db, _ := openIndexTempDb(t)
	idx := createIndex(t, db)

	indexA, err := idx.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed adding of address A1; %s", err)
		return
	}

	indexA2, err := idx.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed adding of address A2; %s", err)
		return
	}
	if indexA != indexA2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}

	indexA3, err := idx.Get(A)
	if err != nil {
		t.Errorf("failed get id of address A3; %s", err)
		return
	}
	if indexA2 != indexA3 {
		t.Errorf("Get returns different value than GetOrAdd")
		return
	}
}

func TestDataPersisted(t *testing.T) {
	db, path := openIndexTempDb(t)
	idx1 := createIndex(t, db)

	_, _ = idx1.GetOrAdd(A)
	_, _ = idx1.GetOrAdd(B)

	if !idx1.Contains(A) {
		t.Errorf("persistent does not contains inserted A")
		return
	}
	if !idx1.Contains(B) {
		t.Errorf("idx1 does not contains inserted B")
		return
	}

	// close and reopen
	db = reopenIndexDb(t, idx1, db, path)
	idx2 := createIndex(t, db)

	// check the values are still there
	if !idx2.Contains(A) {
		t.Errorf("idx1 does not contains inserted A")
		return
	}
	if !idx2.Contains(B) {
		t.Errorf("idx1 does not contains inserted B")
		return
	}

	// third item gets ID = 3
	indexC, err := idx2.GetOrAdd(C)
	if err != nil {
		t.Errorf("failed add of address A; %s", err)
		return
	}
	if indexC != 2 {
		t.Errorf("third inserted is not 2")
		return
	}
}

func TestHash(t *testing.T) {
	db, _ := openIndexTempDb(t)
	idx := createIndex(t, db)

	// the hash is the default one first
	h0, _ := idx.GetStateHash()

	if (h0 != common.Hash{}) {
		t.Errorf("The hash does not match the default one")
	}

	// the hash must change when adding a new item
	_, _ = idx.GetOrAdd(A)
	ha1, _ := idx.GetStateHash()

	if h0 == ha1 {
		t.Errorf("The hash has not changed")
	}

	// the hash remains the same when getting an existing item
	_, _ = idx.GetOrAdd(A)
	ha2, _ := idx.GetStateHash()

	if ha1 != ha2 {
		t.Errorf("The hash has changed")
	}

	// try recursive hash with B and already indexed A
	_, _ = idx.GetOrAdd(B)
	hb1, _ := idx.GetStateHash()

	// The hash must remain the same when adding still the same key
	_, _ = idx.GetOrAdd(B)
	hb2, _ := idx.GetStateHash()

	if hb1 != hb2 {
		t.Errorf("The hash has changed")
	}
}

func TestHashPersisted(t *testing.T) {
	db, path := openIndexTempDb(t)
	idx1 := createIndex(t, db)

	_, _ = idx1.GetOrAdd(A)
	_, _ = idx1.GetOrAdd(B)

	// close and reopen
	db = reopenIndexDb(t, idx1, db, path)
	idx2 := createIndex(t, db)

	// hash must be still there
	h, _ := idx2.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

func TestHashPersistedAndAdded(t *testing.T) {
	db, path := openIndexTempDb(t)
	idx1 := createIndex(t, db)

	_, _ = idx1.GetOrAdd(A)

	// close and reopen
	db = reopenIndexDb(t, idx1, db, path)
	idx2 := createIndex(t, db)

	// hash must be still there even when adding A and B in different sessions
	_, _ = idx2.GetOrAdd(B)
	h, _ := idx2.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

// openIndexTempDb creates a new database on a new temp file
func openIndexTempDb(t *testing.T) (*common.LevelDbMemoryFootprintWrapper, string) {
	path := t.TempDir()
	return openIndexDb(t, path), path
}

// openIndexDb opends LevelDB on the input directory path
func openIndexDb(t *testing.T, path string) *common.LevelDbMemoryFootprintWrapper {
	db, err := common.OpenLevelDb(path, nil)
	if err != nil {
		t.Fatalf("Cannot open Db, err: %s", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

// reopenIndexDb closes database and the index from thw input index wrapper,
// and creates a new  database pointing to the same location
func reopenIndexDb(t *testing.T, idx index.Index[common.Address, uint32], db *common.LevelDbMemoryFootprintWrapper, path string) *common.LevelDbMemoryFootprintWrapper {
	if err := idx.Close(); err != nil {
		t.Errorf("Cannot close Index, err? %s", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("Cannot close DB, err? %s", err)
	}
	return openIndexDb(t, path)
}

// createIndex creates a new instance of the index using the input database
func createIndex(t *testing.T, db common.LevelDbWithMemoryFootprint) index.Index[common.Address, uint32] {
	idx, err := NewIndex[common.Address, uint32](db, common.BalanceStoreKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		t.Fatalf("Cannot open Index, err: %s", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}
