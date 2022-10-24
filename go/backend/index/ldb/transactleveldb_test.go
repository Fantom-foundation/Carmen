package ldb

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

func TestImplementsTr(t *testing.T) {
	var persistent TransactIndex[*common.Address, uint32]
	var _ index.Index[*common.Address, uint32] = &persistent
}

func TestTransactLevelGetSet(t *testing.T) {
	_, tr, _ := openTransactIndexTempDb(t)
	idx := createTransactIndex(t, tr)

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

func TestTransactGet(t *testing.T) {
	_, tr, _ := openTransactIndexTempDb(t)
	idx := createTransactIndex(t, tr)

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

func TestTransactDataPersisted(t *testing.T) {
	db, tr, path := openTransactIndexTempDb(t)
	idx1 := createTransactIndex(t, tr)

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
	_, tr = reopenTransactIndexDb(t, idx1, db, tr, path)
	idx2 := createTransactIndex(t, tr)

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

func TestTransactHash(t *testing.T) {
	_, tr, _ := openTransactIndexTempDb(t)
	idx := createTransactIndex(t, tr)

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

func TestTransactHashPersisted(t *testing.T) {
	db, tr, path := openTransactIndexTempDb(t)
	idx1 := createTransactIndex(t, tr)

	_, _ = idx1.GetOrAdd(A)
	_, _ = idx1.GetOrAdd(B)

	// close and reopen
	_, tr = reopenTransactIndexDb(t, idx1, db, tr, path)
	idx2 := createTransactIndex(t, tr)

	// hash must be still there
	h, _ := idx2.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

func TestTransactHashPersistedAndAdded(t *testing.T) {
	db, tr, path := openTransactIndexTempDb(t)
	idx1 := createTransactIndex(t, tr)

	_, _ = idx1.GetOrAdd(A)

	// close and reopen
	_, tr = reopenTransactIndexDb(t, idx1, db, tr, path)
	idx2 := createTransactIndex(t, tr)

	// hash must be still there even when adding A and B in different sessions
	_, _ = idx2.GetOrAdd(B)
	h, _ := idx2.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

// openIndexTempDb creates a new database on a new temp file
func openTransactIndexTempDb(t *testing.T) (*leveldb.DB, *leveldb.Transaction, string) {
	path := t.TempDir()
	db, tr := openTransactIndexDb(t, path)
	return db, tr, path
}

// openIndexDb opends LevelDB on the input directory path
func openTransactIndexDb(t *testing.T, path string) (*leveldb.DB, *leveldb.Transaction) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Cannot open Db, err: %s", err)
	}

	tr, err := db.OpenTransaction()
	if err != nil {
		t.Fatalf("Cannot open Db transaction, err: %s", err)
	}

	t.Cleanup(func() {
		_ = tr.Commit()
	})

	t.Cleanup(func() { _ = db.Close() })

	return db, tr
}

// reopenIndexDb closes database and the index from thw input index wrapper,
// and creates a new  database pointing to the same location
func reopenTransactIndexDb(t *testing.T, idx index.Index[common.Address, uint32], db *leveldb.DB, tr *leveldb.Transaction, path string) (*leveldb.DB, *leveldb.Transaction) {
	if err := idx.Close(); err != nil {
		t.Errorf("Cannot close TransactIndex, err? %s", err)
	}
	_ = tr.Commit()
	if err := db.Close(); err != nil {
		t.Errorf("Cannot close DB, err? %s", err)
	}
	return openTransactIndexDb(t, path)
}

// createIndex creates a new instance of the index using the input database
func createTransactIndex(t *testing.T, tr *leveldb.Transaction) index.Index[common.Address, uint32] {
	idx, err := NewTransactIndex[common.Address, uint32](tr, common.BalanceKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		t.Fatalf("Cannot open TransactIndex, err: %s", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}
