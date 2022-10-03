package ldb

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
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
	var persistent KVIndex[*common.Address]
	var _ index.Index[*common.Address, uint32] = &persistent
}

func TestBasicOperation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db := openDb(t, tmpDir)
	persistent, _ := NewKVIndex[common.Address](db, common.BalanceKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()

	indexA, err := persistent.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A; %s", err)
		return
	}
	if indexA != 0 {
		t.Errorf("first inserted is not 0")
		return
	}
	indexB, err := persistent.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed add of address B; %s", err)
		return
	}
	if indexB != 1 {
		t.Errorf("second inserted is not 1")
		return
	}

	if !persistent.Contains(A) {
		t.Errorf("persistent does not contains inserted A")
		return
	}
	if !persistent.Contains(B) {
		t.Errorf("persistent does not contains inserted B")
		return
	}

	indexA2, err := persistent.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed second add of address A; %s", err)
		return
	}
	if indexA != indexA2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}

	indexB2, err := persistent.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed second add of address B; %s", err)
		return
	}
	if indexB != indexB2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}
}

func TestDataPersisted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db := openDb(t, tmpDir)
	persistent, _ := NewKVIndex[common.Address](db, common.NonceKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()
	_, _ = persistent.GetOrAdd(A)
	_, _ = persistent.GetOrAdd(B)
	if !persistent.Contains(A) {
		t.Errorf("persistent does not contains inserted A")
		return
	}
	if !persistent.Contains(B) {
		t.Errorf("persistent does not contains inserted B")
		return
	}

	// close and reopen
	closeDb(t, db, persistent)
	db = openDb(t, tmpDir)
	persistent, _ = NewKVIndex[common.Address](db, common.NonceKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()

	// check the values are still there
	if !persistent.Contains(A) {
		t.Errorf("persistent does not contains inserted A")
		return
	}
	if !persistent.Contains(B) {
		t.Errorf("persistent does not contains inserted B")
		return
	}

	// third item gets ID = 3
	indexC, err := persistent.GetOrAdd(C)
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
	tmpDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db := openDb(t, tmpDir)
	persistent, _ := NewKVIndex[common.Address](db, common.SlotKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()

	// the hash is the default one first
	h0, _ := persistent.GetStateHash()

	if (h0 != common.Hash{}) {
		t.Errorf("The hash does not match the default one")
	}

	// the hash must change when adding a new item
	_, _ = persistent.GetOrAdd(A)
	ha1, _ := persistent.GetStateHash()

	if h0 == ha1 {
		t.Errorf("The hash has not changed")
	}

	// the hash remains the same when getting an existing item
	_, _ = persistent.GetOrAdd(A)
	ha2, _ := persistent.GetStateHash()

	if ha1 != ha2 {
		t.Errorf("The hash has changed")
	}

	// try recursive hash with B and already indexed A
	_, _ = persistent.GetOrAdd(B)
	hb1, _ := persistent.GetStateHash()

	// The hash must remain the same when adding still the same key
	_, _ = persistent.GetOrAdd(B)
	hb2, _ := persistent.GetStateHash()

	if hb1 != hb2 {
		t.Errorf("The hash has changed")
	}
}

func TestHashPersisted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db := openDb(t, tmpDir)
	persistent, _ := NewKVIndex[common.Address](db, common.ValueKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()
	_, _ = persistent.GetOrAdd(A)
	_, _ = persistent.GetOrAdd(B)

	// reopen
	closeDb(t, db, persistent)

	db = openDb(t, tmpDir)
	persistent, _ = NewKVIndex[common.Address](db, common.ValueKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()

	// hash must be still there
	h, _ := persistent.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

func TestHashPersistedAndAdded(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db := openDb(t, tmpDir)
	persistent, _ := NewKVIndex[common.Address](db, common.ValueKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()
	_, _ = persistent.GetOrAdd(A)

	// reopen
	closeDb(t, db, persistent)
	db = openDb(t, tmpDir)
	persistent, _ = NewKVIndex[common.Address](db, common.ValueKey, common.AddressSerializer{})
	defer func() {
		closeDb(t, db, persistent)
	}()

	// hash must be still there even when adding A and B in different sessions
	_, _ = persistent.GetOrAdd(B)
	h, _ := persistent.GetStateHash()

	if fmt.Sprintf("%x", h) != HashAB {
		t.Errorf("Hash is %x and not %s", h, HashAB)
	}
}

func openDb(t *testing.T, path string) (db *leveldb.DB) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Errorf("Cannot open DB, err: %s", err)
	}

	return
}

func closeDb[K common.Address](t *testing.T, db *leveldb.DB, p *KVIndex[K]) {
	_ = p.Close()
	_ = db.Close()
}
