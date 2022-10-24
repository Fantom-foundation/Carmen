package ldb

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"testing"
)

func TestLdbDepotImplements(t *testing.T) {
	var s Depot[uint32]
	var _ depot.Depot[uint32] = &s
	var _ io.Closer = &s
}

var (
	A = []byte{0xAA}
	B = []byte{0xBB, 0xBB}
	C = []byte{0xCC}
)

func TestStoringIntoLdbDepot(t *testing.T) {
	d := createNewDepot(t, openLevelDb(t))

	err := d.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = d.Set(1, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = d.Set(2, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := d.Get(5); value != nil {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := d.Get(0); !bytes.Equal(value, A) {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := d.Get(1); !bytes.Equal(value, B) {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := d.Get(2); !bytes.Equal(value, C) {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	d := createNewDepot(t, openLevelDb(t))

	err := d.Set(5, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = d.Set(4, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = d.Set(9, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := d.Get(1); value != nil {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := d.Get(5); !bytes.Equal(value, A) {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := d.Get(4); !bytes.Equal(value, B) {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := d.Get(9); !bytes.Equal(value, C) {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInLdbDepot(t *testing.T) {
	d := createNewDepot(t, openLevelDb(t))

	initialHast, err := d.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	err = d.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}

	newHash, err := d.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if initialHast == newHash {
		t.Errorf("setting into the depot have not changed the hash %x %x", initialHast, newHash)
	}
}

func openLevelDb(t *testing.T) (db *leveldb.DB) {
	db, err := leveldb.OpenFile(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Cannot open DB, err: %s", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return
}

func createNewDepot(t *testing.T, db *leveldb.DB) depot.Depot[uint32] {
	hashTreeFac := htldb.CreateHashTreeFactory(db, common.ValueKey, 3)
	d, err := NewDepot[uint32](db, common.ValueKey, common.Identifier32Serializer{}, hashTreeFac, 2)
	if err != nil {
		t.Fatalf("failed to create memory depot; %s", err)
	}
	t.Cleanup(func() {
		_ = d.Close()
	})
	return d
}
