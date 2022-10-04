package store

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"testing"
)

const (
	PageSize = 2
	Factor   = 3
)

func compareHashes(storeA Store[uint32, common.Value], storeB Store[uint32, common.Value]) error {
	hashA, err := storeA.GetStateHash()
	if err != nil {
		return err
	}
	hashB, err := storeB.GetStateHash()
	if err != nil {
		return err
	}
	if hashA != hashB {
		return fmt.Errorf("different hashes: %x != %x", hashA, hashB)
	}
	return nil
}

func TestStoresHashingByComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	db, err := leveldb.OpenFile(tmpDir, nil)
	defer func() { _ = db.Close() }()

	defaultItem := common.Value{}
	serializer := common.ValueSerializer{}
	indexSerializer := common.IdentifierSerializer32[uint32]{}

	memstore := memory.NewStore[uint32, common.Value](serializer, defaultItem, PageSize, Factor)
	defer memstore.Close()
	filestore, err := file.NewStore[uint32, common.Value](tmpDir, serializer, defaultItem, PageSize, Factor)
	defer filestore.Close()
	levelStore, err := ldb.NewStore[uint32, common.Value](db, common.ValueKey, serializer, indexSerializer, ldb.CreateHashTreeFactory(db, common.ValueKey, Factor), defaultItem, PageSize)
	defer func() { _ = levelStore.Close() }()

	if err := compareHashes(memstore, filestore); err != nil {
		t.Errorf("initial hash: %s", err)
	}

	if err := compareHashes(memstore, levelStore); err != nil {
		t.Errorf("initial hash: %s", err)
	}

	for i := 0; i < 10; i++ {
		if err := memstore.Set(uint32(i), common.Value{byte(0x10 + i)}); err != nil {
			t.Fatalf("failed to set memstore item %d; %s", i, err)
		}
		if err := filestore.Set(uint32(i), common.Value{byte(0x10 + i)}); err != nil {
			t.Fatalf("failed to set filestore item %d; %s", i, err)
		}
		if err := levelStore.Set(uint32(i), common.Value{byte(0x10 + i)}); err != nil {
			t.Fatalf("failed to set filestore item %d; %s", i, err)
		}
		if err := compareHashes(memstore, filestore); err != nil {
			t.Errorf("hash does not match after inserting item %d: %s", i, err)
		}
		if err := compareHashes(memstore, levelStore); err != nil {
			t.Errorf("hash does not match after inserting item %d: %s", i, err)
		}
	}
}
