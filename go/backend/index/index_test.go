package index

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"testing"
)

func compareHashes(indexA Index[common.Address, uint32], indexB Index[common.Address, uint32]) error {
	hashA, err := indexA.GetStateHash()
	if err != nil {
		return err
	}
	hashB, err := indexB.GetStateHash()
	if err != nil {
		return err
	}
	if hashA != hashB {
		return fmt.Errorf("different hashes: %x != %x", hashA, hashB)
	}
	return nil
}

func TestIndexesHashingByComparison(t *testing.T) {
	tmpLdbDir, err := os.MkdirTemp("", "leveldb-based-index-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpLdbDir)
	db, err := leveldb.OpenFile(tmpLdbDir, nil)
	if err != nil {
		t.Fatalf("failed to init leveldb; %s", err)
	}
	defer db.Close()

	memindex := memory.NewMemory[common.Address, uint32](common.AddressSerializer{})
	defer memindex.Close()
	ldbindex, _ := ldb.NewKVIndex[common.Address, uint32](db, common.BalanceKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	defer ldbindex.Close()

	if err := compareHashes(memindex, ldbindex); err != nil {
		t.Errorf("initial hash: %s", err)
	}

	for i := 0; i < 10; i++ {
		idxA, err := memindex.GetOrAdd(common.Address{byte(0x20 + i)})
		if err != nil {
			t.Fatalf("failed to set memstore item %d; %s", i, err)
		}
		idxB, err := ldbindex.GetOrAdd(common.Address{byte(0x20 + i)})
		if err != nil {
			t.Fatalf("failed to set filestore item %d; %s", i, err)
		}
		if idxA != idxB {
			t.Errorf("indexes does not match for inserted item %d: %d != %d", i, idxA, idxB)
		}
		if err := compareHashes(memindex, ldbindex); err != nil {
			t.Errorf("hash does not match after inserting item %d: %s", i, err)
		}
	}
}
