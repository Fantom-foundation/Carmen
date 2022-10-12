package index_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

func initIndexes(t *testing.T) (indexes []index.Index[common.Address, uint32]) {
	db, err := leveldb.OpenFile(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("failed to init leveldb; %s", err)
	}

	keySerializer := common.AddressSerializer{}
	idSerializer := common.Identifier32Serializer{}

	memindex := memory.NewIndex[common.Address, uint32](keySerializer)
	ldbindex, _ := ldb.NewIndex[common.Address, uint32](db, common.BalanceKey, keySerializer, idSerializer)

	t.Cleanup(func() {
		memindex.Close()
		ldbindex.Close()
		db.Close()
	})

	return []index.Index[common.Address, uint32]{memindex, ldbindex}
}

func TestIndexesInitialHash(t *testing.T) {
	indexes := initIndexes(t)

	for _, index := range indexes {
		hash, err := index.GetStateHash()
		if err != nil {
			t.Fatalf("failed to hash empty index; %s", err)
		}
		if hash != (common.Hash{}) {
			t.Errorf("invalid hash of empty index: %x (expected zeros)", hash)
		}
	}
}

func TestIndexesHashingByComparison(t *testing.T) {
	indexes := initIndexes(t)

	for i := 0; i < 10; i++ {
		ids := make([]uint32, len(indexes))
		for indexId, index := range indexes {
			var err error
			ids[indexId], err = index.GetOrAdd(common.Address{byte(0x20 + i)})
			if err != nil {
				t.Fatalf("failed to set index item %d; %s", i, err)
			}
		}
		if err := compareIds(ids); err != nil {
			t.Errorf("ids for item %d does not match: %s", i, err)
		}
		if err := compareHashes(indexes); err != nil {
			t.Errorf("hashes does not match after inserting item %d: %s", i, err)
		}
	}
}

func TestIndexesHashesAgainstReferenceOutput(t *testing.T) {
	indexes := initIndexes(t)

	// Tests the hashes for keys 0x01, 0x02 inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"ff9226e320b1deb7fabecff9ac800cd8eb1e3fb7709c003e2effcce37eec68ed",
		"c28553369c52e217564d3f5a783e2643186064498d1b3071568408d49eae6cbe",
	}

	for i, expectedHash := range expectedHashes {
		for _, index := range indexes {
			_, err := index.GetOrAdd(common.Address{byte(i + 1)}) // 0x01 - 0x02
			if err != nil {
				t.Fatalf("failed to set index item; %s", err)
			}
			hash, err := index.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash index; %s", err)
			}
			if expectedHash != fmt.Sprintf("%x", hash) {
				t.Fatalf("invalid hash: %x (expected %s)", hash, expectedHash)
			}
		}
	}
}

func compareHashes(indexes []index.Index[common.Address, uint32]) error {
	var firstHash common.Hash
	for i, index := range indexes {
		hash, err := index.GetStateHash()
		if err != nil {
			return err
		}
		if i == 0 {
			firstHash = hash
		} else if firstHash != hash {
			return fmt.Errorf("different hashes: %x != %x", firstHash, hash)
		}
	}
	return nil
}

func compareIds(ids []uint32) error {
	var firstId uint32
	for i, id := range ids {
		if i == 0 {
			firstId = id
		} else if firstId != id {
			return fmt.Errorf("different ids: %d != %d", firstId, id)
		}
	}
	return nil
}
