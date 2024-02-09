package index_test

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func indexesFactories() map[string]func(t *testing.T) index.Index[common.Address, uint32] {

	keySerializer := common.AddressSerializer{}
	idSerializer := common.Identifier32Serializer{}

	// smaller number of buckets and page-pool size to have
	// some page evictions and bucket collisions
	numBuckets := 2
	pagePoolSize := 10

	return map[string]func(t *testing.T) index.Index[common.Address, uint32]{
		"memIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			return memory.NewIndex[common.Address, uint32](keySerializer)
		},
		"memLinearHashIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			return memory.NewLinearHashParamsIndex[common.Address, uint32](numBuckets, keySerializer, idSerializer, common.AddressHasher{}, common.AddressComparator{})
		},
		"ldbIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			db, err := backend.OpenLevelDb(t.TempDir(), nil)
			if err != nil {
				t.Fatalf("failed to init leveldb; %s", err)
			}
			idx, err := ldb.NewIndex[common.Address, uint32](db, common.BalanceStoreKey, keySerializer, idSerializer)
			if err != nil {
				t.Fatalf("failed to init index; %s", err)
			}
			t.Cleanup(func() {
				_ = idx.Close()
			})
			return idx
		},
		"fileIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			idx, err := file.NewParamIndex[common.Address, uint32](t.TempDir(), numBuckets, pagePoolSize, keySerializer, idSerializer, common.AddressHasher{}, common.AddressComparator{})
			if err != nil {
				t.Fatalf("failed to init index; %s", err)
			}
			t.Cleanup(func() {
				_ = idx.Close()
			})
			return idx
		},
	}
}

func TestIndexesLoadTest(t *testing.T) {
	n := 10000
	indexes := indexesFactories()

	// generate test data
	var data = make([]common.MapEntry[common.Address, uint32], 0, n)
	for i := 0; i < n; i++ {
		data = append(data, common.MapEntry[common.Address, uint32]{Key: common.AddressFromNumber(i), Val: uint32(i)})
	}

	var prevHash common.Hash

	for name, idxFactory := range indexes {
		t.Run(name, func(t *testing.T) {
			idx := idxFactory(t)

			// init data
			for _, entry := range data {
				if val, err := idx.GetOrAdd(entry.Key); err != nil || entry.Val != val {
					t.Errorf("Wrong value generated: %d != %d or err: %s", entry.Val, val, err)
				}
			}

			// check all data there by get
			for _, entry := range data {
				if val, err := idx.Get(entry.Key); err != nil || entry.Val != val {
					t.Errorf("Wrong value generated: %d != %d or err: %s", entry.Val, val, err)
				}
			}

			// check all data there by contains
			for _, entry := range data {
				if exists := idx.Contains(entry.Key); !exists {
					t.Errorf("Key not present %v", entry.Key)
				}
			}

			currentHash, err := idx.GetStateHash()
			if err != nil {
				t.Errorf("Cannot compute hash: %s", err)
			}

			// compare hashes with other indexes
			if (prevHash != common.Hash{}) {
				if prevHash != currentHash {
					t.Errorf("Hashes do not match: %x != %x", prevHash, currentHash)
				}
			}

			prevHash = currentHash
		})
	}

	if (prevHash == common.Hash{}) {
		t.Errorf("Hash did not cache")
	}

}
