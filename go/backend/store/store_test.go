package store_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile/eviction"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"testing"
)

// test stores parameters (different from benchmark stores parameters)
const (
	BranchingFactor = 3
	PageSize        = 2 * 32
	PoolSize        = 10
)

type storeFactory struct {
	label    string
	getStore func(tempDir string) store.Store[uint32, common.Value]
}

func getStoresFactories(tb testing.TB, branchingFactor int, pageSize int, poolSize int) (stores []storeFactory) {
	return []storeFactory{
		{
			label: "Memory",
			getStore: func(tempDir string) store.Store[uint32, common.Value] {
				hashTreeFac := htmemory.CreateHashTreeFactory(branchingFactor)
				str, err := memory.NewStore[uint32, common.Value](common.ValueSerializer{}, pageSize, hashTreeFac)
				if err != nil {
					tb.Fatalf("failed to init memory store; %s", err)
				}
				return str
			},
		},
		{
			label: "File",
			getStore: func(tempDir string) store.Store[uint32, common.Value] {
				hashTreeFac := htfile.CreateHashTreeFactory(tempDir, branchingFactor)
				str, err := file.NewStore[uint32, common.Value](tempDir, common.ValueSerializer{}, pageSize, hashTreeFac)
				if err != nil {
					tb.Fatalf("failed to init file store; %s", err)
				}
				return str
			},
		},
		{
			label: "PagedFile",
			getStore: func(tempDir string) store.Store[uint32, common.Value] {
				hashTreeFac := htfile.CreateHashTreeFactory(tempDir, branchingFactor)
				str, err := pagedfile.NewStore[uint32, common.Value](tempDir, common.ValueSerializer{}, int64(pageSize), hashTreeFac, poolSize, eviction.NewLRUPolicy(poolSize))
				if err != nil {
					tb.Fatalf("failed to init pagedfile store; %s", err)
				}
				return str
			},
		},
		{
			label: "LevelDb",
			getStore: func(tempDir string) store.Store[uint32, common.Value] {
				db, err := leveldb.OpenFile(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				hashTreeFac := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, branchingFactor)
				str, err := ldb.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTreeFac, pageSize)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				return &storeClosingWrapper{str, []func() error{db.Close}}
			},
		},
		{
			label: "TransactLevelDb",
			getStore: func(tempDir string) store.Store[uint32, common.Value] {
				writeBufferSize := 1024 * opt.MiB
				opts := opt.Options{WriteBuffer: writeBufferSize}
				db, err := leveldb.OpenFile(tempDir, &opts)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}

				tr, err := db.OpenTransaction()
				if err != nil {
					tb.Errorf("failed to init leveldb transaction; %s", err)
				}

				hashTreeFac := htldb.CreateHashTreeFactory(tr, common.ValueStoreKey, branchingFactor)
				str, err := ldb.NewStore[uint32, common.Value](tr, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTreeFac, pageSize)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}

				return &storeClosingWrapper{str, []func() error{tr.Commit, db.Close}}
			},
		},
	}
}

// storeClosingWrapper wraps an instance of the Store to clean-up related resources when the Store is closed
type storeClosingWrapper struct {
	store.Store[uint32, common.Value]
	cleanups []func() error
}

// Close executes clean-up
func (s *storeClosingWrapper) Close() error {
	for _, f := range s.cleanups {
		_ = f()
	}
	return s.Store.Close()
}

func TestStoresInitialHash(t *testing.T) {
	for _, factory := range getStoresFactories(t, BranchingFactor, PageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			s := factory.getStore(t.TempDir())
			defer s.Close()

			hash, err := s.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash empty store; %s", err)
			}
			if hash != (common.Hash{}) {
				t.Errorf("invalid hash of empty store: %x (expected zeros)", hash)
			}

		})
	}
}

func TestStoresHashingByComparison(t *testing.T) {
	stores := make(map[string]store.Store[uint32, common.Value])
	for _, factory := range getStoresFactories(t, BranchingFactor, PageSize, PoolSize) {
		stores[factory.label] = factory.getStore(t.TempDir())
	}
	defer func() {
		for _, d := range stores {
			_ = d.Close()
		}
	}()

	for i := 0; i < 10; i++ {
		for _, store := range stores {
			if err := store.Set(uint32(i), common.Value{byte(0x10 + i)}); err != nil {
				t.Fatalf("failed to set store item %d; %s", i, err)
			}
		}
		if err := compareHashes(stores); err != nil {
			t.Errorf("stores hashes does not match after inserting item %d: %s", i, err)
		}
	}
}

func TestStoresHashesAgainstReferenceOutput(t *testing.T) {
	// Tests the hashes for values 0x00, 0x11 ... 0xFF inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"f5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b",
		"967293ee9d7ba679c3ef076bef139e2ceb96d45d19a624cc59bb5a3c1649ce38",
		"37617dfcbf34b6bd41ef1ba985de1e68b69bf4e42815981868abde09e9e09f0e",
		"735e056698bd4b4953a9838c4526c4d2138efd1aee9a94ff36ca100f16a77581",
		"c1e116b85f59f2ef61d6a64e61947e33c383f0adf252a3249b6172286ca244aa",
		"6001791dfa74121b9d177091606ebcd352e784ecfab05563c40b7ce8346c6f98",
		"57aee44f007524162c86d8ab0b1c67ed481c44d248c5f9c48fca5a5368d3a705",
		"dd29afc37e669458a3f4509023bf5a362f0c0cdc9bb206a6955a8f5124d26086",
		"0ab5ad3ab4f3efb90994cdfd72b2aa0532cc0f9708ea8fb8555677053583e161",
		"901d25766654678c6fe19c3364f34f9ed7b649514b9b5b25389de3bbfa346957",
		"50743156d6a4967c165a340166d31ca986ceebbb1812aebb3ce744ce7cffaa99",
		"592fd0da56dbc41e7ae8d4572c47fe12492eca9ae68b8786ebc322c2e2d61de2",
		"bc57674bfa2b806927af318a51025d833f5950ed6cdab5af3c8a876dac5ba1c4",
		"6523527158ccde9ed47932da61fed960019843f31f1fdbab3d18958450a00e0f",
		"e1bf187a4cd645c7adae643070f070dcb9c4aa8bbc0aded07b99dda3bac6b0ea",
		"9a5be401e5aa0b2b31a3b055811b15041f4842be6cd4cb146f3c2b48e2081e19",
		"6f060e465bb1b155a6b4822a13b704d3986ab43d7928c14b178e07a8f7673951",
	}

	for _, factory := range getStoresFactories(t, BranchingFactor, PageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			s := factory.getStore(t.TempDir())
			defer s.Close()

			for i, expectedHash := range expectedHashes {
				if err := s.Set(uint32(i), common.Value{byte(i<<4 | i)}); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}
				hash, err := s.GetStateHash()
				if err != nil {
					t.Fatalf("failed to hash store with %d values; %s", i+1, err)
				}
				if expectedHash != fmt.Sprintf("%x", hash) {
					t.Errorf("invalid hash: %x (expected %s)", hash, expectedHash)
				}
			}
		})
	}
}

func compareHashes(stores map[string]store.Store[uint32, common.Value]) error {
	var firstHash common.Hash
	var firstLabel string
	for label, store := range stores {
		hash, err := store.GetStateHash()
		if err != nil {
			return err
		}
		if firstHash == (common.Hash{}) {
			firstHash = hash
			firstLabel = label
		} else if firstHash != hash {
			return fmt.Errorf("different hashes: %s(%x) != %s(%x)", firstLabel, firstHash, label, hash)
		}
	}
	return nil
}
