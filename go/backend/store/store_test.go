//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package store_test

import (
	"bytes"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

// test stores parameters (different from benchmark stores parameters)
const (
	BranchingFactor = 3
	ItemsPerPage    = 2
	PageSize        = ItemsPerPage * 32
	PoolSize        = 5
)

type storeFactory[V any] struct {
	label    string
	getStore func(tempDir string) store.Store[uint32, V]
}

func getStoresFactories[V any](tb testing.TB, serializer common.Serializer[V], branchingFactor int, pageSize int, poolSize int) (stores []storeFactory[V]) {
	return []storeFactory[V]{
		{
			label: "Memory",
			getStore: func(tempDir string) store.Store[uint32, V] {
				hashTreeFac := htmemory.CreateHashTreeFactory(branchingFactor)
				str, err := memory.NewStore[uint32, V](serializer, pageSize, hashTreeFac)
				if err != nil {
					tb.Fatalf("failed to init memory store; %s", err)
				}
				return str
			},
		},
		{
			label: "File",
			getStore: func(tempDir string) store.Store[uint32, V] {
				hashTreeFac := htfile.CreateHashTreeFactory(tempDir, branchingFactor)
				str, err := file.NewStore[uint32, V](tempDir, serializer, pageSize, hashTreeFac)
				if err != nil {
					tb.Fatalf("failed to init file store; %s", err)
				}
				return str
			},
		},
		{
			label: "PagedFile",
			getStore: func(tempDir string) store.Store[uint32, V] {
				hashTreeFac := htfile.CreateHashTreeFactory(tempDir, branchingFactor)
				str, err := pagedfile.NewStore[uint32, V](tempDir, serializer, pageSize, hashTreeFac, poolSize)
				if err != nil {
					tb.Fatalf("failed to init pagedfile store; %s", err)
				}
				return str
			},
		},
		{
			label: "LevelDb",
			getStore: func(tempDir string) store.Store[uint32, V] {
				db, err := backend.OpenLevelDb(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				hashTreeFac := htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, branchingFactor)
				str, err := ldb.NewStore[uint32, V](db, backend.ValueStoreKey, serializer, common.Identifier32Serializer{}, hashTreeFac, pageSize)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				return &storeClosingWrapper[V]{str, []func() error{db.Close}}
			},
		},
		{
			label: "CachedLevelDb",
			getStore: func(tempDir string) store.Store[uint32, V] {
				cacheCapacity := 1 << 18
				db, err := backend.OpenLevelDb(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				hashTreeFac := htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, branchingFactor)
				str, err := ldb.NewStore[uint32, V](db, backend.ValueStoreKey, serializer, common.Identifier32Serializer{}, hashTreeFac, pageSize)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				cached := cache.NewStore[uint32, V](str, cacheCapacity)
				return &storeClosingWrapper[V]{cached, []func() error{db.Close}}
			},
		},
	}
}

// storeClosingWrapper wraps an instance of the Store to clean-up related resources when the Store is closed
type storeClosingWrapper[V any] struct {
	store.Store[uint32, V]
	cleanups []func() error
}

// Close executes clean-up
func (s *storeClosingWrapper[V]) Close() error {
	for _, f := range s.cleanups {
		_ = f()
	}
	return s.Store.Close()
}

func TestStoresInitialHash(t *testing.T) {
	for _, factory := range getStoresFactories[common.Value](t, common.ValueSerializer{}, BranchingFactor, PageSize, PoolSize) {
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
	for _, factory := range getStoresFactories[common.Value](t, common.ValueSerializer{}, BranchingFactor, PageSize, PoolSize) {
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

	for _, factory := range getStoresFactories[common.Value](t, common.ValueSerializer{}, BranchingFactor, PageSize, PoolSize) {
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

func TestStoresPaddedPages(t *testing.T) {
	serializer := common.SlotReincValueSerializer{}
	pageSize := serializer.Size()*2 + 4 // page for two values + 4 bytes of padding
	var ref []byte = nil
	for _, factory := range getStoresFactories[common.SlotReincValue](t, serializer, BranchingFactor, pageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			s := factory.getStore(t.TempDir())
			defer s.Close()

			innerStore := s
			wrappingStore, casted := s.(*storeClosingWrapper[common.SlotReincValue])
			if casted {
				innerStore = wrappingStore.Store
			}
			pageProvider, casted := innerStore.(hashtree.PageProvider)
			if !casted {
				t.Skip("not a PageProvider")
			}

			err := s.Set(1, common.SlotReincValue{Reincarnation: 1234, Value: common.Value{0x56}})
			if err != nil {
				t.Fatalf("failed to set into store; %s", err)
			}

			page, err := pageProvider.GetPage(0)
			if err != nil {
				t.Fatalf("failed to get page; %s", err)
			}

			if ref == nil {
				ref = page
			} else if !bytes.Equal(ref, page) {
				t.Errorf("page value from %s does not match the reference:\n page: %x\n ref:  %x", factory.label, page, ref)
			}
		})
	}
}

func TestStoreSnapshotRecovery(t *testing.T) {
	serializer := common.SlotReincValueSerializer{}
	pageSize := serializer.Size()*2 + 4 // page for two values + 4 bytes of padding
	for _, factory := range getStoresFactories[common.SlotReincValue](t, serializer, BranchingFactor, pageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			store1 := factory.getStore(t.TempDir())
			defer store1.Close()

			for i := 0; i < PoolSize*3; i++ {
				if err := store1.Set(uint32(i), common.SlotReincValue{Reincarnation: 1, Value: common.Value{byte(i)}}); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}
			}
			stateHash1, err := store1.GetStateHash()
			if err != nil {
				t.Fatalf("failed to get state hash; %s", err)
			}

			snapshot1, err := store1.CreateSnapshot()
			if err != nil {
				t.Fatalf("failed to create snapshot; %s", err)
			}
			defer snapshot1.Release()
			snapshot1data := snapshot1.GetData()

			store2 := factory.getStore(t.TempDir())
			defer store2.Close()

			err = store2.Restore(snapshot1data)
			if err != nil {
				t.Fatalf("failed to recover snapshot; %s", err)
			}

			for i := 0; i < PoolSize*3; i++ {
				if value, err := store2.Get(uint32(i)); err != nil || value.Value != (common.Value{byte(i)}) {
					t.Errorf("incorrect Get result for recovered store, key %d; %x, %s", i, value.Value, err)
				}
			}
			stateHash2, err := store2.GetStateHash()
			if err != nil {
				t.Fatalf("failed to get recovered store hash; %s", err)
			}
			if stateHash1 != stateHash2 {
				t.Errorf("recovered store hash does not match")
			}
		})
	}
}

func TestStoreSnapshotPartsNum(t *testing.T) {
	serializer := common.SlotReincValueSerializer{}
	pageSize := serializer.Size()*2 + 4 // page for two values + 4 bytes of padding
	for _, factory := range getStoresFactories[common.SlotReincValue](t, serializer, BranchingFactor, pageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			store1 := factory.getStore(t.TempDir())
			defer store1.Close()

			for i := 0; i < 255; i++ {
				if err := store1.Set(uint32(i), common.SlotReincValue{Reincarnation: 1, Value: common.Value{byte(i)}}); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}

				snapshot, err := store1.CreateSnapshot()
				if err != nil {
					t.Fatalf("failed to create snapshot; %s", err)
				}

				expectedPagesCount := (i + 1) / ItemsPerPage
				if (i+1)%ItemsPerPage != 0 {
					expectedPagesCount++
				}
				if snapshot.GetNumParts() != expectedPagesCount {
					t.Errorf("unexpected amount of snapshot parts: %d (expected %d) i=%d", snapshot.GetNumParts(), expectedPagesCount, i)
				}

				if err := snapshot.Release(); err != nil {
					t.Fatalf("failed to release snapshot; %s", err)
				}
			}
		})
	}
}

func TestStoreSnapshotRecoveryOverriding(t *testing.T) {
	serializer := common.SlotReincValueSerializer{}
	pageSize := serializer.Size()*2 + 4 // page for two values + 4 bytes of padding
	for _, factory := range getStoresFactories[common.SlotReincValue](t, serializer, BranchingFactor, pageSize, PoolSize) {
		t.Run(factory.label, func(t *testing.T) {
			store1 := factory.getStore(t.TempDir())
			defer store1.Close()

			for i := 0; i < PoolSize*2; i++ {
				if err := store1.Set(uint32(i), common.SlotReincValue{Reincarnation: 1, Value: common.Value{byte(i)}}); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}
			}
			stateHash1, err := store1.GetStateHash()
			if err != nil {
				t.Fatalf("failed to get state hash; %s", err)
			}

			snapshot1, err := store1.CreateSnapshot()
			if err != nil {
				t.Fatalf("failed to create snapshot; %s", err)
			}
			snapshot1data := snapshot1.GetData()

			// ensure the snapshot is used - change something in the store after the snapshot is created
			if err := store1.Set(uint32(2), common.SlotReincValue{Reincarnation: 1, Value: common.Value{byte(55)}}); err != nil {
				t.Fatalf("failed to set store item %d; %s", 2, err)
			}

			store2 := factory.getStore(t.TempDir())
			defer store2.Close()

			// the store2 will be filled with data before the restore - these should be removed during restore
			for i := 0; i < PoolSize*2+5; i++ {
				if err := store2.Set(uint32(i), common.SlotReincValue{Reincarnation: 2, Value: common.Value{byte(i + 5)}}); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}
			}

			err = store2.Restore(snapshot1data)
			if err != nil {
				t.Fatalf("failed to recover snapshot; %s", err)
			}

			if err := snapshot1.Release(); err != nil {
				t.Errorf("failed to release a snapshot; %s", err)
			}

			for i := 0; i < PoolSize*2; i++ {
				if value, err := store2.Get(uint32(i)); err != nil || value.Value != (common.Value{byte(i)}) {
					t.Errorf("incorrect Get result for recovered store, key %d; %x, %s", i, value.Value, err)
				}
			}
			stateHash2, err := store2.GetStateHash()
			if err != nil {
				t.Fatalf("failed to get recovered store hash; %s", err)
			}
			if stateHash1 != stateHash2 {
				t.Errorf("recovered store hash does not match")
			}
		})
	}
}

func TestStorePersistence(t *testing.T) {
	serializer := common.KeySerializer{}
	pageSize := serializer.Size()*2 + 4 // page for two values + 4 bytes of padding
	for _, factory := range getStoresFactories[common.Key](t, serializer, BranchingFactor, pageSize, PoolSize) {
		if factory.label == "Memory" {
			continue
		}
		t.Run(factory.label, func(t *testing.T) {
			dir := t.TempDir()

			d1 := factory.getStore(dir)
			err := d1.Set(1, common.Key{0x11, 0x22, 0x33})
			if err != nil {
				t.Fatalf("failed to set into a depot; %s", err)
			}
			snap1, err := d1.CreateSnapshot()
			if err != nil {
				t.Fatalf("failed to create snapshot; %s", err)
			}
			parts1 := snap1.GetNumParts()
			_ = snap1.Release()
			_ = d1.Close()

			d2 := factory.getStore(dir)
			value, err := d2.Get(1)
			if err != nil {
				t.Fatalf("failed to get from a store; %s", err)
			}
			if value != (common.Key{0x11, 0x22, 0x33}) {
				t.Errorf("value stored into a store not persisted")
			}
			snap2, err := d2.CreateSnapshot()
			if err != nil {
				t.Fatalf("failed to create snapshot; %s", err)
			}
			parts2 := snap2.GetNumParts()
			if parts1 != parts2 {
				t.Errorf("num of parts persisted in the store does not match: %d != %d", parts1, parts2)
			}
			_ = snap2.Release()
			_ = d2.Close()
		})
	}
}
