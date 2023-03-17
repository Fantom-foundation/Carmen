package index_test

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func initIndexesMap() map[string]func(t *testing.T) index.Index[common.Address, uint32] {

	keySerializer := common.AddressSerializer{}
	idSerializer := common.Identifier32Serializer{}

	return map[string]func(t *testing.T) index.Index[common.Address, uint32]{
		"memindex": func(t *testing.T) index.Index[common.Address, uint32] {
			return memory.NewIndex[common.Address, uint32](keySerializer)
		},
		"memLinearHashIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			return memory.NewLinearHashIndex[common.Address, uint32](keySerializer, idSerializer, common.AddressHasher{}, common.AddressComparator{})
		},
		"cachedindex": func(t *testing.T) index.Index[common.Address, uint32] {
			return cache.NewIndex[common.Address, uint32](memory.NewIndex[common.Address, uint32](keySerializer), 10)
		},
		"ldbindex": func(t *testing.T) index.Index[common.Address, uint32] {
			db, err := common.OpenLevelDb(t.TempDir(), nil)
			if err != nil {
				t.Fatalf("failed to init leveldb; %s", err)
			}
			ldbindex, err := ldb.NewIndex[common.Address, uint32](db, common.BalanceStoreKey, keySerializer, idSerializer)
			if err != nil {
				t.Fatalf("failed to init leveldb; %s", err)
			}
			t.Cleanup(func() {
				_ = ldbindex.Close()
				_ = db.Close()
			})
			return ldbindex
		},
		"fileIndex": func(t *testing.T) index.Index[common.Address, uint32] {
			fileIndex, err := file.NewIndex[common.Address, uint32](t.TempDir(), keySerializer, idSerializer, common.AddressHasher{}, common.AddressComparator{})
			if err != nil {
				t.Fatalf("failed to init leveldb; %s", err)
			}
			t.Cleanup(func() {
				_ = fileIndex.Close()
			})
			return fileIndex
		},
	}
}

func TestIndexesInitialHash(t *testing.T) {
	indexes := initIndexesMap()

	for _, idx := range indexes {
		hash, err := idx(t).GetStateHash()
		if err != nil {
			t.Fatalf("failed to hash empty index; %s", err)
		}
		if hash != (common.Hash{}) {
			t.Errorf("invalid hash of empty index: %x (expected zeros)", hash)
		}
	}
}

func TestIndexesHashingByComparison(t *testing.T) {
	indexes := initIndexesMap()
	for i := 0; i < 10; i++ {
		ids := make([]uint32, len(indexes))
		indexInstances := make([]index.Index[common.Address, uint32], 0, len(indexes))
		var indexId int
		for _, idx := range indexes {
			indexInstance := idx(t)
			idx, err := indexInstance.GetOrAdd(common.Address{byte(0x20 + i)})
			ids[indexId] = idx
			indexId += 1
			indexInstances = append(indexInstances, indexInstance)
			if err != nil {
				t.Fatalf("failed to set index item %d; %s", i, err)
			}
		}
		if err := compareIds(ids); err != nil {
			t.Errorf("ids for item %d does not match: %s", i, err)
		}
		if err := compareHashes(indexInstances); err != nil {
			t.Errorf("hashes does not match after inserting item %d: %s", i, err)
		}
	}
}

func TestIndexesHashesAgainstReferenceOutput(t *testing.T) {
	indexes := initIndexesMap()

	// Tests the hashes for keys 0x01, 0x02 inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"ff9226e320b1deb7fabecff9ac800cd8eb1e3fb7709c003e2effcce37eec68ed",
		"c28553369c52e217564d3f5a783e2643186064498d1b3071568408d49eae6cbe",
	}

	indexInstances := make([]index.Index[common.Address, uint32], 0, len(indexes))
	for _, idx := range indexes {
		indexInstances = append(indexInstances, idx(t))
	}

	for i, expectedHash := range expectedHashes {
		for _, indexInstance := range indexInstances {
			_, err := indexInstance.GetOrAdd(common.Address{byte(i + 1)}) // 0x01 - 0x02
			if err != nil {
				t.Fatalf("failed to set index item; %s", err)
			}
			hash, err := indexInstance.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash index; %s", err)
			}
			if expectedHash != fmt.Sprintf("%x", hash) {
				t.Fatalf("invalid hash: %x (expected %s)", hash, expectedHash)
			}
		}
	}
}

func TestIndexSnapshot_IndexSnapshotCanBeCreatedAndRestored(t *testing.T) {
	for name, idx := range initIndexesMap() {
		for _, size := range []int{0, 1, 5, 1000} {
			t.Run(fmt.Sprintf("index %s size %d", name, size), func(t *testing.T) {

				originalIndex := idx(t)
				original, ok := originalIndex.(backend.Snapshotable)
				if !ok {
					t.Skip(fmt.Sprintf("index: %s is not Snapshotable", name))
				}

				fillIndex(t, originalIndex, size)
				originalProof, err := original.GetProof()
				if err != nil {
					t.Errorf("failed to produce a proof for the original state")
				}

				snapshot, err := original.CreateSnapshot()
				if err != nil {
					t.Errorf("failed to create snapshot: %v", err)
					return
				}
				if snapshot == nil {
					t.Errorf("failed to create snapshot")
					return
				}

				if !originalProof.Equal(snapshot.GetRootProof()) {
					t.Errorf("snapshot proof does not match data structure proof")
				}

				recoveredIndex := idx(t)
				recovered, _ := recoveredIndex.(backend.Snapshotable)
				if err := recovered.Restore(snapshot.GetData()); err != nil {
					t.Errorf("failed to sync to snapshot: %v", err)
					return
				}

				recoveredProof, err := recovered.GetProof()
				if err != nil {
					t.Errorf("failed to produce a proof for the recovered state")
				}

				if !recoveredProof.Equal(snapshot.GetRootProof()) {
					t.Errorf("snapshot proof does not match recovered proof")
				}

				checkIndexContent(t, recoveredIndex, size)

				if err := snapshot.Release(); err != nil {
					t.Errorf("failed to release snapshot: %v", err)
				}
			})
		}
	}
}

func TestIndexSnapshot_IndexSnapshotIsShieldedFromMutations(t *testing.T) {
	for name, idx := range initIndexesMap() {
		t.Run(fmt.Sprintf("index %s", name), func(t *testing.T) {

			originalIndex := idx(t)
			original, ok := originalIndex.(backend.Snapshotable)
			if !ok {
				t.Skip(fmt.Sprintf("index: %s is not Snapshotable", name))
			}

			fillIndex(t, originalIndex, 20)
			originalProof, err := original.GetProof()
			if err != nil {
				t.Errorf("failed to produce a proof for the original state")
			}

			snapshot, err := original.CreateSnapshot()
			if err != nil {
				t.Errorf("failed to create snapshot: %v", err)
				return
			}
			if snapshot == nil {
				t.Errorf("failed to create snapshot")
				return
			}

			// Additional mutations of the original should not be affected.
			if _, err := originalIndex.GetOrAdd(common.Address{0xaa}); err != nil {
				t.Errorf("failed to add key: %v", err)
			}

			if !originalProof.Equal(snapshot.GetRootProof()) {
				t.Errorf("snapshot proof does not match data structure proof")
			}

			recoveredIndex := idx(t)
			recovered, _ := recoveredIndex.(backend.Snapshotable)

			if err := recovered.Restore(snapshot.GetData()); err != nil {
				t.Errorf("failed to sync to snapshot: %v", err)
				return
			}

			if recoveredIndex.Contains(common.Address{0xaa}) {
				t.Errorf("recovered state should not include elements added after snapshot creation")
			}

			if err := snapshot.Release(); err != nil {
				t.Errorf("failed to release snapshot: %v", err)
			}
		})
	}
}

func TestIndexSnapshot_IndexSnapshotCanBeCreatedAndValidated(t *testing.T) {
	for name, idx := range initIndexesMap() {
		for _, size := range []int{0, 1, 5, 1000} {
			t.Run(fmt.Sprintf("index %s size %d", name, size), func(t *testing.T) {

				originalIndex := idx(t)
				original, ok := originalIndex.(backend.Snapshotable)
				if !ok {
					t.Skip(fmt.Sprintf("index: %s is not Snapshotable", name))
				}

				fillIndex(t, originalIndex, size)

				snapshot, err := original.CreateSnapshot()
				if err != nil {
					t.Errorf("failed to create snapshot: %v", err)
					return
				}
				if snapshot == nil {
					t.Errorf("failed to create snapshot")
					return
				}

				remote, err := index.CreateIndexSnapshotFromData[common.Address](common.AddressSerializer{}, snapshot.GetData())
				if err != nil {
					t.Fatalf("failed to create snapshot from snapshot data: %v", err)
				}

				// Test direct and serialized snapshot data access.
				for _, cur := range []backend.Snapshot{snapshot, remote} {

					// The root proof should be equivalent.
					want, err := original.GetProof()
					if err != nil {
						t.Errorf("failed to get root proof from data structure")
					}

					have := cur.GetRootProof()
					if !want.Equal(have) {
						t.Errorf("root proof of snapshot does not match proof of data structure")
					}

					verifier, err := original.GetSnapshotVerifier(cur.GetData())
					if err != nil {
						t.Fatalf("failed to obtain snapshot verifier")
					}

					if proof, err := verifier.VerifyRootProof(cur.GetData()); err != nil || !proof.Equal(want) {
						t.Errorf("snapshot invalid, inconsistent proofs: %v, want %v, got %v", err, want, proof)
					}

					// Verify all pages
					for i := 0; i < cur.GetNumParts(); i++ {
						want, err := cur.GetProof(i)
						if err != nil {
							t.Errorf("failed to fetch proof of part %d", i)
						}
						part, err := cur.GetPart(i)
						if err != nil || part == nil {
							t.Errorf("failed to fetch part %d", i)
						}
						if part != nil && verifier.VerifyPart(i, want.ToBytes(), part.ToBytes()) != nil {
							t.Errorf("failed to verify content of part %d", i)
						}
					}
				}

				if err := remote.Release(); err != nil {
					t.Errorf("failed to release remote snapshot: %v", err)
				}
				if err := snapshot.Release(); err != nil {
					t.Errorf("failed to release snapshot: %v", err)
				}
			})
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

func fillIndex(t *testing.T, index index.Index[common.Address, uint32], size int) {
	for i := 0; i < size; i++ {
		if idx, err := index.GetOrAdd(common.Address{byte(i), byte(i >> 8), byte(i >> 16)}); idx != uint32(i) || err != nil {
			t.Errorf("failed to add address %d", i)
		}
	}
}

func checkIndexContent(t *testing.T, index index.Index[common.Address, uint32], size int) {
	for i := 0; i < size; i++ {
		if idx, err := index.GetOrAdd(common.Address{byte(i), byte(i >> 8), byte(i >> 16)}); idx != uint32(i) || err != nil {
			t.Errorf("failed to locate address %d", i)
		}
	}
}
