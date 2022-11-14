package depot_test

import (
	"bytes"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/file"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

type depotFactory struct {
	label    string
	getDepot func(tempDir string) depot.Depot[uint32]
}

func getDepotsFactories(tb testing.TB, branchingFactor int, hashItems int) (stores []depotFactory) {
	return []depotFactory{
		{
			label: "Memory",
			getDepot: func(tempDir string) depot.Depot[uint32] {
				hashTree := htmemory.CreateHashTreeFactory(branchingFactor)
				d, err := memory.NewDepot[uint32](hashItems, hashTree)
				if err != nil {
					tb.Fatalf("failed to create depot; %s", err)
				}
				return d
			},
		},
		{
			label: "File",
			getDepot: func(tempDir string) depot.Depot[uint32] {
				hashTree := htfile.CreateHashTreeFactory(tempDir, branchingFactor)
				d, err := file.NewDepot[uint32](tempDir, common.Identifier32Serializer{}, hashTree, hashItems)
				if err != nil {
					tb.Fatalf("failed to create depot; %s", err)
				}
				return d
			},
		},
		{
			label: "LevelDb",
			getDepot: func(tempDir string) depot.Depot[uint32] {
				db, err := leveldb.OpenFile(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to open LevelDB; %s", err)
				}
				hashTree := htldb.CreateHashTreeFactory(db, common.DepotCodeKey, branchingFactor)
				dep, err := ldb.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, hashTree, hashItems)
				if err != nil {
					tb.Fatalf("failed to create depot; %s", err)
				}
				return &ldbDepotWrapper{dep, db}
			},
		},
	}
}

// ldbDepotWrapper wraps the ldb.Depot to close the LevelDB on the depot Close
type ldbDepotWrapper struct {
	depot.Depot[uint32]
	db *leveldb.DB
}

func (w *ldbDepotWrapper) Close() error {
	err := w.Depot.Close()
	if err != nil {
		return err
	}
	return w.db.Close()
}

var (
	A = []byte{0xAA}
	B = []byte{0xBB, 0xBB}
	C = []byte{0xCC}
)

func TestSetGet(t *testing.T) {
	for _, factory := range getDepotsFactories(t, 3, 2) {
		t.Run(factory.label, func(t *testing.T) {
			d := factory.getDepot(t.TempDir())
			defer d.Close()

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
		})
	}
}

func TestSetToArbitraryPosition(t *testing.T) {
	for _, factory := range getDepotsFactories(t, 3, 2) {
		t.Run(factory.label, func(t *testing.T) {
			d := factory.getDepot(t.TempDir())
			defer d.Close()

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
		})
	}
}

func TestDepotPersistence(t *testing.T) {
	for _, factory := range getDepotsFactories(t, 3, 2) {
		if factory.label == "Memory" {
			continue
		}
		t.Run(factory.label, func(t *testing.T) {
			dir := t.TempDir()

			d1 := factory.getDepot(dir)
			err := d1.Set(1, B)
			if err != nil {
				t.Fatalf("failed to set into a depot; %s", err)
			}
			_ = d1.Close()

			d2 := factory.getDepot(dir)
			value, err := d2.Get(1)
			if err != nil {
				t.Fatalf("failed to get from a depot; %s", err)
			}
			if !bytes.Equal(value, B) {
				t.Errorf("value stored into a depo not persisted")
			}
			_ = d2.Close()
		})
	}
}

func TestHashing(t *testing.T) {
	for _, factory := range getDepotsFactories(t, 3, 2) {
		t.Run(factory.label, func(t *testing.T) {
			d := factory.getDepot(t.TempDir())
			defer d.Close()

			initialHash, err := d.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if initialHash != (common.Hash{}) {
				t.Fatalf("invalid initial hash %x", initialHash)
			}

			err = d.Set(0, A)
			if err != nil {
				t.Fatalf("failed to set A; %s", err)
			}

			newHash, err := d.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if initialHash == newHash {
				t.Errorf("setting into the depot have not changed the hash %x %x", initialHash, newHash)
			}
		})
	}
}

func TestHashAfterChangingBack(t *testing.T) {
	for _, factory := range getDepotsFactories(t, 3, 2) {
		t.Run(factory.label, func(t *testing.T) {
			d := factory.getDepot(t.TempDir())
			defer d.Close()

			err := d.Set(0, A)
			if err != nil {
				t.Fatalf("failed to set A; %s", err)
			}
			err = d.Set(1, B)
			if err != nil {
				t.Fatalf("failed to set B; %s", err)
			}
			initialHash, err := d.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			err = d.Set(1, C)
			if err != nil {
				t.Fatalf("failed to set C; %s", err)
			}

			hashAfterChange, err := d.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if initialHash == hashAfterChange {
				t.Errorf("setting into depot have not changed the hash %x %x", initialHash, hashAfterChange)
			}

			err = d.Set(1, B)
			if err != nil {
				t.Fatalf("failed to set B back; %s", err)
			}
			hashAfterRevert, err := d.GetStateHash()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if initialHash != hashAfterRevert {
				t.Errorf("setting into depot have not changed the hash back %x %x", initialHash, hashAfterRevert)
			}
		})
	}
}

func TestStoresHashesAgainstReferenceOutput(t *testing.T) {
	// Tests the hashes for values [0x00], [0x00, 0x11] ... [..., 0xFF] inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
		"aea3b18a4991da51ab201722c233c967e9c5d726cbc9a327c42b17d24268303b",
		"e136dc145513327cf5846ea5cbb3b9d30543d27963288dd7bf6ad63360085df8",
		"a98672f2a05a5b71b49451e85238e3f4ebc6fb8cedb00d55d8bc4ea6e52d0117",
		"1e7f4c505dd16f8537bdad064b49a8c0a64a707725fbf09ad4311f280781e9e4",
		"b07ee4eec6d898d88ec3ef9c66c64f3f0896cd1c7e759b825baf541d42e77784",
		"9346102f81ac75e583499081d9ab10c7050ff682c7dfd4700a9f909ee469a2de",
		"44532b1bcf3840a8bf0ead0a6052d4968c5fac6023cd1f86ad43175e53d25e9c",
		"2de0363a6210fca91e2143b945a86f42ae90cd786e641d51e2a7b9c141b020b0",
		"0c81b39c90852a66f18b0518d36dceb2f889501dc279e759bb2d1253a63caa8e",
		"c9fa5b094c4d964bf6d2b25d7ba1e580a83b9ebf2ea8594e99baa81474be4c47",
		"078fb14729015631017d2d82c844642ec723e92e06eb41f88ca83b36e3a04d30",
		"4f91e8c410a52b53e46f7b787fdc240c3349711108c2a1ac69ddb0c64e51f918",
		"4e0d2c84af4f9e54c2d0864302a72703c656996585ec99f7290a2172617ea0e9",
		"38e68d99bafc836105e88a1092ebdadb6d8a4a1acec29eecc7ec01b885e6f820",
		"f9764b20bf761bd89b3266697fbc1c336548c3bcbb1c81e4ecf3829df53d98ec",
	}

	for _, factory := range getDepotsFactories(t, 3, 2) {
		t.Run(factory.label, func(t *testing.T) {
			d := factory.getDepot(t.TempDir())
			defer d.Close()

			var value []byte
			for i, expectedHash := range expectedHashes {
				value = append(value, byte(i<<4|i))
				if err := d.Set(uint32(i), value); err != nil {
					t.Fatalf("failed to set store item %d; %s", i, err)
				}
				hash, err := d.GetStateHash()
				if err != nil {
					t.Fatalf("failed to hash depot with %d values; %s", i+1, err)
				}
				if expectedHash != fmt.Sprintf("%x", hash) {
					t.Errorf("invalid hash: %x (expected %s)", hash, expectedHash)
				}
			}
		})
	}
}

func TestDepotsHashingByComparison(t *testing.T) {
	depots := make(map[string]depot.Depot[uint32])
	for _, fac := range getDepotsFactories(t, 3, 2) {
		depots[fac.label] = fac.getDepot(t.TempDir())
	}
	defer func() {
		for _, d := range depots {
			_ = d.Close()
		}
	}()

	for i := 0; i < 10; i++ {
		for _, d := range depots {
			if err := d.Set(uint32(i), []byte{byte(0x10 + i)}); err != nil {
				t.Fatalf("failed to set depot item %d; %s", i, err)
			}
		}
		if err := compareHashes(depots); err != nil {
			t.Errorf("depots hashes does not match after inserting item %d: %s", i, err)
		}
	}

	// modify one item in the middle
	for _, d := range depots {
		if err := d.Set(2, []byte{byte(0x99)}); err != nil {
			t.Fatalf("failed to set again depot item %d; %s", 2, err)
		}
	}
	if err := compareHashes(depots); err != nil {
		t.Errorf("depots hashes does not match after updating item %d: %s", 2, err)
	}
}

func compareHashes(depots map[string]depot.Depot[uint32]) error {
	var firstHash common.Hash
	var firstLabel string
	for label, d := range depots {
		hash, err := d.GetStateHash()
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
