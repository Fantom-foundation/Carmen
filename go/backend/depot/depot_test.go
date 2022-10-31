package depot

import (
	"bytes"
	"fmt"
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

type DepotFactory struct {
	label    string
	getDepot func(tempDir string) Depot[uint32]
}

func getDepotsFactories(tb testing.TB, db *leveldb.DB) (stores []DepotFactory) {
	return []DepotFactory{
		{
			label: "Memory",
			getDepot: func(tempDir string) Depot[uint32] {
				d, err := memory.NewDepot[uint32](2, htmemory.CreateHashTreeFactory(3))
				if err != nil {
					tb.Fatalf("%s", err)
				}
				return d
			},
		},
		{
			label: "File",
			getDepot: func(tempDir string) Depot[uint32] {
				d, err := file.NewDepot[uint32](tempDir, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(tempDir, 3), 2)
				if err != nil {
					tb.Fatalf("%s", err)
				}
				return d
			},
		},
		{
			label: "LevelDb",
			getDepot: func(tempDir string) Depot[uint32] {
				d, err := ldb.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.DepotCodeKey, 3), 2)
				if err != nil {
					tb.Fatalf("%s", err)
				}
				return d
			},
		},
	}
}

func openLevelDb(t *testing.T, path string) (db *leveldb.DB) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Cannot open DB, err: %s", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return
}

var (
	A = []byte{0xAA}
	B = []byte{0xBB, 0xBB}
	C = []byte{0xCC}
)

func TestSetGet(t *testing.T) {
	for _, factory := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
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
	for _, factory := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
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
	for _, factory := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
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
	for _, factory := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
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
	for _, factory := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
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

func TestDepotsHashingByComparison(t *testing.T) {
	depots := make(map[string]Depot[uint32])
	for _, fac := range getDepotsFactories(t, openLevelDb(t, t.TempDir())) {
		depots[fac.label] = fac.getDepot(t.TempDir())
	}

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
}

func compareHashes(depots map[string]Depot[uint32]) error {
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
