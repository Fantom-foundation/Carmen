package hashtree_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

type hashTreeFactory struct {
	label       string
	getHashtree func(tempDir string, pages [][]byte) closingHashtreeWrapper
}

func getHashTreeFactories(tb testing.TB) (factories []hashTreeFactory) {
	return []hashTreeFactory{
		{
			label: "Memory",
			getHashtree: func(tempDir string, pages [][]byte) closingHashtreeWrapper {
				return closingHashtreeWrapper{htmemory.CreateHashTreeFactory(3).Create(testingPageProvider{pages: pages}), nil}
			},
		},
		{
			label: "File",
			getHashtree: func(tempDir string, pages [][]byte) closingHashtreeWrapper {
				return closingHashtreeWrapper{htfile.CreateHashTreeFactory(tempDir, 3).Create(testingPageProvider{pages: pages}), nil}
			},
		},
		{
			label: "LevelDb",
			getHashtree: func(tempDir string, pages [][]byte) closingHashtreeWrapper {
				db, err := backend.OpenLevelDb(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to open LevelDB; %s", err)
				}
				return closingHashtreeWrapper{htldb.CreateHashTreeFactory(db, backend.DepotCodeKey, 3).Create(testingPageProvider{pages: pages}), db}
			},
		},
	}
}

type closingHashtreeWrapper struct {
	hashtree.HashTree
	closeable io.Closer
}

func (w *closingHashtreeWrapper) Close() {
	if w.closeable != nil {
		_ = w.closeable.Close()
	}
}

type testingPageProvider struct {
	pages [][]byte
}

func (pp testingPageProvider) GetPage(page int) ([]byte, error) {
	if page >= len(pp.pages) {
		return []byte{}, nil
	}
	return pp.pages[page], nil
}

var zeroHash = common.Hash{}

// Test initial and modified state to have different hashes
func TestHashTreeInitialState(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			var pages [][]byte
			tree := factory.getHashtree(t.TempDir(), pages)
			defer tree.Close()

			hash, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if hash != zeroHash {
				t.Errorf("Initial hash is not zero")
			}

			tree.MarkUpdated(0)
			hash, err = tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if hash == zeroHash {
				t.Errorf("Hash of modified state must not be zero")
			}
		})
	}
}

// Test that without actual change, the hash does not change
func TestHashTreeUnchangedState(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			pages := make([][]byte, 10)
			tree := factory.getHashtree(t.TempDir(), pages)
			defer tree.Close()

			for i := 0; i < 10; i++ {
				pages[i] = []byte{byte(i)}
				tree.MarkUpdated(i)
			}
			hashBefore, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			tree.MarkUpdated(5)
			tree.MarkUpdated(3)

			hashAfter, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			if hashBefore != hashAfter {
				t.Errorf("Hash for unchanged state is different")
			}
		})
	}
}

// Test that a change changes the hash
func TestHashTreeChangedState(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			pages := make([][]byte, 10)
			tree := factory.getHashtree(t.TempDir(), pages)
			defer tree.Close()

			for i := 0; i < 10; i++ {
				pages[i] = []byte{byte(i)}
				tree.MarkUpdated(i)
			}

			hashBefore, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			pages[5] = []byte{42}
			tree.MarkUpdated(5)

			hashAfter, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			if hashBefore == hashAfter {
				t.Errorf("Hash for the changed state is the same")
			}
		})
	}
}

// Test that the tree can be retested into the initial state
func TestHashTreeReset(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			pages := make([][]byte, 10)
			tree := factory.getHashtree(t.TempDir(), pages)
			defer tree.Close()

			initialHash, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to get initial hash; %s", err)
			}

			for i := 0; i < 10; i++ {
				pages[i] = []byte{byte(i)}
				tree.MarkUpdated(i)
			}

			changedHash, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if changedHash == initialHash {
				t.Errorf("hash after change does not changed; %x == %x", changedHash, initialHash)
			}

			err = tree.Reset()
			if err != nil {
				t.Fatalf("failed to reset; %s", err)
			}

			resetHash, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if resetHash != initialHash {
				t.Errorf("hash after reset does not match the initial hash; %x != %x", resetHash, initialHash)
			}
		})
	}
}

// Test that two ways of building the same state leads to the same hash
func TestTwoTreesWithSameStateProvidesSameHash(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			// initialize two different states
			pagesA := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {0}, {0}}
			pagesB := [][]byte{{0}, {42}, {2}, {3}, {4}, {5}, {6}, {7}, {0}, {0}, {0}, {0}}
			treeA := factory.getHashtree(t.TempDir(), pagesA)
			treeB := factory.getHashtree(t.TempDir(), pagesB)
			for i := 0; i < 8; i++ {
				treeA.MarkUpdated(i)
				treeB.MarkUpdated(i)
			}
			treeA.MarkUpdated(8)
			treeA.MarkUpdated(9)
			firstHashA, err := treeA.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			firstHashB, err := treeB.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if firstHashA == firstHashB {
				t.Errorf("different starting states provides the same hash")
			}

			// transition the states to the same state
			pagesB[1] = []byte{1}
			pagesB[8] = []byte{8}
			pagesB[9] = []byte{9}
			treeB.MarkUpdated(1)
			treeB.MarkUpdated(8)
			treeB.MarkUpdated(9)
			firstHashA, err = treeA.HashRoot()
			if err != nil {
				t.Fatalf("failed to commit; %s", err)
			}
			firstHashB, err = treeB.HashRoot()
			if err != nil {
				t.Fatalf("failed to commit; %s", err)
			}

			if firstHashA != firstHashB {
				t.Errorf("hashes differ")
			}
		})
	}
}

// TestTreePersisted tests tree is persisted and returns still correct hashes after recovery
func TestTreePersisted(t *testing.T) {
	for _, factory := range getHashTreeFactories(t) {
		if factory.label == "Memory" {
			continue
		}
		t.Run(factory.label, func(t *testing.T) {
			tempDir := t.TempDir()
			pages := make([][]byte, 10)
			tree := factory.getHashtree(tempDir, pages)
			defer tree.Close()

			for i := 0; i < 10; i++ {
				pages[i] = []byte{byte(i)}
				tree.MarkUpdated(i)
			}
			hashBefore, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}

			// reopen and check the hash is still there
			tree.Close()
			tree = factory.getHashtree(tempDir, pages)

			hashReopen, err := tree.HashRoot()
			if err != nil {
				t.Fatalf("failed to hash; %s", err)
			}
			if hashBefore != hashReopen {
				t.Errorf("hashes differ")
			}
		})
	}
}

func TestHashesAgainstReferenceOutput(t *testing.T) {
	// Tests the hashes for values 0x00, 0x11 ... 0x44 inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
		"6c5f701c7f179fe2f65970ec7105d8e5c156c94fdf5aaaa9583be12473c89f0f",
		"d8474951058dfc020b3d9b62b06528130543884b9520a7542be52a2f6344cad4",
		"3cd329e823a238ba7897f2ad62aeea1435a10999cbed87bec7fdd410a93d7096",
		"1964521cc4a514e44c4395c9a23ee88263b4463717d43b0c57808867bcabfd4f",
	}

	for _, factory := range getHashTreeFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			pages := make([][]byte, 5)
			tree := factory.getHashtree(t.TempDir(), pages)
			defer tree.Close()

			for i, expectedHash := range expectedHashes {
				pages[i] = []byte{byte(i<<4 | i)}
				tree.MarkUpdated(i)
				hash, err := tree.HashRoot()
				if err != nil {
					t.Fatalf("failed to hash tree with %d values; %s", i+1, err)
				}
				if expectedHash != fmt.Sprintf("%x", hash) {
					t.Errorf("invalid hash: %x (expected %s)", hash, expectedHash)
				}
			}
		})
	}
}

func TestHashingByComparison(t *testing.T) {
	hashTrees := make(map[string]closingHashtreeWrapper)
	pages := make([][]byte, 10)
	for _, fac := range getHashTreeFactories(t) {
		hashTrees[fac.label] = fac.getHashtree(t.TempDir(), pages)
	}

	for i := 0; i < 10; i++ {
		pages[i] = []byte{byte(0x10 + i)}
		for _, h := range hashTrees {
			h.MarkUpdated(i)
		}
		if err := compareHashes(hashTrees); err != nil {
			t.Errorf("hashTrees hashes does not match after inserting item %d: %s", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := comparePageHashes(hashTrees, i); err != nil {
			t.Errorf("hashTrees page %d hashes does not match after inserting item %d: %s", i, i, err)
		}
	}
}

func compareHashes(hashTrees map[string]closingHashtreeWrapper) error {
	var firstHash common.Hash
	var firstLabel string
	for label, d := range hashTrees {
		hash, err := d.HashRoot()
		if err != nil {
			return err
		}
		if firstHash == zeroHash {
			firstHash = hash
			firstLabel = label
		} else if firstHash != hash {
			return fmt.Errorf("different hashes: %s(%x) != %s(%x)", firstLabel, firstHash, label, hash)
		}
	}
	return nil
}

func comparePageHashes(hashTrees map[string]closingHashtreeWrapper, page int) error {
	var firstHash common.Hash
	var firstLabel string
	for label, d := range hashTrees {
		hash, err := d.GetPageHash(page)
		if err != nil {
			return err
		}
		if firstHash == zeroHash {
			firstHash = hash
			firstLabel = label
		} else if firstHash != hash {
			return fmt.Errorf("different hashes: %s(%x) != %s(%x)", firstLabel, firstHash, label, hash)
		}
	}
	return nil
}
