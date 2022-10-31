package htldb

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

var zeroHash = common.Hash{}

// Test initial and modified state to have different hashes
func TestHashTreeInitialState(t *testing.T) {
	tmpDir := t.TempDir()
	db := openDb(t, tmpDir)

	pages := [][]byte{}
	tree := CreateHashTreeFactory(db, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pages})

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
}

// Test that without actual change, the hash does not change
func TestHashTreeUnchangedState(t *testing.T) {
	tmpDir := t.TempDir()
	db := openDb(t, tmpDir)

	pages := make([][]byte, 10)
	tree := CreateHashTreeFactory(db, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pages})

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
}

// Test that a change changes the hash
func TestHashTreeChangedState(t *testing.T) {
	tmpDir := t.TempDir()
	db := openDb(t, tmpDir)

	pages := make([][]byte, 10)
	tree := CreateHashTreeFactory(db, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pages})

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
}

// Test that two ways of building the same state leads to the same hash
func TestTwoTreesWithSameStateProvidesSameHash(t *testing.T) {
	tmpDirA := t.TempDir()
	tmpDirB := t.TempDir()

	db1 := openDb(t, tmpDirA)
	db2 := openDb(t, tmpDirB)

	// initialize two different states
	pagesA := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {0}, {0}}
	pagesB := [][]byte{{0}, {42}, {2}, {3}, {4}, {5}, {6}, {7}, {0}, {0}, {0}, {0}}
	treeA := CreateHashTreeFactory(db1, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pagesA})
	treeB := CreateHashTreeFactory(db2, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pagesB})
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
}

// TestTreePersisted tests tree is persisted and returns still correct hashes after recovery
func TestTreePersisted(t *testing.T) {
	tmpDir := t.TempDir()
	db := openDb(t, tmpDir)

	pages := make([][]byte, 10)
	tree := CreateHashTreeFactory(db, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pages})

	for i := 0; i < 10; i++ {
		pages[i] = []byte{byte(i)}
		tree.MarkUpdated(i)
	}
	hashBefore, err := tree.HashRoot()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	// reopen and check the hash is still there
	closeHashTreeDb(t, db)
	db = openDb(t, tmpDir)
	tree = CreateHashTreeFactory(db, common.ValueStoreKey, 3).Create(testingPageProvider{pages: pages})

	hashReopen, err := tree.HashRoot()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if hashBefore != hashReopen {
		t.Errorf("hashes differ")
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

func openDb(t *testing.T, path string) (db *leveldb.DB) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Cannot open DB, err: %s", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return
}

func closeHashTreeDb(t *testing.T, db *leveldb.DB) {
	if err := db.Close(); err != nil {
		t.Errorf("Cannot close DB")
	}
}
