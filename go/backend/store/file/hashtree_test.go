package file

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"testing"
)

var zeroHash = common.Hash{}

// Test initial and modified state to have different hashes
func TestHashtreeInitialState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	pages := [][]byte{}
	tree := NewHashTree(tmpDir, 3, testingPageProvider{pages: pages})

	hash, err := tree.HashRoot()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if hash != zeroHash {
		t.Errorf("Initial hash is not zero")
	}

	pages = [][]byte{{0xFA}}
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
func TestHashtreeUnchangedState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	pages := make([][]byte, 10)
	tree := NewHashTree(tmpDir, 3, testingPageProvider{pages: pages})

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
		t.Errorf("Hash for the same unchanged state is different")
	}
}

// Test that a change changes the hash
func TestHashtreeChangedState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	pages := make([][]byte, 10)
	tree := NewHashTree(tmpDir, 3, testingPageProvider{pages: pages})

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
	tmpDirA, err := os.MkdirTemp("", "file-based-store-test-a")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	tmpDirB, err := os.MkdirTemp("", "file-based-store-test-b")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDirA)
	defer os.RemoveAll(tmpDirB)

	// initialize two different states
	pagesA := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {0}, {0}}
	pagesB := [][]byte{{0}, {42}, {2}, {3}, {4}, {5}, {6}, {7}, {0}, {0}, {0}, {0}}
	treeA := NewHashTree(tmpDirA, 3, testingPageProvider{pages: pagesA})
	treeB := NewHashTree(tmpDirB, 3, testingPageProvider{pages: pagesB})
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
		t.Errorf("the trees changed to be identical provides different hashes")
	}
}

// Whitebox test of the amount of hash layers produced by different amounts of pages
func TestAmountOfLevels(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}
	defer os.RemoveAll(tmpDir)

	branchingFactor := 3
	pages := make([][]byte, branchingFactor*branchingFactor+1)
	tree := NewHashTree(tmpDir, branchingFactor, testingPageProvider{pages: pages})

	var i int
	for ; i < branchingFactor*branchingFactor; i++ {
		pages[i] = []byte{byte(i)}
		tree.MarkUpdated(i)
	}
	_, err = tree.HashRoot()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	count, err := tree.getLayersCount()
	if err != nil {
		t.Fatalf("failed to count layers; %s", err)
	}
	if count != 3 {
		t.Errorf("Unexpected amount of tree levels")
	}

	pages[i] = []byte{byte(i)}
	tree.MarkUpdated(i)
	_, err = tree.HashRoot()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	count, err = tree.getLayersCount()
	if err != nil {
		t.Fatalf("failed to count layers; %s", err)
	}
	if count != 4 {
		t.Errorf("Unexpected amount of tree levels")
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
