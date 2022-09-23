package memory

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var zeroHash = common.Hash{}

func TestHashtreeInitialState(t *testing.T) {
	pages := [][]byte{}
	tree := NewHashTree(func(i int) []byte {
		if i >= len(pages) {
			return []byte{}
		}
		return pages[i]
	})

	if tree.GetHash() != zeroHash {
		t.Errorf("Initial hash is not zero")
	}

	err := tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	if tree.GetHash() != zeroHash {
		t.Errorf("Initial hash after commit is not zero, but %x", tree.GetHash())
	}

	pages = [][]byte{{0xFA}}
	tree.MarkUpdated(0)
	err = tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	if tree.GetHash() == zeroHash {
		t.Errorf("Hash of modified state must not be zero")
	}
}

func TestHashtreeUnchangedState(t *testing.T) {
	pages := make([][]byte, 10)
	tree := NewHashTree(func(i int) []byte {
		if i >= len(pages) {
			return []byte{}
		}
		return pages[i]
	})

	for i := 0; i < 10; i++ {
		pages[i] = []byte{byte(i)}
		tree.MarkUpdated(i)
	}
	err := tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	hashBefore := tree.GetHash()

	tree.MarkUpdated(5)
	tree.MarkUpdated(3)

	err = tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	hashAfter := tree.GetHash()

	if hashBefore != hashAfter {
		t.Errorf("Hash for the same unchanged state is different")
	}
}

func TestHashtreeChangedState(t *testing.T) {
	pages := make([][]byte, 10)
	tree := NewHashTree(func(i int) []byte {
		if i >= len(pages) {
			return []byte{}
		}
		return pages[i]
	})

	for i := 0; i < 10; i++ {
		pages[i] = []byte{byte(i)}
		tree.MarkUpdated(i)
	}
	err := tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	hashBefore := tree.GetHash()

	pages[5] = []byte{42}
	tree.MarkUpdated(5)

	err = tree.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	hashAfter := tree.GetHash()

	if hashBefore == hashAfter {
		t.Errorf("Hash for the changed state is the same")
	}
}

func TestTwoTreesWithSameStateProvidesSameHash(t *testing.T) {
	// initialize two different states
	pagesA := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {0}, {0}}
	pagesB := [][]byte{{0}, {42}, {2}, {3}, {4}, {5}, {6}, {7}, {0}, {0}, {0}, {0}}
	treeA := NewHashTree(func(i int) []byte {
		return pagesA[i]
	})
	treeB := NewHashTree(func(i int) []byte {
		return pagesB[i]
	})
	for i := 0; i < 8; i++ {
		treeA.MarkUpdated(i)
		treeB.MarkUpdated(i)
	}
	treeA.MarkUpdated(8)
	treeA.MarkUpdated(9)
	err := treeA.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	err = treeB.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	firstHashA := treeA.GetHash()
	firstHashB := treeB.GetHash()
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
	err = treeA.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}
	err = treeB.Commit()
	if err != nil {
		t.Fatalf("failed to commit; %s", err)
	}

	if firstHashA != treeA.GetHash() {
		t.Errorf("the unchanged tree A provides a different hash")
	}
	if firstHashB == treeB.GetHash() {
		t.Errorf("the changed tree B provides the same hash")
	}
	if treeA.GetHash() != treeB.GetHash() {
		t.Errorf("the trees changed to be identical provides different hashes")
	}
}
