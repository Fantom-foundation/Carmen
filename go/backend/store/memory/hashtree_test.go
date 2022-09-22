package memory

import (
	"fmt"
	"testing"
)

func TestHashtreeOneCommit(t *testing.T) {
	tree := HashTree{
		tree:       [][][]byte{{}},
		dirtyNodes: [][]bool{{}},
	}

	for i := 0; i < 4; i++ {
		tree.MarkUpdated(i, []byte{byte(i)})
	}
	tree.Commit()

	fmt.Printf("hash: %x", tree.GetHash())
}

func TestHashtreeMultipleCommits(t *testing.T) {
	tree := HashTree{
		tree:       [][][]byte{{}},
		dirtyNodes: [][]bool{{}},
	}

	for i := 0; i < 4; i++ {
		tree.MarkUpdated(i, []byte{byte(i)})
		tree.Commit()
	}

	fmt.Printf("hash: %x", tree.GetHash())
}
