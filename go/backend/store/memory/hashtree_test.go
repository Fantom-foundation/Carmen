package memory

import (
	"fmt"
	"testing"
)

func TestHashtreeOneCommit(t *testing.T) {
	tree := HashTree{
		tree:       [][][]byte{{}},
		dirtyNodes: []map[int]bool{{}},
		pageObtainer: func(i int) []byte {
			return []byte{byte(i)}
		},
	}

	for i := 0; i < 4; i++ {
		tree.MarkUpdated(i)
	}
	tree.Commit()

	fmt.Printf("hash: %x", tree.GetHash())
}

func TestHashtreeMultipleCommits(t *testing.T) {
	tree := HashTree{
		tree:       [][][]byte{{}},
		dirtyNodes: []map[int]bool{{}},
		pageObtainer: func(i int) []byte {
			return []byte{byte(i)}
		},
	}

	for i := 0; i < 4; i++ {
		tree.MarkUpdated(i)
		tree.Commit()
	}

	fmt.Printf("hash: %x", tree.GetHash())
}
