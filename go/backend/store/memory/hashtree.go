package memory

import (
	"crypto/sha256"
)

const NODE_LEN = 3

func parentOf(childIdx int) int {
	return childIdx / NODE_LEN
}

func firstChildrenOf(parentIdx int) int {
	return parentIdx * NODE_LEN
}

type HashTree struct {
	tree       [][][]byte // layer - node - byte of hash
	dirtyNodes [][]bool   // layer - node
}

func (ht *HashTree) MarkUpdated(page int, content []byte) {
	h := sha256.New()
	h.Write(content)
	pageHash := h.Sum(nil)

	if len(ht.tree[0]) == page {
		ht.tree[0] = append(ht.tree[0], pageHash)
	} else {
		ht.tree[0][page] = pageHash
	}

	parent := parentOf(page)
	if len(ht.dirtyNodes[0]) == parent {
		ht.dirtyNodes[0] = append(ht.dirtyNodes[0], true)
	} else {
		ht.dirtyNodes[0][parent] = true
	}
}

func (ht *HashTree) Commit() {
	for parentLayer := 1; parentLayer < len(ht.tree); parentLayer++ {
		for node := 0; node < len(ht.tree[parentLayer]); node++ {
			if ht.dirtyNodes[parentLayer-1][node] {

				h := sha256.New()
				firstChild := firstChildrenOf(node)
				for child := firstChild; child < firstChild+NODE_LEN; child++ {
					h.Write(ht.tree[parentLayer-1][child])
				}
				nodeHash := h.Sum(nil)

				if len(ht.tree[parentLayer]) == node {
					ht.tree[parentLayer] = append(ht.tree[0], nodeHash)
				} else {
					ht.tree[parentLayer][node] = nodeHash
				}
				ht.dirtyNodes[parentLayer-1][node] = false

				parent := parentOf(node)
				if len(ht.dirtyNodes[parentLayer]) == parent {
					ht.dirtyNodes[parentLayer] = append(ht.dirtyNodes[parentLayer], true)
				} else {
					ht.dirtyNodes[parentLayer][parent] = true
				}

			}

		}
	}

	lastLayer := len(ht.tree) - 1
	if len(ht.tree[lastLayer]) > 1 {
		h := sha256.New()
		for child := 0; child < len(ht.tree[lastLayer]); child++ {
			h.Write(ht.tree[lastLayer][child])
		}
		nodeHash := h.Sum(nil)

		ht.tree = append(ht.tree, [][]byte{nodeHash})
		ht.dirtyNodes = append(ht.dirtyNodes, []bool{false})
	}
}

func (ht *HashTree) GetHash() []byte {
	lastLayer := len(ht.tree) - 1
	return ht.tree[lastLayer][0]
}
