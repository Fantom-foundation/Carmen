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

func (ht *HashTree) calculateHash(childrenHashes [][]byte) []byte {
	h := sha256.New()
	for i := 0; i < len(childrenHashes); i++ {
		h.Write(childrenHashes[i])
	}
	return h.Sum(nil)
}

func (ht *HashTree) MarkUpdated(page int, content []byte) {
	pageHash := ht.calculateHash([][]byte{content})
	ht.updateNodeHash(0, page, pageHash)
}

func (ht *HashTree) Commit() {
	for layer := 1; layer < len(ht.tree); layer++ {
		for node := 0; node < len(ht.tree[layer]); node++ {
			if ht.dirtyNodes[layer][node] {
				firstChild := firstChildrenOf(node)
				nodeHash := ht.calculateHash(ht.tree[layer-1][firstChild : firstChild+NODE_LEN])
				ht.updateNodeHash(layer, node, nodeHash)
			}
		}
	}

	lastLayer := len(ht.tree) - 1
	if len(ht.tree[lastLayer]) > 1 {
		nodeHash := ht.calculateHash(ht.tree[lastLayer])
		ht.tree = append(ht.tree, [][]byte{nodeHash})
		ht.dirtyNodes = append(ht.dirtyNodes, []bool{false})
	}
}

func (ht *HashTree) updateNodeHash(layer int, node int, nodeHash []byte) {
	if len(ht.tree[layer]) == node {
		ht.tree[layer] = append(ht.tree[0], nodeHash)
	} else {
		ht.tree[layer][node] = nodeHash
	}
	if layer != 0 {
		ht.dirtyNodes[layer][node] = false
	}

	parent := parentOf(node)
	if len(ht.dirtyNodes[layer+1]) == parent {
		ht.dirtyNodes[layer+1] = append(ht.dirtyNodes[layer+1], true)
	} else {
		ht.dirtyNodes[layer+1][parent] = true
	}
}

func (ht *HashTree) GetHash() []byte {
	lastLayer := len(ht.tree) - 1
	return ht.tree[lastLayer][0]
}
