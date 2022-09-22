package memory

import (
	"crypto/sha256"
)

const NODE_LEN = 3

// parentOf provides an index of a parent node, by the child index
func parentOf(childIdx int) int {
	return childIdx / NODE_LEN
}

// firstChildrenOf provides an index of the first child, by the index of the parent node
func firstChildrenOf(parentIdx int) int {
	return parentIdx * NODE_LEN
}

type HashTree struct {
	tree         [][][]byte       // tree of hashes [layer][node][byte of hash]
	dirtyNodes   []map[int]bool   // set of dirty flags of the tree nodes [layer][node]
	pageObtainer func(int) []byte // callback for obtaining data pages
}

func (ht *HashTree) calculateHash(childrenHashes [][]byte) []byte {
	h := sha256.New()
	for i := 0; i < len(childrenHashes); i++ {
		h.Write(childrenHashes[i])
	}
	return h.Sum(nil)
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on Commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyNodes[0][page] = true
}

// Commit updates the necessary parts of the hashing tree
func (ht *HashTree) Commit() {
	for layer := 0; layer < len(ht.tree); layer++ {
		for node, _ := range ht.dirtyNodes {
			var nodeHash []byte
			if layer == 0 {
				content := ht.pageObtainer(node)
				nodeHash = ht.calculateHash([][]byte{content})
			} else {
				childrenStart := firstChildrenOf(node)
				childrenEnd := childrenStart + NODE_LEN
				if childrenEnd >= len(ht.tree[layer-1]) {
					childrenEnd = len(ht.tree[layer-1])
				}
				nodeHash = ht.calculateHash(ht.tree[layer-1][childrenStart:childrenEnd])
			}
			ht.updateNodeHash(layer, node, nodeHash)
		}
	}

	lastLayer := len(ht.tree) - 1
	if len(ht.tree[lastLayer]) > 1 { // need to add new layer
		nodeHash := ht.calculateHash(ht.tree[lastLayer])
		ht.tree = append(ht.tree, [][]byte{nodeHash})
		ht.dirtyNodes = append(ht.dirtyNodes, make(map[int]bool))
	}
}

// updateNodeHash updates the hash-node value to the given value and marks its parent as dirty (needing a recalculation)
func (ht *HashTree) updateNodeHash(layer int, node int, nodeHash []byte) {
	if len(ht.tree[layer]) == node {
		ht.tree[layer] = append(ht.tree[0], nodeHash)
	} else {
		ht.tree[layer][node] = nodeHash
	}

	delete(ht.dirtyNodes[layer], node) // node hash updated, no longer dirty

	if len(ht.dirtyNodes) > layer+1 {
		parent := parentOf(node)
		ht.dirtyNodes[layer+1][parent] = true // parent of this updated node needs to be updated
	}
}

// GetHash provides the hash in the root of the hashing tree
func (ht *HashTree) GetHash() []byte {
	lastLayer := len(ht.tree) - 1
	return ht.tree[lastLayer][0]
}
