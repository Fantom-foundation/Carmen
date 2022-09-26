package memory

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// BranchingFactor is the amount of child nodes per one parent node
const BranchingFactor = 3

// HashTree is a structure allowing to make a hash of the whole state.
// It obtains hashes of individual data pages and reduce them to a hash of the whole state.
type HashTree struct {
	tree       [][][]byte       // tree of hashes [layer][node][byte of hash]
	dirtyNodes []map[int]bool   // set of dirty flags of the tree nodes [layer][node]
	getPage    func(int) []byte // callback for obtaining data pages
}

// NewHashTree constructs a new HashTree
func NewHashTree(pageObtainer func(i int) []byte) HashTree {
	return HashTree{
		tree:       [][][]byte{{}},
		dirtyNodes: []map[int]bool{{}},
		getPage:    pageObtainer,
	}
}

// parentOf provides an index of a parent node, by the child index
func parentOf(childIdx int) int {
	return childIdx / BranchingFactor
}

// firstChildOf provides an index of the first child, by the index of the parent node
func firstChildOf(parentIdx int) int {
	return parentIdx * BranchingFactor
}

// calculateHash computes the hash of given data
func calculateHash(childrenHashes [][]byte) (hash []byte, err error) {
	h := sha256.New()
	for i := 0; i < len(childrenHashes); i++ {
		_, err = h.Write(childrenHashes[i])
		if err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on Commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyNodes[0][page] = true
}

// Commit updates the necessary parts of the hashing tree
func (ht *HashTree) Commit() (err error) {
	for layer := 0; layer < len(ht.tree); layer++ {
		for node, _ := range ht.dirtyNodes[layer] {
			var nodeHash []byte
			if layer == 0 {
				// hash the data of the page, which comes from the outside
				content := ht.getPage(node)
				nodeHash, err = calculateHash([][]byte{content})
			} else {
				// hash children of current node
				childrenStart := firstChildOf(node)
				childrenEnd := childrenStart + BranchingFactor
				nodeHash, err = calculateHash(ht.tree[layer-1][childrenStart:childrenEnd])
			}
			if err != nil {
				return err
			}
			// update the hash of this node, and extend the tree if needed
			err = ht.updateNode(layer, node, nodeHash)
			if err != nil {
				return err
			}
		}
		// if the last layer has more than one node, need to add a new layer
		lastLayer := len(ht.tree) - 1
		if layer == lastLayer && len(ht.tree[lastLayer]) > 1 {
			ht.tree = append(ht.tree, [][]byte{{}})
		}
	}
	return nil
}

// updateNode updates the hash-node value to the given value and marks its parent as dirty (needing a recalculation)
func (ht *HashTree) updateNode(layer int, node int, nodeHash []byte) error {
	// extend the layer size if necessary
	if node >= len(ht.tree[layer]) {
		newLayerSize := (node/BranchingFactor + 1) * BranchingFactor
		ht.tree[layer] = append(ht.tree[layer], make([][]byte, newLayerSize-len(ht.tree[layer]))...)
	}

	ht.tree[layer][node] = nodeHash
	delete(ht.dirtyNodes[layer], node) // node hash updated, no longer dirty

	// parent of the updated node needs to be updated - mark dirty
	parent := parentOf(node)
	if len(ht.dirtyNodes) <= layer+1 {
		ht.dirtyNodes = append(ht.dirtyNodes, map[int]bool{})
	}
	ht.dirtyNodes[layer+1][parent] = true
	return nil
}

// HashRoot provides the hash in the root of the hashing tree
func (ht *HashTree) HashRoot() (out common.Hash) {
	lastLayer := len(ht.tree) - 1
	if len(ht.tree[lastLayer]) == 0 {
		return common.Hash{}
	}
	copy(out[:], ht.tree[lastLayer][0])
	return
}
