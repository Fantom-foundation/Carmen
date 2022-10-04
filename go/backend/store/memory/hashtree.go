package memory

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"hash"
)

// HashTree is a structure allowing to make a hash of the whole database state.
// It obtains hashes of individual data pages and reduce them to a hash of the entire state.
type HashTree struct {
	factor       int            // the branching factor - amount of child nodes per one parent node
	tree         [][][]byte     // tree of hashes [layer][node][byte of hash]
	dirtyNodes   []map[int]bool // set of dirty flags of the tree nodes [layer][node]
	pageProvider hashtree.PageProvider
}

// hashTreeFactory is used for implementation of hashTreeFactory method
type hashTreeFactory struct {
	branchingFactor int
}

// CreateHashTreeFactory creates a new instance of the hashTreeFactory
func CreateHashTreeFactory(branchingFactor int) *hashTreeFactory {
	return &hashTreeFactory{branchingFactor: branchingFactor}
}

// Create creates a new instance of the HashTree
func (f *hashTreeFactory) Create(pageProvider hashtree.PageProvider) hashtree.HashTree {
	return NewHashTree(f.branchingFactor, pageProvider)
}

// NewHashTree constructs a new HashTree
func NewHashTree(branchingFactor int, pageProvider hashtree.PageProvider) *HashTree {
	return &HashTree{
		factor:       branchingFactor,
		tree:         [][][]byte{{}},
		dirtyNodes:   []map[int]bool{{}},
		pageProvider: pageProvider,
	}
}

// parentOf provides an index of a parent node, by the child index
func (ht *HashTree) parentOf(childIdx int) int {
	return childIdx / ht.factor
}

// firstChildOf provides an index of the first child, by the index of the parent node
func (ht *HashTree) firstChildOf(parentIdx int) int {
	return parentIdx * ht.factor
}

// calculateHash computes the hash of given data
func calculateHash(h hash.Hash, childrenHashes [][]byte) (hash []byte, err error) {
	h.Reset()
	for _, childHash := range childrenHashes {
		_, err = h.Write(childHash)
		if err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyNodes[0][page] = true
}

// commit updates the necessary parts of the hashing tree
func (ht *HashTree) commit() (err error) {
	h := sha256.New() // the hasher is created once for the whole block as it hashes the fastest
	for layer := 0; layer < len(ht.tree); layer++ {
		needNextLayer := false
		for node, _ := range ht.dirtyNodes[layer] {
			var nodeHash []byte
			if layer == 0 {
				// hash the data of the page, which comes from the outside
				var content []byte
				content, err = ht.pageProvider.GetPage(node)
				if err != nil {
					return err
				}
				nodeHash, err = calculateHash(h, [][]byte{content})
			} else {
				// hash children of current node
				childrenStart := ht.firstChildOf(node)
				childrenEnd := childrenStart + ht.factor
				nodeHash, err = calculateHash(h, ht.tree[layer-1][childrenStart:childrenEnd])
			}
			if err != nil {
				return err
			}
			// update the hash of this node, and extend the tree if needed
			ht.updateNode(layer, node, nodeHash)
			if node > 0 {
				needNextLayer = true
			}
		}
		// if the last layer has more than one node, need to add a new layer
		lastLayer := len(ht.tree) - 1
		if layer == lastLayer && needNextLayer {
			ht.tree = append(ht.tree, [][]byte{{}})
		}
	}
	return nil
}

// updateNode updates the hash-node value to the given value and marks its parent as dirty (needing a recalculation)
func (ht *HashTree) updateNode(layer int, node int, nodeHash []byte) {
	// extend the layer size if necessary
	if node >= len(ht.tree[layer]) {
		newLayerSize := (node/ht.factor + 1) * ht.factor
		for newLayerSize > len(ht.tree[layer]) {
			ht.tree[layer] = append(ht.tree[layer], make([]byte, common.HashSerializer{}.Size()))
		}
	}

	ht.tree[layer][node] = nodeHash
	delete(ht.dirtyNodes[layer], node) // node hash updated, no longer dirty

	// parent of the updated node needs to be updated - mark dirty
	parent := ht.parentOf(node)
	if len(ht.dirtyNodes) <= layer+1 {
		ht.dirtyNodes = append(ht.dirtyNodes, map[int]bool{})
	}
	ht.dirtyNodes[layer+1][parent] = true
}

// HashRoot provides the hash in the root of the hashing tree
func (ht *HashTree) HashRoot() (out common.Hash, err error) {
	err = ht.commit()
	if err != nil {
		return common.Hash{}, err
	}
	lastLayer := len(ht.tree) - 1
	if len(ht.tree[lastLayer]) == 0 {
		return common.Hash{}, nil
	}
	copy(out[:], ht.tree[lastLayer][0])
	return
}
