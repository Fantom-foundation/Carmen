package htfile

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"hash"
	"io"
	"os"
)

const HashLength = 32

// HashTree is a structure allowing to make a hash of the whole database state.
// It obtains hashes of individual data pages and reduce them to a hash of the entire state.
type HashTree struct {
	path         string
	factor       int          // the branching factor - amount of child nodes per one parent node
	dirtyPages   map[int]bool // set of dirty flags of the tree nodes
	pageProvider hashtree.PageProvider
}

// hashTreeFactory is used for implementation of hashTreeFactory method
type hashTreeFactory struct {
	path            string
	branchingFactor int
}

// CreateHashTreeFactory creates a new instance of the hashTreeFactory
func CreateHashTreeFactory(path string, branchingFactor int) *hashTreeFactory {
	return &hashTreeFactory{path: path, branchingFactor: branchingFactor}
}

// Create creates a new instance of the HashTree
func (f *hashTreeFactory) Create(pageProvider hashtree.PageProvider) hashtree.HashTree {
	return NewHashTree(f.path, f.branchingFactor, pageProvider)
}

// NewHashTree constructs a new HashTree
func NewHashTree(path string, branchingFactor int, pageProvider hashtree.PageProvider) *HashTree {
	return &HashTree{
		path:         path,
		factor:       branchingFactor,
		dirtyPages:   map[int]bool{},
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

func (ht *HashTree) layerFile(layer int) (path string) {
	return fmt.Sprintf("%s/%X", ht.path, layer)
}

// calculateHash computes the hash of given data
func (ht *HashTree) calculateHash(hasher hash.Hash, content []byte) (hash []byte, err error) {
	hasher.Reset()
	_, err = hasher.Write(content)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyPages[page] = true
}

// childrenOfNode provides a concatenation of all children of given node
func (ht *HashTree) childrenOfNode(childrenLayer *os.File, node int) ([]byte, error) {
	childrenStart := int64(ht.firstChildOf(node)) * HashLength
	childrenLength := ht.factor * HashLength
	return ht.readLayer(childrenLayer, childrenStart, childrenLength)
}

// readLayer provides a substring of a layer as a slice of bytes
func (ht *HashTree) readLayer(layer *os.File, from int64, length int) ([]byte, error) {
	_, err := layer.Seek(from, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek in hashtree layer; %s", err)
	}

	bytes := make([]byte, length)
	_, err = layer.Read(bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read hashtree layer; %s", err)
	}
	return bytes, nil
}

// getLayerSize provides the size of a hashtree layer in bytes
func (ht *HashTree) getLayerSize(layer *os.File) (size int64, err error) {
	info, err := layer.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// getLayersCount provides the amount of hashtree layers
func (ht *HashTree) getLayersCount() (count int, err error) {
	files, err := os.ReadDir(ht.path)
	return len(files), err
}

// commit updates the necessary parts of the hashing tree
func (ht *HashTree) commit() (hash []byte, err error) {
	var childrenLayer, parentsLayer *os.File
	defer func() {
		if childrenLayer != nil {
			childrenLayer.Close()
		}
		if parentsLayer != nil {
			parentsLayer.Close()
		}
	}()

	hasher := sha256.New()
	dirtyNodes := ht.dirtyPages // nodes at level 0 are 1:1 to pages
	ht.dirtyPages = make(map[int]bool)

	for layerId := 0; ; layerId++ {
		if childrenLayer != nil {
			childrenLayer.Close()
		}
		childrenLayer = parentsLayer
		parentsLayer, err = os.OpenFile(ht.layerFile(layerId), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open layer file %d; %s", layerId, err)
		}

		// hash children nodes into (dirty) parent nodes
		dirtyNodes, err = ht.updateDirtyNodes(childrenLayer, parentsLayer, layerId, dirtyNodes, hasher)
		if err != nil {
			return nil, err
		}

		layerSize, err := ht.getLayerSize(parentsLayer)
		if err != nil {
			return nil, fmt.Errorf("failed to get layer length; %s", err)
		}
		if layerSize < HashLength { // the layer is empty
			if layerId == 0 {
				return nil, nil // no data in the db - should return zero hash
			} else {
				return nil, fmt.Errorf("unexpected size %d of a hashtree layer %d", layerSize, layerId)
			}
		}
		if layerSize == HashLength {
			// this layer has only one hash - it is the root
			return ht.readLayer(parentsLayer, 0, HashLength)
		}
		// otherwise continue with the following layer
	}
}

// updateDirtyNodes updates parent nodes marked as dirty with a hash of its children
func (ht *HashTree) updateDirtyNodes(childrenLayer, parentsLayer *os.File, layerId int, dirtyNodes map[int]bool, hasher hash.Hash) (newDirtyNodes map[int]bool, err error) {
	newDirtyNodes = make(map[int]bool)
	for node := range dirtyNodes {
		var content, nodeHash []byte
		if layerId == 0 {
			// hash the data of the page
			content, err = ht.pageProvider.GetPage(node)
		} else {
			// hash children of the current node
			content, err = ht.childrenOfNode(childrenLayer, node)
		}
		if err != nil {
			return nil, err
		}
		nodeHash, err = ht.calculateHash(hasher, content)
		if err != nil {
			return nil, err
		}
		// update the hash of this node
		err = ht.updateNode(parentsLayer, node, nodeHash)
		if err != nil {
			return nil, fmt.Errorf("failed to update hashtree node %d/%d; %s", layerId, node, err)
		}
		// parent of the updated node needs to be updated - mark dirty
		newDirtyNodes[ht.parentOf(node)] = true
	}
	return newDirtyNodes, nil
}

// updateNode updates the hash-node value to the given value
func (ht *HashTree) updateNode(layerFile *os.File, node int, nodeHash []byte) error {
	_, err := layerFile.WriteAt(nodeHash, int64(node*HashLength))
	return err
}

// HashRoot provides the hash in the root of the hashing tree
func (ht *HashTree) HashRoot() (out common.Hash, err error) {
	hash, err := ht.commit()
	if err != nil {
		return common.Hash{}, err
	}
	copy(out[:], hash)
	return
}
