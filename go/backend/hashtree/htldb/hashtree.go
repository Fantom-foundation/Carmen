//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package htldb

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"hash"
	"unsafe"
)

const (
	HashLength = 32
	MaxLayer   = 0xFF
	MaxNode    = 0xFFFFFFFF
)

// HashTree is a structure allowing to make a hash of the whole database state.
// It obtains hashes of individual data pages and reduce them to a hash of the entire state.
type HashTree struct {
	db           backend.LevelDB
	table        backend.TableSpace
	factor       int          // the branching factor - amount of child nodes per one parent node
	dirtyPages   map[int]bool // set of dirty flags of the tree nodes
	maxPage      int
	pageProvider hashtree.PageProvider
}

// hashTreeFactory is used for implementation of hashTreeFactory method
type hashTreeFactory struct {
	db              backend.LevelDB
	table           backend.TableSpace
	branchingFactor int
}

// CreateHashTreeFactory creates a new instance of the hashTreeFactory
func CreateHashTreeFactory(db backend.LevelDB, table backend.TableSpace, branchingFactor int) *hashTreeFactory {
	return &hashTreeFactory{db: db, table: table, branchingFactor: branchingFactor}
}

// Create creates a new instance of the HashTree
func (f *hashTreeFactory) Create(pageProvider hashtree.PageProvider) hashtree.HashTree {
	return NewHashTree(f.db, f.table, f.branchingFactor, pageProvider)
}

// NewHashTree constructs a new HashTree
func NewHashTree(db backend.LevelDB, table backend.TableSpace, branchingFactor int, pageProvider hashtree.PageProvider) *HashTree {
	return &HashTree{
		db:           db,
		table:        table,
		factor:       branchingFactor,
		dirtyPages:   map[int]bool{},
		pageProvider: pageProvider,
	}
}

// Reset removes the hashtree content
func (ht *HashTree) Reset() error {
	dbStartKey := getNodeDbKey(ht.table, 0, 0).ToBytes()
	dbEndKey := getNodeDbKey(ht.table, MaxLayer, MaxNode).ToBytes()
	r := util.Range{Start: dbStartKey, Limit: dbEndKey}
	iter := ht.db.NewIterator(&r, nil)
	defer iter.Release()

	var batch leveldb.Batch
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	if err := iter.Error(); err != nil {
		return err
	}
	err := ht.db.Write(&batch, nil)
	ht.dirtyPages = map[int]bool{}
	return err
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyPages[page] = true
	if page > ht.maxPage {
		ht.maxPage = page
	}
}

// HashRoot provides the hash in the root of the hashing tree
func (ht *HashTree) HashRoot() (out common.Hash, err error) {
	hashBytes, err := ht.commit()
	if err != nil {
		return common.Hash{}, err
	}
	copy(out[:], hashBytes)
	return
}

// GetPageHash provides a hash of the tree leaf node.
func (ht *HashTree) GetPageHash(page int) (common.Hash, error) {
	if ht.dirtyPages[page] {
		_, err := ht.commit()
		if err != nil {
			return common.Hash{}, err
		}
	}
	return GetPageHashFromLdb(ht.table, page, ht.db)
}

// GetPageHashFromLdb provides a hash of the tree leaf node from given LevelDB snapshot
func GetPageHashFromLdb(table backend.TableSpace, page int, db backend.LevelDBReader) (common.Hash, error) {
	dbKey := getNodeDbKey(table, 0, page).ToBytes()
	hashBytes, err := db.Get(dbKey, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get page hash; %s", err)
	}
	return *(*common.Hash)(hashBytes), nil
}

// GetBranchingFactor provides the tree branching factor
func (ht *HashTree) GetBranchingFactor() int {
	return ht.factor
}

// childrenOfNode provides a concatenation of all children of given node
func (ht *HashTree) childrenOfNode(layer, node int) (data []byte, err error) {
	// use iterator to read nodes for the current branching factor
	firstNode := ht.firstChildOf(node)
	lastNode := firstNode + ht.factor
	dbStartKey := getNodeDbKey(ht.table, layer-1, firstNode).ToBytes()
	dbEndKey := getNodeDbKey(ht.table, layer-1, lastNode).ToBytes()
	r := util.Range{Start: dbStartKey, Limit: dbEndKey}
	iter := ht.db.NewIterator(&r, nil)
	defer iter.Release()

	// create the page first
	for iter.Next() {
		data = append(data, iter.Value()...)
	}

	// extend the page if needed
	if len(data) < ht.factor*HashLength {
		extraBytes := make([]byte, ht.factor*HashLength-len(data))
		data = append(data, extraBytes...)
	}

	err = iter.Error()
	return
}

// layerLength returns index of last nodes in this layer, which is the length of this layer
func (ht *HashTree) layerLength(layer int) (length int, err error) {
	// set the range for full layer
	firstNode := getNodeDbKey(ht.table, layer, 0).ToBytes()
	lastNode := getNodeDbKey(ht.table, layer, 0xFFFFFFFF).ToBytes()
	r := util.Range{Start: firstNode, Limit: lastNode}
	iter := ht.db.NewIterator(&r, nil)
	defer iter.Release()
	if iter.Last() {
		key := iter.Key()
		// layer length are two last bytes (i.e. the index of the last node in the layer)
		length = int(binary.BigEndian.Uint16(key[len(key)-2:]))
	}
	err = iter.Error()
	return
}

// getRootHash reads the root hash from the database
func (ht *HashTree) getRootHash() (hash []byte, err error) {
	// set the range for full layers
	firstNode := getNodeDbKey(ht.table, 0, 0).ToBytes()
	lastNode := getNodeDbKey(ht.table, 0xFF, 0).ToBytes()
	r := util.Range{Start: firstNode, Limit: lastNode}
	iter := ht.db.NewIterator(&r, nil)
	defer iter.Release()
	if iter.Last() {
		hash = iter.Value()
	}
	err = iter.Error()

	return
}

// updateNode updates the hash-node value to the given value
func (ht *HashTree) updateNode(layer, node int, nodeHash []byte) error {
	dbKey := getNodeDbKey(ht.table, layer, node).ToBytes()
	return ht.db.Put(dbKey, nodeHash, nil)
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
func (ht *HashTree) calculateHash(hasher hash.Hash, content []byte) (hash []byte, err error) {
	hasher.Reset()
	_, err = hasher.Write(content)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

// updateDirtyNodes updates parent nodes marked as dirty with a hash of its children
func (ht *HashTree) updateDirtyNodes(layer int, dirtyNodes map[int]bool, hasher hash.Hash) (newDirtyNodes map[int]bool, nodeHash []byte, err error) {
	newDirtyNodes = make(map[int]bool)
	for node := range dirtyNodes {
		var content []byte
		if layer == 0 {
			// hash the data of the page
			content, err = ht.pageProvider.GetPage(node)
		} else {
			// hash children of the current node
			content, err = ht.childrenOfNode(layer, node)
		}
		if err != nil {
			return nil, nil, err
		}
		nodeHash, err = ht.calculateHash(hasher, content)
		if err != nil {
			return nil, nil, err
		}
		// update the hash of this node
		err = ht.updateNode(layer, node, nodeHash)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to update hashtree node %d/%d; %s", layer, node, err)
		}
		// parent of the updated node needs to be updated - mark dirty
		newDirtyNodes[ht.parentOf(node)] = true
	}
	return
}

// commit updates the necessary parts of the hashing tree
func (ht *HashTree) commit() (hash []byte, err error) {

	// singular case there was no change (i.e. commit called either multiple times or on an empty tree
	if len(ht.dirtyPages) == 0 {
		return ht.getRootHash()
	}

	hasher := sha256.New()
	dirtyNodes := ht.dirtyPages // nodes at level 0 are 1:1 to pages
	ht.dirtyPages = make(map[int]bool)

	// fetch the number of pages at the bottom
	numPages, err := ht.layerLength(0)
	if ht.maxPage > numPages {
		numPages = ht.maxPage
	}
	numPages++ // max node index is N-1, increase to have N pages

	for layerId := 0; ; layerId++ {
		// hash children nodes into (dirty) parent nodes
		dirtyNodes, hash, err = ht.updateDirtyNodes(layerId, dirtyNodes, hasher)
		if numPages <= 1 || err != nil {
			break
		}
		// ceiling when the division overflows to a next page
		padding := 0
		if numPages%ht.factor != 0 {
			padding = 1
		}
		// reduce number of pages for next loop
		numPages = numPages/ht.factor + padding
	}

	return
}

// getNodeDbKey provides the leveldb key, where can be the hash of given node found
func getNodeDbKey(table backend.TableSpace, layer, node int) backend.DbKey {
	//  the key is: [tableSpace]H[layer][node]
	// layer is 8bit (256 layers Max)
	// node is 32bit
	var dbKey backend.DbKey
	dbKey[0] = byte(table)
	dbKey[1] = byte(backend.HashKey)
	dbKey[2] = uint8(layer)
	binary.BigEndian.PutUint32(dbKey[3:], uint32(node))
	return dbKey
}

// GetMemoryFootprint provides the size of the hash-tree in memory in bytes
func (ht *HashTree) GetMemoryFootprint() *common.MemoryFootprint {
	dirtyItemSize := unsafe.Sizeof(struct {
		key   int
		value bool
	}{})
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*ht) + uintptr(len(ht.dirtyPages))*dirtyItemSize)
	mf.AddChild("levelDb", ht.db.GetMemoryFootprint())
	return mf
}
