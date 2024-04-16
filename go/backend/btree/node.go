//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package btree

import "github.com/Fantom-foundation/Carmen/go/common"

type node[K any] interface {
	ForEacher[K]
	nodeChecker[K]
	common.MemoryFootprintProvider

	// insert finds an in-order position of the key and inserts it in this node.
	// When the key already exits, nothing happens.
	// When the key is inserted and its capacity does not exceed, it is added into this node.
	// When the capacity exceeds, this node is split into two, this one (i.e. the "left" node)
	// and a new one (i.e. the "right" node), and the keys are distributed between these two nodes.
	// Keys are split in the middle, and the middle key is returned. When the number
	// of keys is even, the right node has one key less  than the left one.
	// The right node, the middle key, and a split flag are returned.
	insert(key K) (right node[K], middle K, split bool)

	// contains returns true when the input key exists in this node or its children
	contains(key K) bool

	// remove deletes the input key from this node or its child.
	// if a subtree rooted at this node shrinks, a new root is returned.
	// For convenience, the same node is returned even when the tree does not shrink.
	remove(key K) node[K]

	// removeAt removes key and potentially a link to a child at the index.
	// If the index is zero and the number of keys is greater than one, i.e. the key is at the beginning of a chain,
	// left child is deleted as well. For other indexes, or a single key node, right child is removed.
	// if the node is leaf, no child is changed and returned node is nil.
	// Removed key is returned. For non-leaf node, the deleted child is returned as well.
	removeAt(index int) (K, node[K])

	// insertAt inserts key at the index.
	// it inserts child nodes left and right from this key.
	// The nodes may be nil and in this case nodes are not added.
	// If the node is a leaf, no children are added, i.e. if hey are provided, they are ignored
	insertAt(index int, k K, left, right node[K])

	// append copies content of child node at the end of this node.
	// The input key is appended to the destination node together
	// with keys of the child node. When the nodes are non-leaf, children are copied as well
	append(k K, child node[K])

	// getAt returns a child and key at the input index.
	// For a leaf node, child is returned as nil.
	// When the last child of a node is requested, the last key is returned as well
	// even if the index is actually one position beyond this key.
	// This method does not check bounds and will panic when called for indexes ouf of the size
	// the node.
	getAt(index int) (k K, child node[K])

	// getKeys returns keys of a node to be checked
	getKeys() []K

	// size return number of keys in current node
	size() int

	// findItem finds a key in the list, if it exists.
	// It returns the index of the key that was found, and it returns true.
	// If the key does not exist, it returns false and the index is equal to the last
	// visited position in the list, traversed using binary search.
	// The index is increased by one when the last visited key was lower than the input key
	// so the new key may be inserted after this key.
	// It means the index can be used as a position to insert the key in the list.
	findItem(key K) (index int, exists bool)

	//next moves position of the input iterator, and returns next key using the iterator.
	next(iterator *Iterator[K]) (k K)

	//hasNext returns true if next item exits.
	hasNext(iterator *Iterator[K]) bool
}

type ForEacher[K any] interface {
	// ForEach iterates ordered keys of the node including possible children
	ForEach(callback func(k K))
}

// nodeChecker allows for checking node properties
type nodeChecker[K any] interface {
	// checkProperties verifies BTree properties of a node,
	// i.e. it checks minimal capacity, order of keys, tree depth
	checkProperties(treeDepth *int, currentLevel int) error
}

// isMinSize returns true if the size of the input node is at minimal capacity.
// Minimal capacity is half of the full capacity rounded up.
func isMinSize[K any](m node[K], capacity int) bool {
	return m.size() <= (capacity-1)/2
}
