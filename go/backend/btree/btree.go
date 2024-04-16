//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.
//

package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// BTree implements a classic B-tree.
// The tree is initialized with the node capacity, and the tree is balanced
// so no node exceeds the capacity.
// The keys are stored  ordered using insertion order. If a node exceeds its capacity
// it is split in two, and the keys are distributed into left and right nodes.
// The middle key is moved to the parent node.
type BTree[K any] struct {
	root node[K]

	nodeCapacity int
	comparator   common.Comparator[K]
}

// NewBTree creates a new instance of BTree
func NewBTree[K any](nodeCapacity int, comparator common.Comparator[K]) *BTree[K] {
	return &BTree[K]{
		root:         newLeafNode(nodeCapacity, comparator),
		nodeCapacity: nodeCapacity,
		comparator:   comparator,
	}
}

// Insert inserts this the input key in this BTree. If the key already exists, nothing happens.
func (m *BTree[K]) Insert(key K) {
	right, middle, split := m.root.insert(key)
	if split {
		newNode := initNewInnerNode[K](m.root, right, middle, m.nodeCapacity, m.comparator)
		m.root = newNode
	}
}

// Remove deletes the input key from this BTree. If the key already exists, nothing happens
func (m *BTree[K]) Remove(key K) {
	m.root = m.root.remove(key)
}

// NewIterator creates am iterator for the input key ranges.
func (m *BTree[K]) NewIterator(start, end K) *Iterator[K] {
	return newIterator[K](start, end, m.root)
}

// Contains returns true if the input key exists in this BTree
func (m *BTree[K]) Contains(key K) bool {
	return m.root.contains(key)
}

// ForEach iterates over this BTree and visits all keys in order
func (m *BTree[K]) ForEach(callback func(k K)) {
	m.root.ForEach(callback)
}

func (m BTree[K]) String() string {
	return fmt.Sprintf("%v", m.root)
}

func (m *BTree[K]) checkProperties() error {
	height := -1
	return m.root.checkProperties(&height, 0)
}

func (m *BTree[K]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("nodes", m.root.GetMemoryFootprint())
	return mf
}
