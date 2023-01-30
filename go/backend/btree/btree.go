package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// BTree implements a classic B-tree.
// The tree is initialized with the node capacity, and the tree is balanced
// so no node exceeds the capacity.
// The keys are stored  ordered using insertion order. If a node exceeds its capacity
// it is split in two, and the keys are distributed into left and right nodes.
// The middle key is moved to the parent node.
type BTree[K comparable] struct {
	// TODO replace by node ID later and fetch from the page pool
	root Node[K]

	nodeCapacity int
	comparator   common.Comparator[K]
}

// NewBTree creates a new instance of BTree
func NewBTree[K comparable](nodeCapacity int, comparator common.Comparator[K]) *BTree[K] {
	return &BTree[K]{
		root:         newLeafNode(nodeCapacity, comparator),
		nodeCapacity: nodeCapacity,
		comparator:   comparator,
	}
}

func (m *BTree[K]) Insert(key K) {
	right, middle, split := m.root.Insert(key)
	if split {
		newNode := initNewInnerNode[K](m.root, right, middle, m.nodeCapacity, m.comparator)
		m.root = newNode
	}
}

func (m *BTree[K]) Contains(key K) bool {
	if m.root != nil {
		return m.root.Contains(key)
	}

	return false
}

func (m *BTree[K]) ForEach(callback func(k K)) {
	if m.root != nil {
		m.root.ForEach(callback)
	}
}

func (m BTree[K]) String() string {
	return m.printDump()
}

// printDump collects debug print of this tree
func (m BTree[K]) printDump() string {
	switch v := m.root.(type) {
	case *LeafNode[K]:
		return fmt.Sprintf("%v", v)
	case *InnerNode[K]:
		lines := v.printDump(make([]string, 0), 0)
		str := fmt.Sprintf("%v\n", v.keys)
		for _, line := range lines {
			str = str + line + "\n"
		}
		return str
	default:
	}

	return ""
}
