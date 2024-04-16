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

package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// InnerNode contains keys as the LeafNode, in addition, it contains array of children elements.
type InnerNode[K any] struct {
	LeafNode[K] // LeafNode is the base for this node, it contains common properties such as the keys, and common methods

	children []node[K] // children is the array of IDs of child nodes
}

// newInnerNode creates a new instance with the given capacity.
func newInnerNode[K any](capacity int, comparator common.Comparator[K]) *InnerNode[K] {
	return &InnerNode[K]{
		children: make([]node[K], 0, capacity+2),
		LeafNode: *newLeafNode[K](capacity, comparator)}
}

// initNewInnerNode creates a new inner node.
// It assigns left and right child, and a value that falls between these children.
// The left child can be set to nil, in which case it is not added
func initNewInnerNode[K any](left, right node[K], middle K, capacity int, comparator common.Comparator[K]) *InnerNode[K] {
	innerNode := newInnerNode[K](capacity, comparator)
	innerNode.children = append(innerNode.children, left)
	innerNode.keys = append(innerNode.keys, middle)
	if right != nil {
		innerNode.children = append(innerNode.children, right)
	}
	return innerNode
}

func (m *InnerNode[K]) insert(key K) (right node[K], middle K, split bool) {
	index, exists := m.findItem(key)
	if !exists {
		// insert into child, when split has happened, insert result in this node
		if right, middle, split := m.children[index].insert(key); split {
			m.insertAt(index, middle, nil, right)
		}

		// check and potentially split this node
		if len(m.keys) >= m.capacity+1 {
			right, middle = m.split()
			split = true
		}
	}

	return
}

func (m *InnerNode[K]) contains(key K) bool {
	index, exists := m.findItem(key)

	if exists {
		return true
	}

	return m.children[index].contains(key)
}

// split this node into two. Keys in this node are reduced to half
// and the other half is put in the output node.
func (m *InnerNode[K]) split() (right *InnerNode[K], middle K) {
	right = newInnerNode[K](m.capacity, m.comparator)
	midIndex := len(m.keys) / 2

	// collect middle value
	middle = m.keys[midIndex]

	// split keys
	right.keys = append(right.keys, m.keys[midIndex+1:len(m.keys)]...)
	m.keys = m.keys[0:midIndex]

	// split children
	right.children = append(right.children, m.children[midIndex+1:len(m.children)]...)
	m.children = m.children[0 : midIndex+1]

	return
}

func (m *InnerNode[K]) insertAt(index int, key K, left, right node[K]) {
	m.LeafNode.insertAt(index, key, nil, nil)
	if left != nil {
		m.insertChildAt(left, index)
	}
	if right != nil {
		m.insertChildAt(right, index+1)
	}
}

// insertChildAt inserts a child node at the input index.
func (m *InnerNode[K]) insertChildAt(node node[K], index int) {
	if index == len(m.children) {
		m.children = append(m.children, node)
	} else {
		for i := len(m.children) - 1; i >= index; i-- {
			if i+1 == len(m.children) {
				m.children = append(m.children, m.children[i])
			} else {
				m.children[i+1] = m.children[i]
			}
		}
		m.children[index] = node
	}
}

// ForEach iterates ordered keys
func (m *InnerNode[K]) ForEach(callback func(k K)) {
	for i, child := range m.children {
		child.ForEach(callback)
		if i < len(m.keys) {
			callback(m.keys[i])
		}
	}
}

// remove deletes key from this node or its children, potentially rebalancing the subtree rooted at this node.
// This operation performs a top-down approach, where it is assured at every level that the child
// where the algorithm descends has at least one key above the minimal capacity first, only then the algorithm descend
// to the subtree unless the key to delete is reached.
// At every level of recursion, when it is discovered that a child is at minimal capacity, rebalacing happens.
// Rebalancing has two forms: rotation or merging. Rotation borrows a key from a sibling via the parent node so that
// the keys are shifted sibling->parent->child. Merging happens when the sibling does not have enough keys to lend.
// Merging joins siblings and the key from their common parent.
// Deletion of key has two forms as well: for a leaf node, the key is directly removed, i.e. the leaf will have
// one less element after this operation. For a non-leaf node, the key is swapped with a key from leaf, and then the key
// is deleted from the leaf. The key is swapped with its rightmost predecessor od leftmost successor.
// While this algorithm is very straightforward, it has a subtle limitation: it assures that the subtree with the key
// to delete has sufficient number of keys (i.e. +1 of min capacity) at every level. It has a benefit that the algorithm
// does not have to look ahead of more levels, on the other hand it may produce some seemingly unnecessary rebalancing
// when the nodes at certain level are at minimal capacity already, but the key to delete is stored somewhere
// deeper in the tree. In this case the subtree is rebalanced at the current level without knowing if rebalancing
// is necessary for removing the key.
// This implementation has been inspired by: https://www.geeksforgeeks.org/delete-operation-in-b-tree/
func (m *InnerNode[K]) remove(key K) node[K] {
	if index, exists := m.findItem(key); exists {
		// key is in this node - remove it potentially borrowing/merging from children
		m.removeKeyAt(index)
	} else {
		// check if the index is at the last position
		// following merge operation can reduce one position from this node
		// and in this case deletion has to descent to the previous position
		isLastIndex := index == m.size()

		// key is not in this node - descent to child
		// also assure to fill the child if it does not have enough elements
		// this is done from here as this node has access to siblings of the child node
		if isMinSize(m.children[index], m.capacity) {
			m.rebalance(index)
		}

		// child reorganised to have enough keys, it can be deleted.
		if isLastIndex && index > m.size() {
			m.children[index-1].remove(key)
		} else {
			m.children[index].remove(key)
		}
	}

	if m.size() == 0 {
		// this node becomes empty as part of merge with children
		return m.children[0]
	}
	return m
}

// removeKeyAt removes key at the input position.
// It replaces the deleted key either by the right most key from the predecessor subtree,
// or by the left most keys from the successor subtree.
// If the subtree size is at minimal capacity, two child nodes are merged
func (m *InnerNode[K]) removeKeyAt(index int) {
	if !isMinSize(m.children[index], m.capacity) {
		// copy from predecessor if it has capacity
		predKey := m.getPred(index)
		// this key replaces the key we want to delete
		m.keys[index] = predKey
		// remove this key from the subtree
		m.children[index].remove(predKey)
	} else if !isMinSize(m.children[index+1], m.capacity) {
		// copy from successor if it has capacity
		sucKey := m.getSuc(index)
		// this key replaces the key we want to delete
		m.keys[index] = sucKey
		// remove this key from the subtree
		m.children[index+1].remove(sucKey)
	} else {
		// not enough spare keys at both sides, children have to be merged
		k := m.keys[index]
		m.merge(index)
		// repeat deletion after merge
		m.children[index].remove(k)
	}
}

// getPred locates and returns rightmost key from the subtree of a child denoted by input index
func (m *InnerNode[K]) getPred(index int) (k K) {
	n := m.children[index]

	for n != nil {
		// iterate to the right most subtree - i.e. the last index at every level
		k, n = n.getAt(n.size())
	}
	// at this point, "k" is the rightmost key from the rightmost leaf
	return
}

// getSuc locates and returns leftmost key from the subtree of a child denoted by input index
func (m *InnerNode[K]) getSuc(index int) (k K) {
	n := m.children[index+1]

	for n != nil {
		// iterate to the left most subtree - i.e. the first index at every level
		k, n = n.getAt(0)
	}
	// at this point, "k" is the leftmost key from the leftmost leaf
	return
}

// rebalance assures child nodes do not have minimal number of keys.
// It is done so by employing one of three strategies:
//  1. keys are borrowed from left child if it has sufficient number of spare keys
//  2. keys are borrowed from right child if it has sufficient number of spare keys
//  3. left and right keys are merged into one, together with their parent
func (m *InnerNode[K]) rebalance(index int) {
	if index != 0 && !isMinSize(m.children[index-1], m.capacity) {
		m.rotateRight(index - 1) // left child has capacity - can rotate right
	} else if index < m.size() && !isMinSize(m.children[index+1], m.capacity) {
		m.rotateLeft(index) // right child has capacity - can rotate left
	} else {
		// not enough keys left or right, have to merge
		if index == m.size() {
			m.merge(index - 1) // merge with last child, start the merge from the previous one
		} else {
			m.merge(index)
		}
	}
}

// rotateRight picks a predecessor child and its rightmost key. It puts this key in this node at the position,
// while moving the current key from this position to leftmost index of the right child.
// In other worlds it rotates the keys: left child -> parent -> right child.
// Any link of a rightmost (sub)-child of the left child is removed and linked to the leftmost position of the right child.
func (m *InnerNode[K]) rotateRight(index int) {
	left := m.children[index]
	right := m.children[index+1]

	// borrow from left and this node
	borrowParentKey := m.keys[index]
	borrowChildKey, borrowChildNode := left.removeAt(left.size() - 1) // right most index

	// push into right node - split() does not have to be checked, as rotation is happening
	// because there is not enough keys in the right.
	// push at the beginning - left most index
	right.insertAt(0, borrowParentKey, borrowChildNode, nil)

	// borrowed key from left becomes a new parent
	m.keys[index] = borrowChildKey
}

// rotateLeft picks a successor child and its leftmost key, puts it in this node at the input position, and the  key from current position
// is put in the left child at the last position.
func (m *InnerNode[K]) rotateLeft(index int) {
	left := m.children[index]
	right := m.children[index+1]

	// borrow from right and this node
	borrowParentKey := m.keys[index]
	borrowChildKey, borrowChildNode := right.removeAt(0) // left most index

	// push into left node - split() does not have to be checked, as rotation is happening
	// because there is not enough keys in the left.
	// push at the beginning - left most index
	left.insertAt(left.size(), borrowParentKey, nil, borrowChildNode)

	// borrowed key from right becomes a new parent
	m.keys[index] = borrowChildKey
}

// merge two children. It picks a child at the input index and a right child (i.e. index+1).
// It moves all keys from right to left child while the right child is released from the tree.
func (m *InnerNode[K]) merge(index int) {
	parentKey, _ := m.LeafNode.removeAt(index)
	right := m.removeChildAt(index + 1)
	left := m.children[index]
	left.append(parentKey, right)
}

func (m *InnerNode[K]) append(k K, sibling node[K]) {
	m.keys = append(m.keys, k)
	m.keys = append(m.keys, sibling.getKeys()...)
	// we can safely cast here because merged nodes at the same level are both either leafs or inner nodes
	m.children = append(m.children, sibling.(*InnerNode[K]).children...)
}

func (m *InnerNode[K]) getAt(index int) (k K, child node[K]) {
	k, _ = m.LeafNode.getAt(index)
	return k, m.children[index]
}

func (m *InnerNode[K]) removeAt(index int) (K, node[K]) {
	var removedNode node[K]
	if index == 0 && m.size() > 1 {
		removedNode = m.removeChildAt(index)
	} else {
		removedNode = m.removeChildAt(index + 1)
	}

	removedKey, _ := m.LeafNode.removeAt(index)
	return removedKey, removedNode
}

func (m *InnerNode[K]) removeChildAt(index int) node[K] {
	removedChild := m.children[index]
	for i := index; i < len(m.children)-1; i++ {
		m.children[i] = m.children[i+1]
	}
	m.children = m.children[0 : len(m.children)-1]

	return removedChild
}

func (m *InnerNode[K]) hasNext(iterator *Iterator[K]) (exists bool) {
	if iterator.visitChild {
		// if there is a child
		if iterator.currentLevel().currentIndex < iterator.currentLevel().endIndex+1 {
			// peek in it
			child := m.children[iterator.currentLevel().currentIndex]
			startIndex, _ := child.findItem(iterator.start)
			endIndex, _ := child.findItem(iterator.end)
			iterator.pushLevel(startIndex, endIndex, child)
			exists = iterator.HasNext()
		}
	} else {
		exists = m.LeafNode.hasNext(iterator)
	}
	return
}

func (m *InnerNode[K]) next(iterator *Iterator[K]) (k K) {
	if !iterator.HasNext() {
		return
	}
	if iterator.visitChild {
		// drill down to a child node first
		child := m.children[iterator.currentLevel().currentIndex]
		startIndex, _ := child.findItem(iterator.start)
		endIndex, _ := child.findItem(iterator.end)
		iterator.pushLevel(startIndex, endIndex, child)
		k = iterator.Next()
	} else {
		// no data in child, or child already visited - get a key from this node
		k = m.LeafNode.next(iterator)
		iterator.visitChild = true
	}

	return
}

func (m InnerNode[K]) String() string {
	var str string
	for i, child := range m.children {
		str += fmt.Sprintf("%v", child)
		if i < len(m.children)-1 {
			str += ", "
		}
		if i < len(m.keys) {
			str += fmt.Sprintf("%v", m.keys[i])
		}
		if i < len(m.children)-1 {
			str += ", "
		}
	}
	return fmt.Sprintf("[%v]", str)
}

func (m InnerNode[K]) checkProperties(treeDepth *int, currentLevel int) error {
	for _, child := range m.children {
		if err := child.checkProperties(treeDepth, currentLevel+1); err != nil {
			return err
		}
	}

	if len(m.children) != len(m.keys)+1 {
		return fmt.Errorf("num of keys lower than num of children: %d < %d", len(m.children), len(m.keys))
	}

	// check order including children
	for i := 0; i < len(m.keys); i++ {
		// right predecessor
		key := m.getPred(i)
		if m.comparator.Compare(&key, &m.keys[i]) >= 0 {
			return fmt.Errorf("keys not ordered: %v >= %v", m.keys[i], m.keys[i+1])
		}
		if i < len(m.keys)-1 && m.comparator.Compare(&m.keys[i], &m.keys[i+1]) >= 0 {
			return fmt.Errorf("keys not ordered: %v >= %v", m.keys[i], m.keys[i+1])
		}
	}

	// last child
	if len(m.keys) > len(m.children) {
		// left successor
		childKeys := m.children[len(m.children)-1].getKeys()
		key := childKeys[0]
		if m.comparator.Compare(&key, &m.keys[len(m.keys)-1]) <= 0 {
			return fmt.Errorf("keys not ordered: %v <= %v", &key, &m.keys[len(m.keys)-1])
		}
	}

	// check capacity (for non-root leaf)
	if currentLevel > 0 && m.size() < (m.capacity-1)/2 {
		return fmt.Errorf("size below minimal capacity: %d < %d", m.size(), m.capacity/2)
	}

	// check capacity is not exceeded
	if m.size() > m.capacity {
		return fmt.Errorf("size above the maximal capacity: %d > %d", m.size(), m.capacity)
	}

	return nil
}

func (m *InnerNode[K]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	var k K
	keysSize := uintptr(len(m.keys)) * unsafe.Sizeof(k)
	var childrenSize uintptr
	for _, child := range m.children {
		childrenSize += child.GetMemoryFootprint().Value()
	}
	return common.NewMemoryFootprint(selfSize + keysSize + childrenSize)
}
