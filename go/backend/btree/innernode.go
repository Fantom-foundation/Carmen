package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// InnerNode contains keys as the LeafNode, in addition, it contains array of children elements.
type InnerNode[K comparable] struct {
	LeafNode[K] // LeafNode is the base for this node, it contains common properties such as the keys, and common methods

	// TODO replace the node list by iIDs and fetch nodes from the pool
	children []Node[K] // children is the array of IDs of child nodes
}

// newInnerNode creates a new instance with the given capacity.
func newInnerNode[K comparable](capacity int, comparator common.Comparator[K]) *InnerNode[K] {
	return &InnerNode[K]{
		children: make([]Node[K], 0, capacity+2),
		LeafNode: *newLeafNode[K](capacity, comparator)}
}

// initNewInnerNode creates a new inner node.
// It assigns left and right child, and a value that falls between these children.
// The left child can be set to nil, in which case it is not added
func initNewInnerNode[K comparable](left, right Node[K], middle K, capacity int, comparator common.Comparator[K]) *InnerNode[K] {
	innerNode := newInnerNode[K](capacity, comparator)
	innerNode.children = append(innerNode.children, left)
	innerNode.keys = append(innerNode.keys, middle)
	if right != nil {
		innerNode.children = append(innerNode.children, right)
	}
	return innerNode
}

func (m *InnerNode[K]) Insert(key K) (right Node[K], middle K, split bool) {
	index, exists := m.findItem(key)
	if !exists {
		// insert into child, when split has happened, insert result in this node
		if right, middle, split := m.children[index].Insert(key); split {
			m.insertAt(middle, right, index)
		}

		// check and potentially split this node
		if len(m.keys) == m.capacity+1 {
			right, middle = m.split()
			split = true
		}
	}

	return
}

func (m *InnerNode[K]) Contains(key K) bool {
	index, exists := m.findItem(key)

	if exists {
		return true
	}

	return m.children[index].Contains(key)
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

// insertAt extends the leaf of one item and inserts the input key
// at the input position. The keys beyond this index
// are shifted right.
func (m *InnerNode[K]) insertAt(key K, right Node[K], index int) {
	if index == len(m.keys) {
		m.keys = append(m.keys, key)
		m.children = append(m.children, right)
	} else {
		// shift keys + children
		for i := len(m.keys) - 1; i >= index; i-- {
			if i+1 == len(m.keys) {
				m.keys = append(m.keys, m.keys[i])
			} else {
				m.keys[i+1] = m.keys[i]
			}
		}

		m.keys[index] = key

		for i := len(m.children) - 1; i > index; i-- {
			if i+1 == len(m.children) {
				m.children = append(m.children, m.children[i])
			} else {
				m.children[i+1] = m.children[i]
			}
		}

		m.children[index+1] = right
	}

	return
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

func (m InnerNode[K]) String() string {
	return fmt.Sprintf("%v", m.children)
}

// printDump collects debug print of this tree
func (m InnerNode[K]) printDump(lines []string, level int) []string {
	if len(lines) >= level {
		var str string
		lines = append(lines, str)
	}

	for _, child := range m.children {
		switch v := child.(type) {
		case *LeafNode[K]:
			lines[level] = lines[level] + fmt.Sprintf("%v-", v)
		case *InnerNode[K]:
			lines = v.printDump(lines, level+1)
			lines[level] = lines[level] + fmt.Sprintf("%v-", v.keys)
		default:
		}
	}

	return lines
}
