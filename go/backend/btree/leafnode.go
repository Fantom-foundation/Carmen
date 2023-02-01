package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// LeafNode contains a list of keys. The node has a maximal capacity, i.e. the number of keys it can hold.
type LeafNode[K any] struct {
	keys []K // keys is the array of elements stored in this node

	capacity   int
	comparator common.Comparator[K]
}

// newLeafNode creates a new instance with the given capacity.
func newLeafNode[K any](capacity int, comparator common.Comparator[K]) *LeafNode[K] {
	return &LeafNode[K]{
		keys:       make([]K, 0, capacity+1),
		capacity:   capacity,
		comparator: comparator}
}

func (m *LeafNode[K]) insert(key K) (right node[K], middle K, split bool) {
	index, exists := m.findItem(key)
	if !exists {
		m.insertAt(key, index)

		// split when overflow
		if len(m.keys) == m.capacity+1 {
			right, middle = m.split()
			split = true
		}
	}

	return
}

func (m *LeafNode[K]) contains(key K) bool {
	_, exists := m.findItem(key)
	return exists
}

func (m *LeafNode[K]) hasNext(iterator *Iterator[K]) bool {
	return iterator.currentLevel().currentIndex < iterator.currentLevel().endIndex
}

func (m *LeafNode[K]) next(iterator *Iterator[K]) (k K) {
	// no next item
	if !m.hasNext(iterator) {
		return
	}

	level := iterator.currentLevel()
	k = m.keys[level.currentIndex]
	level.currentIndex += 1 // move to next position
	return k
}

// split this node into two. Keys in this node are reduced to half
// and the other half is put in the output node.
func (m *LeafNode[K]) split() (right *LeafNode[K], middle K) {
	right = newLeafNode[K](m.capacity, m.comparator)
	midIndex := len(m.keys) / 2
	right.keys = append(right.keys, m.keys[midIndex+1:len(m.keys)]...)
	middle = m.keys[midIndex]
	m.keys = m.keys[0:midIndex]

	return
}

// insertAt extends the leaf of one item and inserts the input key
// at the input position. The keys beyond this index
// are shifted right.
func (m *LeafNode[K]) insertAt(key K, index int) {
	if index == len(m.keys) {
		m.keys = append(m.keys, key) // does not matter that we add the input key, it will get replaced
	} else {
		// shift
		for i := len(m.keys) - 1; i >= index; i-- {
			if i+1 == len(m.keys) {
				m.keys = append(m.keys, m.keys[i])
			} else {
				m.keys[i+1] = m.keys[i]
			}
		}
	}

	m.keys[index] = key

	return
}

// findItem finds a key in the list, if it exists.
// It returns the index of the key that was found, and it returns true.
// If the key does not exist, it returns false and the index is equal to the last
// visited position in the list, traversed using binary search.
// The index is increased by one when the last visited key was lower than the input key
// so the new key may be inserted after this key.
// It means the index can be used as a position to insert the key in the list.
func (m *LeafNode[K]) findItem(key K) (index int, exists bool) {
	end := len(m.keys) - 1
	var res, start, mid int
	for start <= end {
		mid = (start + end) / 2
		res = m.comparator.Compare(&m.keys[mid], &key)
		if res == 0 {
			return mid, true
		} else if res < 0 {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	if res < 0 {
		mid += 1
	}
	return mid, false
}

// ForEach iterates ordered keys
func (m *LeafNode[K]) ForEach(callback func(k K)) {
	for _, key := range m.keys {
		callback(key)
	}
}

func (m LeafNode[K]) String() string {
	return fmt.Sprintf("%v", m.keys)
}
