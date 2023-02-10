package btree

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
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
	if index, exists := m.findItem(&key); !exists {
		m.insertAt(index, key, nil, nil)

		// split when overflow
		if len(m.keys) == m.capacity+1 {
			right, middle = m.split()
			split = true
		}
	}

	return
}

func (m *LeafNode[K]) contains(key K) bool {
	_, exists := m.findItem(&key)
	return exists
}

func (m *LeafNode[K]) remove(key K) node[K] {
	if index, exists := m.findItem(&key); exists {
		m.removeAt(index)
	}

	return m
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

func (m *LeafNode[K]) removeAt(index int) (K, node[K]) {
	removed := m.keys[index]
	for i := index; i < len(m.keys)-1; i++ {
		m.keys[i] = m.keys[i+1]
	}
	m.keys = m.keys[0 : len(m.keys)-1]

	return removed, nil
}

func (m *LeafNode[K]) append(k K, sibling node[K]) {
	m.keys = append(m.keys, k)
	m.keys = append(m.keys, sibling.getKeys()...)
}

func (m *LeafNode[K]) getAt(index int) (K, node[K]) {
	if index > 0 && index == len(m.keys) {
		return m.keys[index-1], nil
	}
	return m.keys[index], nil
}

func (m *LeafNode[K]) insertAt(index int, key K, left, right node[K]) {
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

func (m *LeafNode[K]) findItem(key *K) (index int, exists bool) {
	end := len(m.keys) - 1
	var res, start, mid int
	for start <= end {
		mid = (start + end) / 2
		res = m.comparator.Compare(&m.keys[mid], key)
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

func (m *LeafNode[K]) size() int {
	return len(m.keys)
}

func (m *LeafNode[K]) getKeys() []K {
	return m.keys
}

// ForEach iterates ordered keys
func (m *LeafNode[K]) ForEach(callback func(k K)) {
	for _, key := range m.keys {
		callback(key)
	}
}

func (m LeafNode[K]) String() string {
	var str string
	for i, key := range m.keys {
		str += fmt.Sprintf("%v", key)
		if i < len(m.keys)-1 {
			str += ", "
		}
	}

	return fmt.Sprintf("[%v]", str)
}

func (m LeafNode[K]) checkProperties(treeDepth *int, currentLevel int) error {
	// check depth
	if *treeDepth == -1 {
		*treeDepth = currentLevel
	} else {
		if currentLevel != *treeDepth {
			return fmt.Errorf("leaf has wrong depth: %d != %d", currentLevel, *treeDepth)
		}
	}

	// check order
	for i := 0; i < m.size()-1; i++ {
		if m.comparator.Compare(&m.keys[i], &m.keys[i+1]) >= 0 {
			return fmt.Errorf("keys not ordered: %v >= %v", m.keys[i], m.keys[i+1])
		}
	}

	// check capacity (for non-root leaf)
	if currentLevel > 0 && m.size() < m.capacity/2 {
		return fmt.Errorf("size below minimal capacity: %d < %d", m.size(), m.capacity/2)
	}

	return nil
}

func (m *LeafNode[K]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	var k K
	keysSize := uintptr(len(m.keys)) * unsafe.Sizeof(k)
	return common.NewMemoryFootprint(selfSize + keysSize)
}
