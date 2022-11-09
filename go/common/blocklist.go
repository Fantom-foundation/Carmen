package common

import (
	"fmt"
	"unsafe"
)

// BlockList is a data structure that maintains a list of blocks. Each block maps a fixed number of Key/Value pairs.
// When the size overflows, a new block is created and linked in this list. Further keys are stored in this block.
type BlockList[K comparable, V any] struct {
	list []*SortedMap[K, V]

	comparator    Comparator[K]
	blockCapacity int
	size          int // current size computed during addition for fast read
}

// NewBlockList creates a new instance, each block will have the given maximal capacity.
// blockCapacity is maximal size of each block in this list
func NewBlockList[K comparable, V any](blockCapacity int, comparator Comparator[K]) *BlockList[K, V] {
	return &BlockList[K, V]{
		blockCapacity: blockCapacity,
		comparator:    comparator,
		list:          make([]*SortedMap[K, V], 0, 10),
	}
}

// ForEach all entries - calls the callback for each key-value pair in the table
func (m *BlockList[K, V]) ForEach(callback func(K, V)) error {
	for _, item := range m.list {
		item.ForEach(callback)
	}
	return nil
}

// BulkInsert creates content of this list from the input data.
func (m *BlockList[K, V]) BulkInsert(data []MapEntry[K, V]) error {
	var start int
	// fill-in possible half empty last element
	if len(m.list) > 0 {
		tail := m.list[len(m.list)-1]
		start = m.blockCapacity - tail.Size()
		if start > 0 {
			tail.BulkInsert(data[0:start])
		}
		m.size += start
	}

	// segment and bulk insert rest of the data
	for i := start; i < len(data); i += m.blockCapacity {
		newBlock := NewSortedMap[K, V](m.blockCapacity, m.comparator)
		end := i + m.blockCapacity
		if end > len(data) {
			end = len(data)
		}
		newBlock.BulkInsert(data[i:end])
		m.list = append(m.list, newBlock)
		m.size += end - i
	}

	return nil
}

// GetAll collects data from all blocks and returns them as one slice
func (m *BlockList[K, V]) GetAll() ([]MapEntry[K, V], error) {
	data := make([]MapEntry[K, V], 0, m.size)
	for _, item := range m.list {
		data = append(data, item.GetAll()...)
	}

	return data, nil
}

// Get returns a value from the table or false.
func (m *BlockList[K, V]) Get(key K) (val V, exists bool, err error) {
	for _, item := range m.list {
		val, exists = item.Get(key)
		if exists {
			break
		}
	}
	return
}

// Put associates a key to a value.
// If the key is already present, the value is updated.
func (m *BlockList[K, V]) Put(key K, val V) error {
	item := m.findBlock(key)
	item.Put(key, val) // associate a new value to tail
	return nil
}

// findBlock iterates blocks and finds the block to insert the key into.
// It returns the last block if the key is not in any block yet
// and can even create a new block if there is no space for the new key
// in existing blocks.
// This method increases the size of items if the key is not present
// as it is expected that the key will be added following the call if this method.
func (m *BlockList[K, V]) findBlock(key K) *SortedMap[K, V] {
	if len(m.list) == 0 {
		newBlock := NewSortedMap[K, V](m.blockCapacity, m.comparator)
		m.size += 1
		m.list = append(m.list, newBlock)
		return newBlock
	}

	for _, item := range m.list {
		// replace value if it already exists.
		if _, update := item.Get(key); update {
			return item
		}
	}

	tail := m.list[len(m.list)-1]
	// add a new block when overflow
	if tail.Size() == m.blockCapacity {
		tail = NewSortedMap[K, V](m.blockCapacity, m.comparator)
		m.list = append(m.list, tail)
	}

	m.size += 1

	return tail
}

// Remove deletes the key from the map
func (m *BlockList[K, V]) Remove(key K) (exists bool, err error) {
	for _, item := range m.list {
		// replace value if it already exists.
		if exists = item.Remove(key); exists {
			m.size -= 1
			m.fillFromTail(item)
			break
		}
	}

	return
}

// fillFromTail picks a random item from the tail of this list and inserts it into the input item.
// It is meant to fill a place in the block caused by deletion of an item.
// If the input item is the tail, no element is removed, but the tail may be deleted if it is empty.
// If the tail becomes empty, it is removed from the list.
func (m *BlockList[K, V]) fillFromTail(item *SortedMap[K, V]) {
	tail := m.list[len(m.list)-1]

	if tail != item {
		if k, v, exists := m.pickTailEntry(tail); exists {
			item.Put(k, v)
			tail.Remove(k)
		}
	}

	// remove tail if empty
	if tail.Size() == 0 {
		m.list[len(m.list)-1] = nil // allow for GC
		m.list = m.list[0 : len(m.list)-1]
	}

	return
}

// pickTailEntry picks a random (first) value from tail
func (m *BlockList[K, V]) pickTailEntry(tail *SortedMap[K, V]) (key K, val V, exists bool) {
	if tail.Size() > 0 {
		entry := tail.GetAll()[tail.Size()-1]
		key = entry.Key
		val = entry.Val
		exists = true
	}

	return
}

func (m *BlockList[K, V]) Size() int {
	return m.size
}

func (m *BlockList[K, V]) Clear() error {
	m.size = 0
	for i := range m.list {
		m.list[i] = nil
	}
	m.list = m.list[0:0]
	return nil
}

func (m *BlockList[K, V]) PrintDump() {
	for i, item := range m.list {
		fmt.Printf("Block: %d, size: %d \n", i, item.Size())
		item.ForEach(func(k K, v V) {
			fmt.Printf("  %2v -> %3v \n", k, v)
		})
	}
}

func (m *BlockList[K, V]) GetMemoryFootprint() *MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	var entrySize uintptr

	// items before tail have all the same sizes
	size := len(m.list)
	if size > 1 {
		entrySize += uintptr(size-1) * unsafe.Sizeof(&SortedMap[K, V]{})
		entrySize += uintptr(size-1) * m.list[0].GetMemoryFootprint().Value()
	}
	// add size of tail
	if size > 0 {
		tail := m.list[size-1]
		entrySize += unsafe.Sizeof(&SortedMap[K, V]{})
		entrySize += tail.GetMemoryFootprint().Value()
	}

	footprint := NewMemoryFootprint(selfSize)
	footprint.AddChild("blocks", NewMemoryFootprint(entrySize))
	return footprint
}
