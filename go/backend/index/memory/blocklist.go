// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// BlockList is a data structure that maintains a list of blocks. Each block maps a fixed number of Key/Value pairs.
// When the size overflows, a new block is created and linked in this list. Further keys are stored in this block.
type BlockList[K comparable, V any] struct {
	list []*common.SortedMap[K, V]

	comparator    common.Comparator[K]
	blockCapacity int
	size          int // current size computed during addition for fast read
}

// NewBlockList creates a new instance, each block will have the given maximal capacity.
// blockCapacity is maximal size of each block in this list
func NewBlockList[K comparable, V any](blockCapacity int, comparator common.Comparator[K]) *BlockList[K, V] {
	return &BlockList[K, V]{
		blockCapacity: blockCapacity,
		comparator:    comparator,
		list:          make([]*common.SortedMap[K, V], 0, 10),
	}
}

// InitBlockList creates a new instance, each block will have the given maximal capacity.
// blockCapacity is maximal size of each block in this list.
// This block list will be initialised with input data
func InitBlockList[K comparable, V any](blockCapacity int, data []common.MapEntry[K, V], comparator common.Comparator[K]) *BlockList[K, V] {
	list := make([]*common.SortedMap[K, V], 0, len(data))
	for i := 0; i < len(data); i += blockCapacity {
		end := i + blockCapacity
		if end > len(data) {
			end = len(data)
		}
		newBlock := common.InitSortedMap[K, V](blockCapacity, data[i:end], comparator)
		list = append(list, newBlock)
	}

	b := &BlockList[K, V]{
		blockCapacity: blockCapacity,
		comparator:    comparator,
		list:          list,
	}

	b.size = len(data)
	return b
}

// ForEach all entries - calls the callback for each key-value pair in the table.
func (m *BlockList[K, V]) ForEach(callback func(K, V)) {
	for _, item := range m.list {
		item.ForEach(callback)
	}
}

// GetEntries collects data from all blocks and returns them as one slice.
func (m *BlockList[K, V]) GetEntries() []common.MapEntry[K, V] {
	data := make([]common.MapEntry[K, V], 0, m.size)
	for _, item := range m.list {
		data = append(data, item.GetEntries()...)
	}
	return data
}

// Get returns a value from the table or false.
func (m *BlockList[K, V]) Get(key K) (val V, exists bool) {
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
func (m *BlockList[K, V]) Put(key K, val V) {
	_, item, exists := m.findBlock(key)
	if !exists {
		m.size += 1
	}
	// always replace existing value
	item.Put(key, val)
}

func (m *BlockList[K, V]) GetOrAdd(key K, val V) (value V, exists bool) {
	existsVal, page, exists := m.findBlock(key)

	if exists {
		return existsVal, true
	}

	m.size += 1
	page.Put(key, val)

	return val, false
}

// findBlock iterates blocks and finds the block to insert the key into.
// It returns the last block if the key is not in any block yet
// and can even create a new block if there is no space for the new key
// in existing blocks. True is returned if the key was found in one of the blocks
// and the value for this key is returned as well.
// If false is returned, the returned value should be ignored and the output block
// may be used to associate the key.
func (m *BlockList[K, V]) findBlock(key K) (val V, block *common.SortedMap[K, V], exists bool) {
	if len(m.list) == 0 {
		newBlock := common.NewSortedMap[K, V](m.blockCapacity, m.comparator)
		m.list = append(m.list, newBlock)
		return val, newBlock, false
	}

	for _, item := range m.list {
		if existingVal, update := item.Get(key); update {
			return existingVal, item, true
		}
	}

	tail := m.list[len(m.list)-1]
	// add a new block when overflow
	if tail.Size() == m.blockCapacity {
		tail = common.NewSortedMap[K, V](m.blockCapacity, m.comparator)
		m.list = append(m.list, tail)
	}

	return val, tail, false
}

// Remove deletes the key from the map.
func (m *BlockList[K, V]) Remove(key K) (exists bool) {
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
func (m *BlockList[K, V]) fillFromTail(item *common.SortedMap[K, V]) {
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

// pickTailEntry picks a random (first) value from tail.
func (m *BlockList[K, V]) pickTailEntry(tail *common.SortedMap[K, V]) (key K, val V, exists bool) {
	if tail.Size() > 0 {
		entry := tail.GetEntries()[tail.Size()-1]
		key = entry.Key
		val = entry.Val
		exists = true
	}

	return
}

func (m *BlockList[K, V]) Size() int {
	return m.size
}

func (m *BlockList[K, V]) Clear() {
	m.size = 0
	for i := range m.list {
		m.list[i] = nil
	}
	m.list = m.list[0:0]
}

func (m *BlockList[K, V]) PrintDump() {
	for i, item := range m.list {
		fmt.Printf("Block: %d, size: %d \n", i, item.Size())
		item.ForEach(func(k K, v V) {
			fmt.Printf("  %2v -> %3v \n", k, v)
		})
	}
}

func (m *BlockList[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	var entrySize uintptr

	// items before tail have all the same sizes
	size := len(m.list)
	if size > 1 {
		entrySize += uintptr(size-1) * unsafe.Sizeof(&common.SortedMap[K, V]{})
		entrySize += uintptr(size-1) * m.list[0].GetMemoryFootprint().Value()
	}
	// add size of tail
	if size > 0 {
		tail := m.list[size-1]
		entrySize += unsafe.Sizeof(&common.SortedMap[K, V]{})
		entrySize += tail.GetMemoryFootprint().Value()
	}

	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("blocks", common.NewMemoryFootprint(entrySize))
	return footprint
}
