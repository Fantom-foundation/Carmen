package memory

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const LengthSize = 4 // uint32

// Depot is an in-memory store.Depot implementation - it maps IDs to values
type Depot[I common.Identifier] struct {
	data      [][]byte // data of pages [item][byte of item]
	hashTree  hashtree.HashTree
	hashItems int // the amount of items in one hashing group
}

// NewDepot constructs a new instance of Depot.
// It needs a serializer of data items and the default value for a not-set item.
func NewDepot[I common.Identifier](hashItems int, hashtreeFactory hashtree.Factory) (*Depot[I], error) {
	if hashItems <= 0 || hashtreeFactory == nil {
		return nil, fmt.Errorf("depot parameters invalid")
	}

	m := &Depot[I]{
		data:      [][]byte{},
		hashItems: hashItems,
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemHashGroup provides the hash group into which belongs the item
func (m *Depot[I]) itemHashGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// GetPage provides all data of one hashing group in a byte slice
func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	start := m.hashItems * hashGroup
	end := start + m.hashItems
	if end > len(m.data) {
		end = len(m.data)
	}
	outLen := m.hashItems * LengthSize
	for i := start; i < end; i++ {
		outLen += len(m.data[i])
	}
	out = make([]byte, outLen)
	outIt := 0
	for i := start; i < start+m.hashItems; i++ {
		if i < end {
			binary.LittleEndian.PutUint32(out[outIt:], uint32(len(m.data[i])))
		}
		outIt += LengthSize
	}
	for i := start; i < end; i++ {
		copy(out[outIt:], m.data[i])
		outIt += len(m.data[i])
	}
	return
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	for int(id) >= len(m.data) {
		m.data = append(m.data, nil)
	}
	m.data[id] = value
	m.hashTree.MarkUpdated(m.itemHashGroup(id))
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	if int(id) < len(m.data) {
		out = m.data[id]
	}
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (int, error) {
	value, err := m.Get(id)
	return len(value), err
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Flush the depot
func (m *Depot[I]) Flush() error {
	return nil // no-op for in-memory database
}

// Close the depot
func (m *Depot[I]) Close() error {
	return nil // no-op for in-memory database
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	size := unsafe.Sizeof(*m)
	for _, d := range m.data {
		size += uintptr(len(d))
	}
	mf := common.NewMemoryFootprint(size)
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	return mf
}
