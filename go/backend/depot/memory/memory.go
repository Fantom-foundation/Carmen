package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

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

// itemPage provides the hash group of an item
func (m *Depot[I]) itemPage(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// hashGroupRange provides range of data indexes of given hashing group
func (m *Depot[I]) hashGroupRange(page int) (start int, end int) {
	return m.hashItems * page, (m.hashItems * page) + m.hashItems
}

func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	start, end := m.hashGroupRange(hashGroup)
	if end > len(m.data) {
		end = len(m.data)
	}
	groupLen := 0
	for i := start; i < end; i++ {
		groupLen += len(m.data[i])
	}
	out = make([]byte, groupLen)
	outIt := 0
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
	m.hashTree.MarkUpdated(m.itemPage(id))
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	if int(id) < len(m.data) {
		out = m.data[id]
	}
	return
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Close the store
func (m *Depot[I]) Close() error {
	return nil // no-op for in-memory database
}
