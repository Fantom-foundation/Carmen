package ldb

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Depot is an in-memory store.Depot implementation - it maps IDs to values
type Depot[I common.Identifier] struct {
	db              *leveldb.DB
	table           common.TableSpace
	hashTree        hashtree.HashTree
	indexSerializer common.Serializer[I]
	hashItems       int // the amount of items in one hashing group
}

// NewDepot constructs a new instance of Depot.
// It needs a serializer of data items and the default value for a not-set item.
func NewDepot[I common.Identifier](db *leveldb.DB,
	table common.TableSpace,
	indexSerializer common.Serializer[I],
	hashtreeFactory hashtree.Factory,
	hashItems int,
) (*Depot[I], error) {
	if hashItems <= 0 || hashtreeFactory == nil || indexSerializer == nil {
		return nil, fmt.Errorf("depot parameters invalid")
	}

	m := &Depot[I]{
		db:              db,
		table:           table,
		indexSerializer: indexSerializer,
		hashItems:       hashItems,
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemHashGroup provides the hash group into which belongs the item
func (m *Depot[I]) itemHashGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// hashGroupRange provides range of data indexes of given hashing group
func (m *Depot[I]) hashGroupRange(page int) (start int, end int) {
	return m.hashItems * page, (m.hashItems * page) + m.hashItems
}

func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	start, end := m.hashGroupRange(hashGroup)
	r := util.Range{Start: m.convertKey(I(start)), Limit: m.convertKey(I(end))}
	iter := m.db.NewIterator(&r, nil)
	defer iter.Release()

	for iter.Next() {
		out = append(out, iter.Value()...)
	}

	err = iter.Error()

	return
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	err := m.db.Put(m.convertKey(id), value, nil)
	if err != nil {
		return err
	}
	m.hashTree.MarkUpdated(m.itemHashGroup(id))
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	out, err = m.db.Get(m.convertKey(id), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Depot[I]) convertKey(idx I) []byte {
	return m.table.ToDBKey(m.indexSerializer.ToBytes(idx)).ToBytes()
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Close the store
func (m *Depot[I]) Close() error {
	return nil // no-op for in-memory database
}
