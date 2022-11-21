package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"unsafe"
)

// Depot is an LevelDB backed store.Depot implementation
type Depot[I common.Identifier] struct {
	db              common.LevelDB
	table           common.TableSpace
	hashTree        hashtree.HashTree
	indexSerializer common.Serializer[I]
	hashItems       int // the amount of items in one hashing group
}

// NewDepot constructs a new instance of Depot.
func NewDepot[I common.Identifier](db common.LevelDB,
	table common.TableSpace,
	indexSerializer common.Serializer[I],
	hashtreeFactory hashtree.Factory,
	hashItems int,
) (*Depot[I], error) {
	m := &Depot[I]{
		db:              db,
		table:           table,
		indexSerializer: indexSerializer,
		hashItems:       hashItems,
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemHashGroup provides the hash group into which the item belongs
func (m *Depot[I]) itemHashGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// hashGroupRange provides range of data indexes of given hashing group
func (m *Depot[I]) hashGroupRange(page int) (start int, end int) {
	return m.hashItems * page, (m.hashItems * page) + m.hashItems
}

func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	startKey, endKey := m.hashGroupRange(hashGroup)
	startDbKey := m.convertKey(I(startKey)).ToBytes()
	endDbKey := m.convertKey(I(endKey)).ToBytes()
	keysRange := util.Range{Start: startDbKey, Limit: endDbKey}
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	for iter.Next() {
		out = append(out, iter.Value()...)
	}

	err = iter.Error()

	return
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	err := m.db.Put(m.convertKey(id).ToBytes(), value, nil)
	if err != nil {
		return err
	}
	m.hashTree.MarkUpdated(m.itemHashGroup(id))
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	out, err = m.db.Get(m.convertKey(id).ToBytes(), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (int, error) {
	value, err := m.Get(id)
	return len(value), err
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Depot[I]) convertKey(idx I) common.DbKey {
	return m.table.ToDBKey(m.indexSerializer.ToBytes(idx))
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Flush the depot
func (m *Depot[I]) Flush() error {
	// commit state hash root
	_, err := m.GetStateHash()
	return err
}

// Close the store
func (m *Depot[I]) Close() error {
	return m.Flush()
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.AddChild("levelDb", m.db.GetMemoryFootprint())
	return mf
}
