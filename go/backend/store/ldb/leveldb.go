package ldb

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Store is a database-based store.Store implementation. It stores items in a key-value database.
type Store[I common.Identifier, V any] struct {
	db              common.LevelDbWithMemoryFootprint
	hashTree        hashtree.HashTree
	valueSerializer common.Serializer[V]
	indexSerializer common.Serializer[I]
	pageSize        int // the amount of items stored in one database page
	itemSize        int // the amount of bytes per one value
	table           common.TableSpace
}

// NewStore constructs a new instance of the Store.
func NewStore[I common.Identifier, V any](
	db common.LevelDbWithMemoryFootprint,
	table common.TableSpace,
	serializer common.Serializer[V],
	indexSerializer common.Serializer[I],
	hashTreeFactory hashtree.Factory,
	pageSize int) (store *Store[I, V], err error) {

	if pageSize < serializer.Size() {
		return nil, fmt.Errorf("leveldb store pageSize too small (minimum %d)", serializer.Size())
	}

	store = &Store[I, V]{
		db:              db,
		valueSerializer: serializer,
		indexSerializer: indexSerializer,
		pageSize:        pageSize / serializer.Size(),
		itemSize:        serializer.Size(),
		table:           table,
	}
	store.hashTree = hashTreeFactory.Create(store)
	return
}

// itemPosition provides the position of an item in the page as well as the page number
func (m *Store[I, V]) itemPosition(id I) (page int, position int) {
	return int(id) / m.pageSize, (int(id) % m.pageSize) * m.valueSerializer.Size()
}

func (m *Store[I, V]) GetPage(page int) (pageData []byte, err error) {
	pageStartKey := page * m.pageSize
	pageEndKey := pageStartKey + m.pageSize
	startDbKey := m.convertKey(I(pageStartKey)).ToBytes()
	endDbKey := m.convertKey(I(pageEndKey)).ToBytes()
	r := util.Range{Start: startDbKey, Limit: endDbKey}
	iter := m.db.NewIterator(&r, nil)
	defer iter.Release()

	// create the page first
	pageData = make([]byte, m.pageSize*m.itemSize)
	// inject values at the right positions
	for iter.Next() {
		key := m.indexSerializer.FromBytes(iter.Key()[1:]) // strip first byte (table space) and get the idx only
		_, position := m.itemPosition(key)
		copy(pageData[position:], iter.Value())
	}

	err = iter.Error()

	return
}

func (m *Store[I, V]) Set(id I, value V) (err error) {
	// index is mapped in the database directly
	dbKey := m.convertKey(id).ToBytes()
	if err = m.db.Put(dbKey, m.valueSerializer.ToBytes(value), nil); err == nil {
		page, _ := m.itemPosition(id)
		m.hashTree.MarkUpdated(page)
	}
	return
}

func (m *Store[I, V]) Get(id I) (v V, err error) {
	// index is mapped in the database directly
	var val []byte
	dbKey := m.convertKey(id).ToBytes()
	if val, err = m.db.Get(dbKey, nil); err != nil {
		if err == leveldb.ErrNotFound {
			return v, nil
		}
	} else {
		v = m.valueSerializer.FromBytes(val)
	}
	return
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

func (m *Store[I, V]) Flush() error {
	// commit state hash root
	_, err := m.GetStateHash()
	return err
}

func (m *Store[I, V]) Close() error {
	return m.Flush()
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Store[I, V]) convertKey(idx I) common.DbKey {
	return m.table.ToDBKey(m.indexSerializer.ToBytes(idx))
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.AddChild("levelDb", m.db.GetMemoryFootprint())
	return mf
}
