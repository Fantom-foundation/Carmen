package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// KVStore is a database-based store.Store implementation. It stores items in a key-value databse.
type KVStore[I common.Identifier, V any] struct {
	db              *leveldb.DB
	hashTree        hashtree.HashTree
	valueSerializer common.Serializer[V]
	indexSerializer common.Serializer[I]
	pageSize        int // the amount of items stored in one database page
	itemSize        int // the amount of bytes per one value
	table           common.TableSpace
	itemDefault     V
}

// NewStore constructs a new instance of the KVStore.
func NewStore[I common.Identifier, V any](
	db *leveldb.DB,
	table common.TableSpace,
	serializer common.Serializer[V],
	indexSerializer common.Serializer[I],
	hashTreeFactory hashtree.Factory,
	itemDefault V,
	pageSize int) (store *KVStore[I, V], err error) {

	store = &KVStore[I, V]{
		db:              db,
		valueSerializer: serializer,
		indexSerializer: indexSerializer,
		pageSize:        pageSize,
		itemSize:        serializer.Size(),
		table:           table,
		itemDefault:     itemDefault,
	}
	store.hashTree = hashTreeFactory.Create(store)
	return
}

// itemPosition provides the position of an item in the page as well as the page number
func (m *KVStore[I, V]) itemPosition(id I) (page int, position int) {
	return int(id) / m.pageSize, (int(id) % m.pageSize) * m.valueSerializer.Size()
}

func (m *KVStore[I, V]) GetPage(page int) (pageData []byte, err error) {
	pageStartKey := page * m.pageSize
	pageEndKey := pageStartKey + m.pageSize
	r := util.Range{Start: m.convertKey(I(pageStartKey)), Limit: m.convertKey(I(pageEndKey))}
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

func (m *KVStore[I, V]) Set(id I, value V) (err error) {
	// index is mapped in the database directly
	if err = m.db.Put(m.convertKey(id), m.valueSerializer.ToBytes(value), nil); err == nil {
		page, _ := m.itemPosition(id)
		m.hashTree.MarkUpdated(page)
	}
	return
}

func (m *KVStore[I, V]) Get(id I) (v V, err error) {
	// index is mapped in the database directly
	var val []byte
	if val, err = m.db.Get(m.convertKey(id), nil); err != nil {
		if err == leveldb.ErrNotFound {
			return m.itemDefault, nil
		}
	} else {
		v = m.valueSerializer.FromBytes(val)
	}
	return
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *KVStore[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

func (m *KVStore[I, V]) Close() error {
	// commit state hash root before closing
	_, err := m.GetStateHash()
	return err
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *KVStore[I, V]) convertKey(idx I) []byte {
	return m.table.AppendKey(m.indexSerializer.ToBytes(idx))
}
