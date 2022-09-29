package kvdb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// KVStore is a database-based store.Store implementation. It stores items in a key-value databse.
type KVStore[V any] struct {
	db          *leveldb.DB
	hashTree    hashtree.HashTree
	serializer  common.Serializer[V]
	pageSize    uint32 // the amount of items stored in one database page
	itemSize    int    // the amount of bytes per one value
	table       []byte
	itemDefault V
}

// NewStore constructs a new instance of the KVStore.
func NewStore[V any](db *leveldb.DB, table []byte, serializer common.Serializer[V], hashTreeFactory hashtree.HashTreeFactory, itemDefault V, pageSize uint32) (store *KVStore[V], err error) {
	store = &KVStore[V]{
		db:          db,
		serializer:  serializer,
		pageSize:    pageSize,
		itemSize:    serializer.Size(),
		table:       table,
		itemDefault: itemDefault,
	}
	store.hashTree = hashTreeFactory.Create(store)
	return
}

// itemPosition provides the position of an item in the page as well as the page number
func (m *KVStore[V]) itemPosition(id uint32) (page int, position int) {
	return int(id / m.pageSize), int(id%m.pageSize) * m.serializer.Size()
}

func (m *KVStore[V]) GetPage(page int) (pageData []byte, err error) {
	pageStartKey := uint32(page) * m.pageSize
	pageEndKey := pageStartKey + m.pageSize
	r := util.Range{Start: m.appendKey(pageStartKey), Limit: m.appendKey(pageEndKey)}
	iter := m.db.NewIterator(&r, nil)
	defer iter.Release()

	// create the page first
	pageData = make([]byte, m.pageSize*uint32(m.itemSize))
	// inject values at the right positions
	for iter.Next() {
		key := fromBytes(iter.Key()[len(m.table):]) // strip first byte (table space) and get the idx only
		_, position := m.itemPosition(key)
		copy(pageData[position:], iter.Value())
	}

	err = iter.Error()

	return
}

func (m *KVStore[V]) Set(id uint32, value V) (err error) {
	// index is mapped in the database directly
	if err = m.db.Put(m.appendKey(id), m.serializer.ToBytes(value), nil); err == nil {
		page, _ := m.itemPosition(id)
		m.hashTree.MarkUpdated(page)
	}
	return
}

func (m *KVStore[V]) Get(id uint32) (v V, err error) {
	// index is mapped in the database directly
	var val []byte
	if val, err = m.db.Get(m.appendKey(id), nil); err != nil {
		if err == leveldb.ErrNotFound {
			return m.itemDefault, nil
		}
	} else {
		v = m.serializer.FromBytes(val)
	}
	return
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *KVStore[V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

func (m *KVStore[V]) Close() error {
	// commit state hash root before closing
	_, err := m.GetStateHash()
	return err
}

// TODO move this to serializer and use elsewhere (when other PRs are merged)
func toBytes(idx uint32) (b []byte) {
	b = make([]byte, 4)
	binary.BigEndian.PutUint32(b, idx)
	return
}

func fromBytes(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func (m *KVStore[V]) appendKey(key uint32) []byte {
	return append(m.table, toBytes(key)...)
}

func (m *KVStore[V]) appendKeyStr(key string) []byte {
	return append(m.table, key...)
}
