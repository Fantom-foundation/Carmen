package kvdb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// KVStore is a database-based store.Store implementation. It stores items in a key-value databse.
type KVStore[V any] struct {
	db          *leveldb.DB
	hashTree    store.HashTree
	serializer  common.Serializer[V]
	pageSize    uint32 // the amount of items stored in one database page
	itemSize    int    // the amount of bytes per one value
	itemDefault V
}

// NewStore constructs a new instance of the KVStore.
func NewStore[V any](path string, serializer common.Serializer[V], hashTree store.HashTree, itemDefault V, pageSize uint32) (store *KVStore[V], err error) {
	var db *leveldb.DB
	if db, err = leveldb.OpenFile(path, nil); err == nil {
		store = &KVStore[V]{
			db:          db,
			hashTree:    hashTree,
			serializer:  serializer,
			pageSize:    pageSize,
			itemSize:    serializer.Size(),
			itemDefault: itemDefault,
		}
	}
	return
}

// itemPosition provides the position of an item in the page as well as the page number
func (m *KVStore[V]) itemPosition(id uint32) uint32 {
	return id % m.pageSize * uint32(m.itemSize)
}

func (m *KVStore[V]) GetPage(page int) (pageData []byte, err error) {
	pageStartKey := uint32(page)
	//pageEndKey := pageStartKey + m.pageSize	// TODO check if needed for the limit
	r := util.Range{Start: toBytes(pageStartKey), Limit: toBytes(m.pageSize)}
	iter := m.db.NewIterator(&r, nil)
	defer iter.Release()

	// create the page first
	pageData = make([]byte, m.pageSize*uint32(m.itemSize))
	// inject values at the right positions
	for iter.Next() {
		key := fromBytes(iter.Key())
		position := m.itemPosition(key)
		copy(pageData[position:], iter.Value())
	}

	err = iter.Error()

	return
}

func (m *KVStore[V]) Set(id uint32, value V) error {
	// index is mapped in the database directly
	return m.db.Put(toBytes(id), m.serializer.ToBytes(value), nil)
}

func (m *KVStore[V]) Get(id uint32) (v V, err error) {
	// index is mapped in the database directly
	var val []byte
	if val, err = m.db.Get(toBytes(id), nil); err != nil {
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
	return m.hashTree.HashRoot(m)
}

func (m *KVStore[V]) Close() error {
	return m.db.Close()
}

// TODO move this to serializer and use elsewhere (when other PRs are merged)
func toBytes(idx uint32) (b []byte) {
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, idx)
	return
}

func fromBytes(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
