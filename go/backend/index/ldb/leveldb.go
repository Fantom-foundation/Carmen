package ldb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	HashKey      = "hash"
	LastIndexKey = "last"
)

// KVIndex represents a key-value store for holding the index data.
type KVIndex[K comparable] struct {
	db             *leveldb.DB
	table          common.TableSpace
	serializer     common.Serializer[K]
	hashIndex      *index.HashIndex[K]
	lastIndex      uint32
	hashSerializer common.HashSerializer
}

// NewKVIndex creates a new instance of the index backed by a persisted database
func NewKVIndex[K comparable](db *leveldb.DB, table common.TableSpace, serializer common.Serializer[K]) (p *KVIndex[K], err error) {
	// read the last hash from the database
	var hash []byte
	if hash, err = db.Get(table.AppendKeyStr(HashKey), nil); err != nil {
		if err == errors.ErrNotFound {
			hash = []byte{}
		} else {
			return
		}
	}

	// read the last index from the database
	var last []byte
	if last, err = db.Get(table.AppendKeyStr(LastIndexKey), nil); err != nil {
		if err == errors.ErrNotFound {
			last = make([]byte, 4)
		} else {
			return
		}
	}

	hashSerializer := common.HashSerializer{}
	p = &KVIndex[K]{
		db:             db,
		table:          table,
		serializer:     serializer,
		hashIndex:      index.InitHashIndex[K](hashSerializer.FromBytes(hash), serializer),
		lastIndex:      binary.LittleEndian.Uint32(last),
		hashSerializer: hashSerializer,
	}

	return p, nil
}

func (m *KVIndex[K]) GetOrAdd(key K) (idx uint32, err error) {
	var val []byte
	if val, err = m.db.Get(m.appendKey(key), nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := make([]byte, 4)
			binary.LittleEndian.PutUint32(idxArr, m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := make([]byte, 4)
			binary.LittleEndian.PutUint32(nextIdxArr, m.lastIndex)

			batch := new(leveldb.Batch)
			batch.Put(m.appendKeyStr(LastIndexKey), nextIdxArr)
			batch.Put(m.appendKey(key), idxArr)
			if err = m.db.Write(batch, nil); err != nil {
				return
			}
			m.hashIndex.AddKey(key)
		} else {
			return
		}
	} else {
		idx = binary.LittleEndian.Uint32(val)
	}

	return idx, nil
}

func (m *KVIndex[K]) Contains(key K) bool {
	exists, _ := m.db.Has(m.appendKey(key), nil)
	return exists
}

func (m *KVIndex[K]) GetStateHash() (hash common.Hash, err error) {
	// compute and persist new hash
	if hash, err = m.hashIndex.Commit(); err != nil {
		return
	}

	if err = m.db.Put(m.appendKeyStr(HashKey), m.hashSerializer.ToBytes(hash), nil); err != nil {
		return
	}

	return
}

func (m *KVIndex[K]) Close() (err error) {
	// commit the state
	_, err = m.GetStateHash()
	return
}

func (m *KVIndex[K]) appendKey(key K) []byte {
	return m.table.AppendKey(m.serializer.ToBytes(key))
}

func (m *KVIndex[K]) appendKeyStr(key string) []byte {
	return m.table.AppendKeyStr(key)
}
