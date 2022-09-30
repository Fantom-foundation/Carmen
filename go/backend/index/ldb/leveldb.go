package ldb

import (
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
type KVIndex[K comparable, I common.Identifier] struct {
	db              *leveldb.DB
	table           common.TableSpace
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *index.HashIndex[K]
	lastIndex       I
	hashSerializer  common.HashSerializer
}

// NewKVIndex creates a new instance of the index backed by a persisted database
func NewKVIndex[K comparable, I common.Identifier](
	db *leveldb.DB,
	table common.TableSpace,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I]) (p *KVIndex[K, I], err error) {

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
	p = &KVIndex[K, I]{
		db:              db,
		table:           table,
		keySerializer:   keySerializer,
		indexSerializer: indexSerializer,
		hashIndex:       index.InitHashIndex[K](hashSerializer.FromBytes(hash), keySerializer),
		lastIndex:       indexSerializer.FromBytes(last),
		hashSerializer:  hashSerializer,
	}

	return p, nil
}

func (m *KVIndex[K, I]) GetOrAdd(key K) (idx I, err error) {
	var val []byte
	if val, err = m.db.Get(m.appendKey(key), nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := m.indexSerializer.ToBytes(m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := m.indexSerializer.ToBytes(m.lastIndex)

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
		idx = m.indexSerializer.FromBytes(val)
	}

	return idx, nil
}

func (m *KVIndex[K, I]) Contains(key K) bool {
	exists, _ := m.db.Has(m.appendKey(key), nil)
	return exists
}

func (m *KVIndex[K, I]) GetStateHash() (hash common.Hash, err error) {
	// compute and persist new hash
	if hash, err = m.hashIndex.Commit(); err != nil {
		return
	}

	if err = m.db.Put(m.appendKeyStr(HashKey), m.hashSerializer.ToBytes(hash), nil); err != nil {
		return
	}

	return
}

func (m *KVIndex[K, I]) Close() (err error) {
	// commit the state
	_, err = m.GetStateHash()
	return
}

func (m *KVIndex[K, I]) appendKey(key K) []byte {
	return m.table.AppendKey(m.keySerializer.ToBytes(key))
}

func (m *KVIndex[K, I]) appendKeyStr(key string) []byte {
	return m.table.AppendKeyStr(key)
}
