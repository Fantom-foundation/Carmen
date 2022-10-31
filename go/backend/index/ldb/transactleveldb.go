package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/hashindex"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

// TransactIndex represents a key-value store for holding the index data.
type TransactIndex[K comparable, I common.Identifier] struct {
	tr              *leveldb.Transaction
	table           common.TableSpace
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *hashindex.HashIndex[K]
	lastIndex       I
	hashSerializer  common.HashSerializer
}

// NewTransactIndex creates a new instance of the index backed by a persisted database
func NewTransactIndex[K comparable, I common.Identifier](
	tr *leveldb.Transaction,
	table common.TableSpace,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I]) (p *TransactIndex[K, I], err error) {

	// read the last hash from the database
	var hash []byte
	if hash, err = tr.Get(table.StrToDBKey(HashKey).ToBytes(), nil); err != nil {
		if err == errors.ErrNotFound {
			hash = []byte{}
		} else {
			return
		}
	}

	// read the last index from the database
	var last []byte
	if last, err = tr.Get(table.StrToDBKey(LastIndexKey).ToBytes(), nil); err != nil {
		if err == errors.ErrNotFound {
			last = make([]byte, 4)
		} else {
			return
		}
	}

	hashSerializer := common.HashSerializer{}
	p = &TransactIndex[K, I]{
		tr:              tr,
		table:           table,
		keySerializer:   keySerializer,
		indexSerializer: indexSerializer,
		hashIndex:       hashindex.InitHashIndex[K](hashSerializer.FromBytes(hash), keySerializer),
		lastIndex:       indexSerializer.FromBytes(last),
		hashSerializer:  hashSerializer,
	}

	// set err to nil as it can contain an ErrNotFound, which we want to suppress
	return p, nil
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *TransactIndex[K, I]) GetOrAdd(key K) (idx I, err error) {
	var val []byte
	if val, err = m.tr.Get(m.convertKey(key).ToBytes(), nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := m.indexSerializer.ToBytes(m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := m.indexSerializer.ToBytes(m.lastIndex)

			batch := new(leveldb.Batch)
			batch.Put(m.convertKeyStr(LastIndexKey).ToBytes(), nextIdxArr)
			batch.Put(m.convertKey(key).ToBytes(), idxArr)
			if err = m.tr.Write(batch, nil); err != nil {
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

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists
func (m *TransactIndex[K, I]) Get(key K) (idx I, err error) {
	var val []byte
	if val, err = m.tr.Get(m.convertKey(key).ToBytes(), nil); err != nil {
		if err == errors.ErrNotFound {
			err = index.ErrNotFound
		}
		return
	}

	idx = m.indexSerializer.FromBytes(val)
	return idx, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *TransactIndex[K, I]) Contains(key K) bool {
	exists, _ := m.tr.Has(m.convertKey(key).ToBytes(), nil)
	return exists
}

// GetStateHash returns the index hash.
func (m *TransactIndex[K, I]) GetStateHash() (hash common.Hash, err error) {
	return m.hashIndex.Commit()
}

func (m *TransactIndex[K, I]) Flush() error {
	// commit and persist the state
	hash, err := m.GetStateHash()
	if err != nil {
		return err
	}
	return m.tr.Put(m.convertKeyStr(HashKey).ToBytes(), m.hashSerializer.ToBytes(hash), nil)
}

func (m *TransactIndex[K, I]) Close() error {
	return m.Flush()
}

// convertKey translates the TransactIndex representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *TransactIndex[K, I]) convertKey(key K) common.DbKey {
	return m.table.ToDBKey(m.keySerializer.ToBytes(key))
}

// convertKeyStr translates the TransactIndex representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// from string
func (m *TransactIndex[K, I]) convertKeyStr(key string) common.DbKey {
	return m.table.StrToDBKey(key)
}
