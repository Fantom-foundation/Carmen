package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/hashindex"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	HashKey      = "hash"
	LastIndexKey = "last"
)

// Index represents a key-value store for holding the index data.
type Index[K comparable, I common.Identifier] struct {
	db              *leveldb.DB
	table           common.TableSpace
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *hashindex.HashIndex[K]
	lastIndex       I
	hashSerializer  common.HashSerializer
}

// NewIndex creates a new instance of the index backed by a persisted database
func NewIndex[K comparable, I common.Identifier](
	db *leveldb.DB,
	table common.TableSpace,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I]) (p *Index[K, I], err error) {

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
	p = &Index[K, I]{
		db:              db,
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
func (m *Index[K, I]) GetOrAdd(key K) (idx I, err error) {
	var val []byte
	if val, err = m.db.Get(m.convertKey(key), nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := m.indexSerializer.ToBytes(m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := m.indexSerializer.ToBytes(m.lastIndex)

			batch := new(leveldb.Batch)
			batch.Put(m.convertKeyStr(LastIndexKey), nextIdxArr)
			batch.Put(m.convertKey(key), idxArr)
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

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists
func (m *Index[K, I]) Get(key K) (idx I, err error) {
	var val []byte
	if val, err = m.db.Get(m.convertKey(key), nil); err != nil {
		if err == errors.ErrNotFound {
			err = index.ErrNotFound
		}
		return
	}

	idx = m.indexSerializer.FromBytes(val)
	return idx, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Index[K, I]) Contains(key K) bool {
	exists, _ := m.db.Has(m.convertKey(key), nil)
	return exists
}

// GetStateHash returns the index hash.
func (m *Index[K, I]) GetStateHash() (hash common.Hash, err error) {
	return m.hashIndex.Commit()
}

func (m *Index[K, I]) Close() error {
	// commit and persist the state
	hash, err := m.GetStateHash()
	if err != nil {
		return err
	}
	return m.db.Put(m.convertKeyStr(HashKey), m.hashSerializer.ToBytes(hash), nil)
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Index[K, I]) convertKey(key K) []byte {
	return m.table.AppendKey(m.keySerializer.ToBytes(key))
}

// convertKeyStr translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// from string
func (m *Index[K, I]) convertKeyStr(key string) []byte {
	return m.table.AppendKeyStr(key)
}
