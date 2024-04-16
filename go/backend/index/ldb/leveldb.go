//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package ldb

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
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
	db              backend.LevelDB
	table           backend.TableSpace
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *indexhash.IndexHash[K]
	lastIndex       I
}

// NewIndex creates a new instance of the index backed by a persisted database
func NewIndex[K comparable, I common.Identifier](
	db backend.LevelDB,
	table backend.TableSpace,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I]) (p *Index[K, I], err error) {

	// read the last hash from the database
	var hash common.Hash
	hashDbKey := strToDBKey(table, HashKey).ToBytes()
	hashBytes, err := db.Get(hashDbKey, nil)
	if err != nil && err != errors.ErrNotFound {
		return
	}
	if err == nil {
		hash = *(*common.Hash)(hashBytes)
	}

	// read the last index from the database
	var lastIndex I
	lastDbKey := strToDBKey(table, LastIndexKey).ToBytes()
	lastIndexBytes, err := db.Get(lastDbKey, nil)
	if err != nil && err != errors.ErrNotFound {
		return
	}
	if err == nil {
		lastIndex = indexSerializer.FromBytes(lastIndexBytes)
	}

	p = &Index[K, I]{
		db:              db,
		table:           table,
		keySerializer:   keySerializer,
		indexSerializer: indexSerializer,
		hashIndex:       indexhash.InitIndexHash[K](hash, keySerializer),
		lastIndex:       lastIndex,
	}

	// set err to nil as it can contain an ErrNotFound, which we want to suppress
	return p, nil
}

// Size returns the number of registered keys.
func (m *Index[K, I]) Size() I {
	return m.lastIndex
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Index[K, I]) GetOrAdd(key K) (idx I, err error) {
	var val []byte
	dbKey := m.convertKey(key).ToBytes()
	if val, err = m.db.Get(dbKey, nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := m.indexSerializer.ToBytes(m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := m.indexSerializer.ToBytes(m.lastIndex)

			lastIndexDbKey := m.convertKeyStr(LastIndexKey).ToBytes()
			batch := new(leveldb.Batch)
			batch.Put(lastIndexDbKey, nextIdxArr)
			batch.Put(dbKey, idxArr)
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
	dbKey := m.convertKey(key).ToBytes()
	if val, err = m.db.Get(dbKey, nil); err != nil {
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
	dbKey := m.convertKey(key).ToBytes()
	exists, _ := m.db.Has(dbKey, nil)
	return exists
}

// GetStateHash returns the index hash.
func (m *Index[K, I]) GetStateHash() (hash common.Hash, err error) {
	return m.hashIndex.Commit()
}

func (m *Index[K, I]) Flush() error {
	// commit and persist the state
	hash, err := m.GetStateHash()
	if err != nil {
		return err
	}

	hashDbKey := m.convertKeyStr(HashKey).ToBytes()
	return m.db.Put(hashDbKey, hash[:], nil)
}

func (m *Index[K, I]) Close() error {
	return m.Flush()
}

func (s *Index[K, I]) GetProof() (backend.Proof, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *Index[K, I]) CreateSnapshot() (backend.Snapshot, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *Index[K, I]) Restore(data backend.SnapshotData) error {
	return backend.ErrSnapshotNotSupported
}

func (s *Index[K, I]) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return nil, backend.ErrSnapshotNotSupported
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Index[K, I]) convertKey(key K) backend.DbKey {
	return backend.ToDBKey(m.table, m.keySerializer.ToBytes(key))
}

// convertKeyStr translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// from string
func (m *Index[K, I]) convertKeyStr(key string) backend.DbKey {
	return strToDBKey(m.table, key)
}

// GetMemoryFootprint provides the size of the index in memory in bytes
func (m *Index[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashIndex", m.hashIndex.GetMemoryFootprint())
	mf.AddChild("levelDb", m.db.GetMemoryFootprint())
	mf.SetNote(fmt.Sprintf("(items: %d)", m.lastIndex))
	return mf
}

// strToDBKey converts the input key to its respective table space key
func strToDBKey(t backend.TableSpace, key string) backend.DbKey {
	return backend.ToDBKey(t, []byte(key))
}
