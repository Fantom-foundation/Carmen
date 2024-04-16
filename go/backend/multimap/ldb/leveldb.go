//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"unsafe"
)

// MultiMap is a LevelDB multimap.MultiMap implementation - it maps IDs to values
type MultiMap[K any, V any] struct {
	db              backend.LevelDB
	table           backend.TableSpace
	keySerializer   common.Serializer[K]
	valueSerializer common.Serializer[V]
}

func NewMultiMap[K any, V any](
	db backend.LevelDB,
	table backend.TableSpace,
	keySerializer common.Serializer[K],
	valueSerializer common.Serializer[V],
) *MultiMap[K, V] {
	return &MultiMap[K, V]{
		db:              db,
		table:           table,
		keySerializer:   keySerializer,
		valueSerializer: valueSerializer,
	}
}

// Add adds the given key/value pair.
func (m *MultiMap[K, V]) Add(key K, value V) error {
	var dbKey DbKey[K, V]
	dbKey.SetTableKey(m.table, key, m.keySerializer)
	dbKey.SetValue(value, m.valueSerializer)
	return m.db.Put(dbKey[:], nil, nil)
}

// Remove removes a single key/value entry.
func (m *MultiMap[K, V]) Remove(key K, value V) error {
	var dbKey DbKey[K, V]
	dbKey.SetTableKey(m.table, key, m.keySerializer)
	dbKey.SetValue(value, m.valueSerializer)
	return m.db.Delete(dbKey[:], nil)
}

func (m *MultiMap[K, V]) getRangeForKey(key K) util.Range {
	var startDbKey, endDbKey DbKey[K, V]
	startDbKey.SetTableKey(m.table, key, m.keySerializer)
	endDbKey.CopyFrom(&startDbKey)
	endDbKey.SetMaxValue()

	return util.Range{Start: startDbKey[:], Limit: endDbKey[:]}
}

// RemoveAll removes all entries with the given key.
func (m *MultiMap[K, V]) RemoveAll(key K) error {
	keysRange := m.getRangeForKey(key)
	batch := new(leveldb.Batch)
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	for iter.Next() {
		batch.Delete(iter.Key())
	}
	if err := iter.Error(); err != nil {
		return err
	}

	return m.db.Write(batch, nil)
}

// GetAll provides all values associated with the given key.
func (m *MultiMap[K, V]) GetAll(key K) ([]V, error) {
	keysRange := m.getRangeForKey(key)
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	values := make([]V, 0, 64)
	for iter.Next() {
		values = append(values, m.valueSerializer.FromBytes(iter.Key()[9:]))
	}
	return values, iter.Error()
}

// Flush the store
func (m *MultiMap[K, V]) Flush() error {
	return nil // no-op for ldb database
}

// Close the store
func (m *MultiMap[K, V]) Close() error {
	return nil // no-op for ldb database
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *MultiMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("levelDb", m.db.GetMemoryFootprint())
	return mf
}
