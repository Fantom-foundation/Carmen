package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/util"
	"unsafe"
)

const KeySize = 8
const ValueSize = 8

// MultiMap is a LevelDB multimap.MultiMap implementation - it maps IDs to values
type MultiMap[K common.Identifier, V common.Identifier] struct {
	db              common.LevelDB
	table           common.TableSpace
	keySerializer   common.Serializer[K]
	valueSerializer common.Serializer[V]
}

func NewMultiMap[K common.Identifier, V common.Identifier](
	db common.LevelDB,
	table common.TableSpace,
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
	var dbKey [1 + KeySize + ValueSize]byte
	dbKey[0] = byte(m.table)
	m.keySerializer.CopyBytes(key, dbKey[1:1+KeySize])
	m.valueSerializer.CopyBytes(value, dbKey[1+KeySize:])
	return m.db.Put(dbKey[:], nil, nil)
}

// Remove removes a single key/value entry.
func (m *MultiMap[K, V]) Remove(key K, value V) error {
	var dbKey [17]byte
	dbKey[0] = byte(m.table)
	m.keySerializer.CopyBytes(key, dbKey[1:1+KeySize])
	m.valueSerializer.CopyBytes(value, dbKey[1+KeySize:])
	return m.db.Delete(dbKey[:], nil)
}

func (m *MultiMap[K, V]) getRangeForKey(key K) util.Range {
	var startDbKey, endDbKey [17]byte
	startDbKey[0] = byte(m.table)
	m.keySerializer.CopyBytes(key, startDbKey[1:1+KeySize])
	copy(endDbKey[0:1+KeySize], startDbKey[0:1+KeySize])
	for i := 1 + KeySize; i < 1+KeySize+ValueSize; i++ {
		endDbKey[i] = 0xFF
	}

	return util.Range{Start: startDbKey[:], Limit: endDbKey[:]}
}

// RemoveAll removes all entries with the given key.
func (m *MultiMap[K, V]) RemoveAll(key K) error {
	keysRange := m.getRangeForKey(key)
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	for iter.Next() {
		err := m.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	return iter.Error()
}

// ForEach applies the given operation on each value associated to the given key.
func (m *MultiMap[K, V]) ForEach(key K, callback func(V)) error {
	keysRange := m.getRangeForKey(key)
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	for iter.Next() {
		callback(m.valueSerializer.FromBytes(iter.Key()[9:]))
	}
	return iter.Error()
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
