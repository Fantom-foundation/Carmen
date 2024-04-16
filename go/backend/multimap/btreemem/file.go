//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.
//

package btreemem

import (
	"github.com/Fantom-foundation/Carmen/go/backend/btree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// MultiMap implemented via BTree, and persisted via pagepool.PagePool and pagepool.FilePageStorage
type MultiMap[K any, V any] struct {
	btree *btree.BTree[dbKey[K, V]]

	keyComparator   common.Comparator[K]
	valueComparator common.Comparator[V]
}

// NewMultiMap creates new instance
func NewMultiMap[K any, V any](
	keySerializer common.Serializer[K],
	valueSerializer common.Serializer[V],
	keyComparator common.Comparator[K],
	valueComparator common.Comparator[V],
) *MultiMap[K, V] {

	dbKeySize := keySerializer.Size() + valueSerializer.Size()
	pageItems := common.PageSize / dbKeySize
	return &MultiMap[K, V]{
		btree:           btree.NewBTree[dbKey[K, V]](pageItems, dbKeyComparator[K, V]{keyComparator, valueComparator}),
		keyComparator:   keyComparator,
		valueComparator: valueComparator,
	}
}

func (m *MultiMap[K, V]) Add(key K, value V) error {
	m.btree.Insert(newDbKey[K, V](key, value))
	return nil
}

func (m *MultiMap[K, V]) Remove(key K, value V) error {
	m.btree.Remove(newDbKey[K, V](key, value))
	return nil
}

func (m *MultiMap[K, V]) RemoveAll(key K) error {
	it := m.btree.NewIterator(newDbKeyMinVal[K, V](key), newDbKeyMaxVal[K, V](key))
	keys := make([]dbKey[K, V], 0, 100)
	for it.HasNext() {
		keys = append(keys, it.Next())
	}
	for _, key := range keys {
		m.btree.Remove(key)
	}
	return nil
}

func (m *MultiMap[K, V]) GetAll(key K) ([]V, error) {
	it := m.btree.NewIterator(newDbKeyMinVal[K, V](key), newDbKeyMaxVal[K, V](key))
	values := make([]V, 0, 100)
	for it.HasNext() {
		values = append(values, it.Next().value)
	}
	return values, nil
}

// Flush the store
func (m *MultiMap[K, V]) Flush() error {
	return nil
}

// Close the store
func (m *MultiMap[K, V]) Close() error {
	return nil
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *MultiMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("btree", m.btree.GetMemoryFootprint())
	return mf
}
