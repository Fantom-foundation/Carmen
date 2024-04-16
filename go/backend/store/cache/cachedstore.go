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

package cache

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Store wraps a cache and a store. It caches the stored keys
type Store[I common.Identifier, V any] struct {
	store store.Store[I, V]
	cache *common.LruCache[I, V]
}

// NewStore creates a new store wrapping the input one, and creates a new cache with the given capacity
// this store maintains a cache of keys
func NewStore[I common.Identifier, V any](store store.Store[I, V], cacheCapacity int) *Store[I, V] {
	return &Store[I, V]{store, common.NewLruCache[I, V](cacheCapacity)}
}

func (m *Store[I, V]) Set(id I, value V) error {
	// write through cache
	m.cache.Set(id, value)
	return m.store.Set(id, value)
}

func (m *Store[I, V]) Get(id I) (v V, err error) {
	v, exists := m.cache.Get(id)
	if !exists {
		if v, err = m.store.Get(id); err == nil {
			m.cache.Set(id, v)
		}
	}

	return
}

func (m *Store[I, V]) GetProof() (backend.Proof, error) {
	return m.store.GetProof()
}

func (m *Store[I, V]) CreateSnapshot() (backend.Snapshot, error) {
	return m.store.CreateSnapshot()
}

func (m *Store[I, V]) Restore(data backend.SnapshotData) error {
	m.cache.Clear()
	return m.store.Restore(data)
}

func (m *Store[I, V]) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return m.store.GetSnapshotVerifier(metadata)
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.store.GetStateHash()
}

func (m *Store[I, V]) Flush() error {
	// commit state hash root
	_, err := m.GetStateHash()
	return err
}

func (m *Store[I, V]) Close() error {
	if err := m.Flush(); err != nil {
		return err
	}
	return m.store.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("backingStore", m.store.GetMemoryFootprint())
	mf.AddChild("cache", m.cache.GetMemoryFootprint(0))
	return mf
}
