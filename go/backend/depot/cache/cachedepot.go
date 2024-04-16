//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package cache

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Depot wraps a Depot with a cache.
type Depot[I common.Identifier] struct {
	depot     depot.Depot[I]
	cache     *common.LruCache[I, []byte]
	sizeCache *common.LruCache[I, int]
}

// NewDepot constructs a new Depot instance, caching access to the wrapped Depot.
func NewDepot[I common.Identifier](wrapped depot.Depot[I], cacheCapacity int, sizeCacheCapacity int) *Depot[I] {
	return &Depot[I]{wrapped, common.NewLruCache[I, []byte](cacheCapacity), common.NewLruCache[I, int](sizeCacheCapacity)}
}

func (m *Depot[I]) Set(id I, value []byte) (err error) {
	err = m.depot.Set(id, value)
	if err != nil {
		return err
	}
	m.cache.Set(id, value)
	m.sizeCache.Set(id, len(value))
	return nil
}

// Get a value associated with the index (or nil if not defined)
func (m *Depot[I]) Get(id I) (value []byte, err error) {
	value, exists := m.cache.Get(id)
	if exists {
		return value, nil
	}
	value, err = m.depot.Get(id)
	if err == nil {
		m.cache.Set(id, value)
		m.sizeCache.Set(id, len(value))
	}
	return value, err
}

// GetSize of a value associated with the index (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (size int, err error) {
	size, exists := m.sizeCache.Get(id)
	if exists {
		return size, nil
	}
	size, err = m.depot.GetSize(id)
	if err == nil {
		m.sizeCache.Set(id, size)
	}
	return size, err
}

func (m *Depot[I]) GetProof() (backend.Proof, error) {
	return m.depot.GetProof()
}

func (m *Depot[I]) CreateSnapshot() (backend.Snapshot, error) {
	return m.depot.CreateSnapshot()
}

func (m *Depot[I]) Restore(data backend.SnapshotData) error {
	m.cache.Clear()
	return m.depot.Restore(data)
}

func (m *Depot[I]) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return m.depot.GetSnapshotVerifier(metadata)
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.depot.GetStateHash()
}

// Flush the depot
func (m *Depot[I]) Flush() error {
	return m.depot.Flush()
}

// Close the depot
func (m *Depot[I]) Close() error {
	return m.depot.Close()
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("cache", m.cache.GetDynamicMemoryFootprint(func(value []byte) uintptr {
		return uintptr(cap(value)) // memory consumed by the code slice
	}))
	mf.AddChild("sizeCache", m.sizeCache.GetMemoryFootprint(0))
	mf.AddChild("sourceDepot", m.depot.GetMemoryFootprint())
	return mf
}
