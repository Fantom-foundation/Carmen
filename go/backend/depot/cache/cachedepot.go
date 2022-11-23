package cache

import (
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Depot wraps a Depot with a cache.
type Depot[I common.Identifier] struct {
	depot     depot.Depot[I]
	cache     *common.Cache[I, []byte]
	sizeCache *common.Cache[I, int]
}

// NewDepot constructs a new Depot instance, caching access to the wrapped Depot.
func NewDepot[I common.Identifier](wrapped depot.Depot[I], cacheCapacity int, sizeCacheCapacity int) *Depot[I] {
	return &Depot[I]{wrapped, common.NewCache[I, []byte](cacheCapacity), common.NewCache[I, int](sizeCacheCapacity)}
}

func (m *Depot[I]) Set(id I, value []byte) (err error) {
	err = m.depot.Set(id, value)
	if err != nil {
		return err
	}
	m.cache.Set(id, value)
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
		return uintptr(len(value))
	}))
	mf.AddChild("sourceDepot", m.depot.GetMemoryFootprint())
	return mf
}
