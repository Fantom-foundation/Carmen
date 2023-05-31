package cache

import (
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Index wraps another index and a cache
type Index[K comparable, I common.Identifier] struct {
	wrapped index.Index[K, I]
	cache   *common.Cache[K, I]
}

// NewIndex constructs a new Index instance, which either delegates to the wrapped index or gets data from the cache if it has them.
func NewIndex[K comparable, I common.Identifier](wrapped index.Index[K, I], cacheCapacity int) *Index[K, I] {
	return &Index[K, I]{wrapped, common.NewCache[K, I](cacheCapacity)}
}

// Size returns the number of registered keys.
func (m *Index[K, I]) Size() I {
	return m.wrapped.Size()
}

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Index[K, I]) GetOrAdd(key K) (idx I, err error) {
	idx, exists := m.cache.Get(key)
	if !exists {
		idx, err = m.wrapped.GetOrAdd(key)
		m.cache.Set(key, idx)
	}
	return
}

// GetOrAddMany is the same as GetOrAdd but for a list of elements.
func (m *Index[K, I]) GetOrAddMany(keys []K) ([]I, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) == 1 {
		i, err := m.GetOrAdd(keys[0])
		return []I{i}, err
	}

	// Merge results from the cache and the underlying implementation.
	res := make([]I, len(keys))

	// Collect cached elements.
	missing := make([]K, 0, len(keys))
	positions := make([]int, 0, len(keys))
	for i, key := range keys {
		idx, exists := m.cache.Get(key)
		if exists {
			res[i] = idx
		} else {
			missing = append(missing, key)
			positions = append(positions, i)
		}
	}

	// Resolve missing elements.
	if len(missing) > 0 {
		idxs, err := m.wrapped.GetOrAddMany(missing)
		if err != nil {
			return nil, err
		}
		for i, idx := range idxs {
			res[positions[i]] = idx
			m.cache.Set(missing[i], idx)
		}
	}

	return res, nil
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists
func (m *Index[K, I]) Get(key K) (idx I, err error) {
	idx, exists := m.cache.Get(key)
	if !exists {
		idx, err = m.wrapped.Get(key)
		if err == nil {
			m.cache.Set(key, idx)
		}
	}
	return
}

// Contains returns whether the key exists in the mapping or not.
func (m *Index[K, I]) Contains(key K) (exists bool) {
	_, exists = m.cache.Get(key)
	if !exists {
		if idx, err := m.wrapped.Get(key); err == nil {
			m.cache.Set(key, idx)
		}
	}

	return
}

// GetStateHash returns the index hash.
func (m *Index[K, I]) GetStateHash() (common.Hash, error) {
	return m.wrapped.GetStateHash()
}

// Flush pushes buffered write operations to disk.
func (m *Index[K, I]) Flush() error {
	return m.wrapped.Flush()
}

// Close closes the storage and clean-ups all possible dirty values.
func (m *Index[K, I]) Close() error {
	return m.wrapped.Close()
}

func (m *Index[K, I]) GetProof() (backend.Proof, error) {
	hash, err := m.GetStateHash()
	if err != nil {
		return nil, err
	}

	return index.NewIndexProof(common.Hash{}, hash), nil
}

func (m *Index[K, I]) CreateSnapshot() (backend.Snapshot, error) {
	return m.wrapped.CreateSnapshot()
}

func (m *Index[K, I]) Restore(data backend.SnapshotData) error {
	m.cache.Clear()
	return m.wrapped.Restore(data)
}

func (m *Index[K, I]) GetSnapshotVerifier(data []byte) (backend.SnapshotVerifier, error) {
	return m.wrapped.GetSnapshotVerifier(data)
}

// GetMemoryFootprint provides the size of the index in memory in bytes
func (m *Index[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("cache", m.cache.GetMemoryFootprint(0))
	mf.AddChild("sourceIndex", m.wrapped.GetMemoryFootprint())
	return mf
}
