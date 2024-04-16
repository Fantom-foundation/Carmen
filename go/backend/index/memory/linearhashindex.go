//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package memory

import (
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// LinearHashIndex is an in-memory implementation of index.Index.
// It is implemented using the linear hash map implementation, instead of a build-in map
type LinearHashIndex[K comparable, I common.Identifier] struct {
	table           *LinearHashMap[K, I]
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *indexhash.IndexHash[K]

	maxIndex I // max index to fast compute next item
}

// NewLinearHashIndex constructs a new Index instance.
func NewLinearHashIndex[K comparable, I common.Identifier](keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) *LinearHashIndex[K, I] {
	numBuckets := 1 << 17 // 128K * 4kB -> 512MB init size
	return NewLinearHashParamsIndex[K, I](numBuckets, keySerializer, indexSerializer, hasher, comparator)
}

// NewLinearHashParamsIndex constructs a new Index instance with parameters setting up the number of buckets
func NewLinearHashParamsIndex[K comparable, I common.Identifier](numBuckets int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) *LinearHashIndex[K, I] {
	pageSize := 1 << 12 / (keySerializer.Size() + indexSerializer.Size()) // about 4kB per page
	memory := LinearHashIndex[K, I]{
		table:         NewLinearHashMap[K, I](pageSize, numBuckets, hasher, comparator),
		keySerializer: keySerializer,
		hashIndex:     indexhash.NewIndexHash[K](keySerializer),
	}
	return &memory
}

// Size returns the number of registered keys.
func (m *LinearHashIndex[K, I]) Size() I {
	return m.maxIndex
}

// GetOrAdd returns an index mapping for the key, or creates the new index.
func (m *LinearHashIndex[K, I]) GetOrAdd(key K) (val I, err error) {
	val, exists := m.table.GetOrAdd(key, m.maxIndex)
	if !exists {
		val = m.maxIndex
		m.maxIndex += 1 // increment to next index
		m.hashIndex.AddKey(key)
	}
	return
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists.
func (m *LinearHashIndex[K, I]) Get(key K) (val I, err error) {
	val, exists := m.table.Get(key)
	if !exists {
		err = index.ErrNotFound
	}
	return
}

// Contains returns whether the key exists in the mapping or not.
func (m *LinearHashIndex[K, I]) Contains(key K) (exists bool) {
	_, exists = m.table.Get(key)
	return
}

// GetStateHash returns the index hash.
func (m *LinearHashIndex[K, I]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

// Flush does nothing.
func (m *LinearHashIndex[K, I]) Flush() error {
	return nil
}

// Close closes the storage and clean-ups all possible dirty values.
func (m *LinearHashIndex[K, I]) Close() error {
	return nil
}

func (s *LinearHashIndex[K, I]) GetProof() (backend.Proof, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *LinearHashIndex[K, I]) CreateSnapshot() (backend.Snapshot, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *LinearHashIndex[K, I]) Restore(data backend.SnapshotData) error {
	return backend.ErrSnapshotNotSupported
}

func (s *LinearHashIndex[K, I]) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (m *LinearHashIndex[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("hashIndex", m.hashIndex.GetMemoryFootprint())
	memoryFootprint.AddChild("linearHash", m.table.GetMemoryFootprint())

	return memoryFootprint
}
