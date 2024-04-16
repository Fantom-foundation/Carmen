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

package mpt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type CachedHasher[T any] interface {
	Hash(T) (common.Hash, bool)
	common.MemoryFootprintProvider
}

func NewAddressHasher() CachedHasher[common.Address] {
	return newGenericHasher[common.Address](
		func(addr common.Address) int { return int(addr[0]) | (int(addr[1]) << 8) | (int(addr[2]) << 16) },
		func(addr common.Address) common.Hash { return common.Keccak256ForAddress(addr) },
	)
}

func NewKeyHasher() CachedHasher[common.Key] {
	return newGenericHasher[common.Key](
		func(key common.Key) int {
			// Here the last 3 bytes are used since some keys are low-range big-endian values.
			return int(key[31]) | (int(key[30]) << 8) | (int(key[29]) << 16)
		},
		func(key common.Key) common.Hash { return common.Keccak256ForKey(key) },
	)
}

const hashCacheSize = 1 << 17 // ~128K entries

type genericHasher[T comparable] struct {
	entries    []cachedHasherEntry[T]
	simpleHash func(T) int
	cryptoHash func(T) common.Hash
}

// newGenericHasher creates a generic hash cache for a fixed type using the
// given simpleHash function for managing cached instances and the provided
// cryptoHash function for computing hashes of missing entries.
func newGenericHasher[T comparable](
	simpleHash func(T) int,
	cryptoHash func(T) common.Hash,
) CachedHasher[T] {
	return &genericHasher[T]{
		entries:    make([]cachedHasherEntry[T], hashCacheSize),
		simpleHash: simpleHash,
		cryptoHash: cryptoHash,
	}
}

func (h *genericHasher[T]) Hash(key T) (common.Hash, bool) {
	pos := h.simpleHash(key)
	entry := &h.entries[pos%hashCacheSize]
	entry.mutex.Lock()
	if entry.key == key && entry.used {
		res := entry.hash
		entry.mutex.Unlock()
		return res, true
	}
	entry.used = true
	entry.key = key
	entry.hash = h.cryptoHash(key)
	res := entry.hash
	entry.mutex.Unlock()
	return res, false
}

func (h *genericHasher[T]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	entrySize := unsafe.Sizeof(cachedHasherEntry[T]{})
	mf := common.NewMemoryFootprint(selfSize + uintptr(len(h.entries))*(entrySize))
	return mf
}

type cachedHasherEntry[K comparable] struct {
	key   K
	hash  common.Hash
	mutex sync.Mutex
	used  bool // TODO [perf]: eliminate the used field by initializing the cache
}

type HitMissTrackingCachedHasher[T any] struct {
	cache  CachedHasher[T]
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewHitMissTrackingCache wraps the given cache into a version tracking hits and misses.
func NewHitMissTrackingCache[T any](cache CachedHasher[T]) *HitMissTrackingCachedHasher[T] {
	return &HitMissTrackingCachedHasher[T]{cache: cache}
}

func (h *HitMissTrackingCachedHasher[T]) Hash(value T) (common.Hash, bool) {
	res, hit := h.cache.Hash(value)
	if hit {
		h.hits.Add(1)
	} else {
		h.misses.Add(1)
	}
	return res, hit
}

func (h *HitMissTrackingCachedHasher[T]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*h))
	mf.AddChild("cache", h.cache.GetMemoryFootprint())
	hits := h.hits.Load()
	misses := h.misses.Load()
	mf.SetNote(fmt.Sprintf("(hash-cache, hits %d, misses %d, hit ratio %f)", hits, misses, float64(hits)/float64(hits+misses)))
	return mf
}
