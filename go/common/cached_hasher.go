// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"sync"
	"unsafe"
)

// CachedHasher allows for hashing input keys.
// It caches the keys and returns an already cached value
// when it exists in the cache.
// If the key is not in the cache, it is hashed, stored in the cache
// and returned.
// This structure is safe for concurrent access, except
// the input hasher of the Hash method is modified (reset) with every
// hashing, i.e. the caller must make sure the hasher is thread local
// (i.e. not share across threads)
type CachedHasher[T comparable] struct {
	cache      *LruCache[T, Hash]
	serializer Serializer[T]
	cached     bool
	lock       *sync.Mutex
}

// NewCachedHasher creates a new hasher, that will use cache of computed hashes sized to the input capacity.
// If the capacity is set to zero, or negative, no cache will be used.
// Input serializer is used to convert the type, which will be hashed, to byte slice.
func NewCachedHasher[T comparable](cacheCapacity int, serializer Serializer[T]) *CachedHasher[T] {
	return &CachedHasher[T]{
		cache:      NewLruCache[T, Hash](cacheCapacity),
		serializer: serializer,
		cached:     cacheCapacity > 0,
		lock:       &sync.Mutex{},
	}
}

// Hash hashes the input type. It uses an internal cache, returning the hash
// from the cache, if the input type was already used and is retained in the cache.
// This method is thread safe.
func (h *CachedHasher[T]) Hash(t T) Hash {
	if !h.cached {
		return Keccak256(h.serializer.ToBytes(t))
	}

	h.lock.Lock()
	res, exists := h.cache.Get(t)
	h.lock.Unlock()
	if exists {
		return res
	}

	res = Keccak256(h.serializer.ToBytes(t))

	h.lock.Lock()
	h.cache.Set(t, res)
	h.lock.Unlock()
	return res
}

func (h *CachedHasher[T]) GetMemoryFootprint() *MemoryFootprint {
	mf := NewMemoryFootprint(unsafe.Sizeof(*h))
	mf.AddChild("cache", h.cache.GetMemoryFootprint(0))
	return mf
}
