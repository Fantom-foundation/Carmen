package common

import (
	"golang.org/x/crypto/sha3"
	"hash"
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
	cache      *Cache[T, Hash]
	serializer Serializer[T]
	cached     bool
	lock       *sync.Mutex
	pool       *hasherPool
}

// NewCachedHasher creates a new hasher, that will use cache of computed hashes sized to the input capacity.
// If the capacity is set to zero, or negative, no cache will be used.
// Input serializer is used to convert the type, which will be hashed, to byte slice.
func NewCachedHasher[T comparable](cacheCapacity int, serializer Serializer[T]) *CachedHasher[T] {
	return &CachedHasher[T]{
		cache:      NewCache[T, Hash](cacheCapacity),
		serializer: serializer,
		cached:     cacheCapacity > 0,
		lock:       &sync.Mutex{},
		pool:       newHasherPool(),
	}
}

// Hash hashes the input type. It uses an internal cache, returning the hash
// from the cache, if the input type was already used and is retained in the cache.
// This method is thread safe.
func (h *CachedHasher[T]) Hash(t T) Hash {
	if !h.cached {
		hasher := h.pool.getHasher()
		defer h.pool.returnHasher(hasher)
		return GetHash(hasher, h.serializer.ToBytes(t))
	}

	h.lock.Lock()
	res, exists := h.cache.Get(t)
	h.lock.Unlock()
	if exists {
		return res
	}

	hasher := h.pool.getHasher()
	res = GetHash(hasher, h.serializer.ToBytes(t))
	h.pool.returnHasher(hasher)

	h.lock.Lock()
	h.cache.Set(t, res)
	h.lock.Unlock()
	return res
}

func (h *CachedHasher[T]) GetMemoryFootprint() *MemoryFootprint {
	mf := NewMemoryFootprint(unsafe.Sizeof(*h))
	mf.AddChild("cache", h.cache.GetMemoryFootprint(0))
	mf.AddChild("hashersPool", h.pool.GetMemoryFootprint())
	return mf
}

// hasherPool is a synchronised pool of hashers. Whenever a hasher is required
// it is either returned from the pool, or created as new, if no hasher is available in the pool
type hasherPool struct {
	pool []hash.Hash
	lock *sync.Mutex
}

func newHasherPool() *hasherPool {
	return &hasherPool{
		pool: make([]hash.Hash, 0, 100),
		lock: &sync.Mutex{},
	}
}

// getHasher returns a hasher. The hasher is either from the pool,
// or created as a new one.
func (p *hasherPool) getHasher() hash.Hash {
	p.lock.Lock()
	defer p.lock.Unlock()

	var hasher hash.Hash
	if len(p.pool) > 0 {
		hasher = p.pool[len(p.pool)-1]
		p.pool = p.pool[0 : len(p.pool)-1]
	} else {
		hasher = sha3.NewLegacyKeccak256()
	}

	return hasher
}

// returnHasher returns the hasher back to the pool. It is not checked if the method was
// called at most once for the same hasher. It is up to the caller.
func (p *hasherPool) returnHasher(hasher hash.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.pool = append(p.pool, hasher)
}

func (p *hasherPool) GetMemoryFootprint() *MemoryFootprint {
	mf := NewMemoryFootprint(unsafe.Sizeof(*p))
	var h hash.Hash
	mf.AddChild("cache", NewMemoryFootprint(uintptr(len(p.pool))*unsafe.Sizeof(h)))
	return mf

}
