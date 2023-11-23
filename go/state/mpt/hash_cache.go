package mpt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

const hashCacheSize = 1 << 17 // ~128K entries
const enableHitMissCounter = false

type AddressHasher struct {
	entries []cachedHasherEntry[common.Address]
	hits    atomic.Uint64
	misses  atomic.Uint64
}

func NewAddressHasher() *AddressHasher {
	return &AddressHasher{
		entries: make([]cachedHasherEntry[common.Address], hashCacheSize),
	}
}

func (h *AddressHasher) Get(addr *common.Address) common.Hash {
	pos := int(addr[0]) | (int(addr[1]) << 8) | (int(addr[2]) << 16)
	entry := &h.entries[pos%hashCacheSize]
	entry.mutex.Lock()
	if entry.key == *addr && entry.used {
		if enableHitMissCounter {
			h.hits.Add(1)
		}
		res := entry.hash
		entry.mutex.Unlock()
		return res
	}
	if enableHitMissCounter {
		h.misses.Add(1)
	}
	entry.used = true
	entry.key = *addr
	entry.hash = common.Keccak256(addr[:])
	res := entry.hash
	entry.mutex.Unlock()
	return res
}

func (h *AddressHasher) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	entrySize := unsafe.Sizeof(cachedHasherEntry[common.Address]{})
	mf := common.NewMemoryFootprint(selfSize + uintptr(len(h.entries))*(entrySize))
	if enableHitMissCounter {
		hits := h.hits.Load()
		misses := h.misses.Load()
		mf.SetNote(fmt.Sprintf("(fast, hits %d, misses %d, hit ratio %f)", hits, misses, float64(hits)/float64(hits+misses)))
	}
	return mf
}

type KeyHasher struct {
	entries []cachedHasherEntry[common.Key]
	hits    atomic.Uint64
	misses  atomic.Uint64
}

func NewKeyHasher() *KeyHasher {
	return &KeyHasher{
		entries: make([]cachedHasherEntry[common.Key], hashCacheSize),
	}
}

func (h *KeyHasher) Get(key *common.Key) common.Hash {
	// Here the last 3 bytes are used since some keys are low-range big-endian values.
	pos := int(key[31]) | (int(key[30]) << 8) | (int(key[29]) << 16)
	entry := &h.entries[pos%hashCacheSize]
	entry.mutex.Lock()
	if entry.key == *key && entry.used {
		if enableHitMissCounter {
			h.hits.Add(1)
		}
		res := entry.hash
		entry.mutex.Unlock()
		return res
	}
	if enableHitMissCounter {
		h.misses.Add(1)
	}
	entry.used = true
	entry.key = *key
	entry.hash = common.Keccak256(key[:])
	res := entry.hash
	entry.mutex.Unlock()
	return res
}

func (h *KeyHasher) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*h)
	entrySize := unsafe.Sizeof(cachedHasherEntry[common.Address]{})
	mf := common.NewMemoryFootprint(selfSize + uintptr(len(h.entries))*(entrySize))
	if enableHitMissCounter {
		hits := h.hits.Load()
		misses := h.misses.Load()
		mf.SetNote(fmt.Sprintf("(fast, hits %d, misses %d, hit ratio %f)", hits, misses, float64(hits)/float64(hits+misses)))
	}
	return mf
}

type cachedHasherEntry[K comparable] struct {
	key   K
	hash  common.Hash
	mutex sync.Mutex
	used  bool // TODO: eliminate the used field by initializing the cache
}
