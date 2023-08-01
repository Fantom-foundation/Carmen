package cache

import (
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

const (
	BranchingFactor = 3
	PageSize        = 2 * 32
	CacheCapacity   = 3
)

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
	D = common.Value{0xDD}
)

func TestStoreCacheFilled(t *testing.T) {
	path := t.TempDir()
	mem, _ := pagedfile.NewStore[uint32, common.Value](path, common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(path, BranchingFactor), 10000)
	store := NewStore[uint32, common.Value](mem, CacheCapacity)

	if _, err := store.Get(0); err != nil {
		t.Errorf("Error: %x", err)
	}

	// default value is cached
	if _, exists := store.cache.Get(0); !exists {
		t.Errorf("Cache must be filled")
	}

	// fill in next store elements
	if err := store.Set(1, B); err != nil {
		t.Errorf("Error: %x", err)
	}
	if err := store.Set(2, C); err != nil {
		t.Errorf("Error: %x", err)
	}

	// and check the cache
	if _, exists := store.cache.Get(0); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(1); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(2); !exists {
		t.Errorf("Cache must be filled")
	}
}

func TestStoreCacheEviction(t *testing.T) {
	path := t.TempDir()
	mem, _ := pagedfile.NewStore[uint32, common.Value](path, common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(path, BranchingFactor), 10000)
	store := NewStore[uint32, common.Value](mem, CacheCapacity)

	// fill in store
	if err := store.Set(0, A); err != nil {
		t.Errorf("Error: %x", err)
	}
	if err := store.Set(1, B); err != nil {
		t.Errorf("Error: %x", err)
	}
	if err := store.Set(2, C); err != nil {
		t.Errorf("Error: %x", err)
	}
	// case eviction of "A" (first one)
	if err := store.Set(3, D); err != nil {
		t.Errorf("Error: %x", err)
	}

	// and check the cache - first one is evicted
	if _, exists := store.cache.Get(0); exists {
		t.Errorf("Cache item must be evicted")
	}
	if _, exists := store.cache.Get(1); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(2); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(3); !exists {
		t.Errorf("Cache must be filled")
	}

	// but the item is in the store - it will go back to the cache
	if _, err := store.Get(0); err != nil {
		t.Errorf("Value cannot be fetched: %x", err)
	}

	// first value went back to the cache
	if _, exists := store.cache.Get(0); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(1); exists {
		t.Errorf("Cache item must be evicted")
	}
	if _, exists := store.cache.Get(2); !exists {
		t.Errorf("Cache must be filled")
	}
	if _, exists := store.cache.Get(3); !exists {
		t.Errorf("Cache must be filled")
	}
}
