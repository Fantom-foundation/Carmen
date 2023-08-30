package mpt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestInMemoryHashStore_SetAndGet(t *testing.T) {
	const N = 1000
	store, err := OpenInMemoryHashStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	for i := 0; i < 2*N; i++ {
		if err := store.Set(ValueId(uint64(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
			t.Fatalf("failed to set hash: %v", err)
		}
	}

	for i := 0; i < 2*N; i++ {
		want := common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}
		if hash, err := store.Get(ValueId(uint64(i))); err != nil || hash != want {
			t.Fatalf("fetched invalid hash for %d, got %v, wanted %v, err %v", i, hash, want, err)
		}
	}
}

func TestInMemoryHashStore_ConcurrentSetAndGetAreRaceFree(t *testing.T) {
	const N = 100
	store, err := OpenInMemoryHashStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	errors := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < N; j++ {
				for i := 0; i < 2*N; i++ {
					if err := store.Set(ValueId(uint64(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
						errors[id] = fmt.Errorf("failed to set hash: %v", err)
						return
					}
				}

				for i := 0; i < 2*N; i++ {
					if _, err := store.Get(ValueId(uint64(i))); err != nil {
						errors[id] = fmt.Errorf("failed to get hash: %v", err)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("issue encountered by goroutine %d: %v", i, err)
		}
	}
}

func TestFileBasedHashStore_SetAndGet(t *testing.T) {
	const N = 1000
	store, err := openFileBasedHashStore(t.TempDir(), N)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	for i := 0; i < 2*N; i++ {
		if err := store.Set(ValueId(uint64(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
			t.Fatalf("failed to set hash: %v", err)
		}
	}

	for i := 0; i < 2*N; i++ {
		want := common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}
		if hash, err := store.Get(ValueId(uint64(i))); err != nil || hash != want {
			t.Fatalf("fetched invalid hash for %d, got %v, wanted %v, err %v", i, hash, want, err)
		}
	}
}

func TestFileBasedHashStore_ConcurrentSetAndGetAreRaceFree(t *testing.T) {
	const N = 100
	store, err := openFileBasedHashStore(t.TempDir(), N)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	errors := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < N; j++ {
				for i := 0; i < 2*N; i++ {
					if err := store.Set(ValueId(uint64(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
						errors[id] = fmt.Errorf("failed to set hash: %v", err)
						return
					}
				}

				for i := 0; i < 2*N; i++ {
					if _, err := store.Get(ValueId(uint64(i))); err != nil {
						errors[id] = fmt.Errorf("failed to get hash: %v", err)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("issue encountered by goroutine %d: %v", i, err)
		}
	}
}
