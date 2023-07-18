package s4

import (
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
		if err := store.Set(ValueId(uint32(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
			t.Fatalf("failed to set hash: %v", err)
		}
	}

	for i := 0; i < 2*N; i++ {
		want := common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}
		if hash, err := store.Get(ValueId(uint32(i))); err != nil || hash != want {
			t.Fatalf("fetched invalid hash for %d, got %v, wanted %v, err %v", i, hash, want, err)
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
		if err := store.Set(ValueId(uint32(i)), common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}); err != nil {
			t.Fatalf("failed to set hash: %v", err)
		}
	}

	for i := 0; i < 2*N; i++ {
		want := common.Hash{byte(i >> 16), byte(i >> 8), byte(i)}
		if hash, err := store.Get(ValueId(uint32(i))); err != nil || hash != want {
			t.Fatalf("fetched invalid hash for %d, got %v, wanted %v, err %v", i, hash, want, err)
		}
	}
}
