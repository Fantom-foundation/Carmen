package memory

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"io"
	"testing"
)

func TestMemoryDepotImplements(t *testing.T) {
	var s Depot[uint32]
	var _ depot.Depot[uint32] = &s
	var _ io.Closer = &s
}

var (
	A = []byte{0xAA}
	B = []byte{0xBB, 0xBB}
	C = []byte{0xCC}
)

func TestStoringIntoMemoryDepot(t *testing.T) {
	memory, err := NewDepot[uint64](2, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory depot; %s", err)
	}
	defer memory.Close()

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(1, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(2, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := memory.Get(5); value != nil {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := memory.Get(0); !bytes.Equal(value, A) {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := memory.Get(1); !bytes.Equal(value, B) {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := memory.Get(2); !bytes.Equal(value, C) {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	memory, err := NewDepot[uint64](2, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory depot; %s", err)
	}
	defer memory.Close()

	err = memory.Set(5, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(4, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(9, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := memory.Get(1); value != nil {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := memory.Get(5); !bytes.Equal(value, A) {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := memory.Get(4); !bytes.Equal(value, B) {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := memory.Get(9); !bytes.Equal(value, C) {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInMemoryDepot(t *testing.T) {
	memory, err := NewDepot[uint64](2, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory depot; %s", err)
	}
	defer memory.Close()

	initialHast, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}

	newHash, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if initialHast == newHash {
		t.Errorf("setting into the depot have not changed the hash %x %x", initialHast, newHash)
	}
}
