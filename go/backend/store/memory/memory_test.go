package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/state"
	"testing"
)

func TestImplements(t *testing.T) {
	var s Memory[*state.Value]
	var _ store.Store[uint64, *state.Value] = &s
}

var (
	empty = state.Value{0xFF}
	A     = state.Value{0xAA}
	B     = state.Value{0xBB}
	C     = state.Value{0xCC}
)

func TestStoringIntoMemoryIndex(t *testing.T) {
	memory := NewMemory[*state.Value](&empty) // only the pointer type implements Serializable
	defer memory.Close()

	err := memory.Set(0, &A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(1, &B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(2, &C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	outItem := state.Value{}

	exists := memory.Get(5, &outItem)
	if exists {
		t.Fatalf("not-existing value is not reported as not-existing")
	}

	exists = memory.Get(0, &outItem)
	if !exists || outItem != A {
		t.Fatalf("reading written A returned different value")
	}

	exists = memory.Get(1, &outItem)
	if !exists || outItem != B {
		t.Fatalf("reading written B returned different value")
	}
}
