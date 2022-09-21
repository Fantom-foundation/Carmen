package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/state"
	"testing"
)

func TestImplements(t *testing.T) {
	var s Memory[state.Value]
	var _ store.Store[uint64, state.Value] = &s
}

var (
	empty = state.Value{0xFF}
	A     = state.Value{0xAA}
	B     = state.Value{0xBB}
	C     = state.Value{0xCC}
)

func TestStoringIntoMemoryIndex(t *testing.T) {
	memory := NewMemory[state.Value](empty)
	defer memory.Close()

	err := memory.Set(0, A)
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

	notExisting := memory.Get(5)
	if notExisting != empty {
		t.Fatalf("not-existing value is not empty value")
	}

	readA := memory.Get(0)
	if readA != A {
		t.Fatalf("reading written A returned different value")
	}

	readB := memory.Get(1)
	if readB != B {
		t.Fatalf("reading written B returned different value")
	}
}
