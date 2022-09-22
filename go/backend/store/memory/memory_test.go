package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestImplements(t *testing.T) {
	var s Memory[common.Value]
	var _ store.Store[uint64, common.Value] = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoringIntoMemoryIndex(t *testing.T) {
	defaultItem := common.Value{}
	memory := NewMemory[common.Value](common.ValueSerializer{}, defaultItem)
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

	if *memory.Get(5) != defaultItem {
		t.Fatalf("not-existing value is not reported as not-existing")
	}

	if *memory.Get(0) != A {
		t.Fatalf("reading written A returned different value")
	}

	if *memory.Get(1) != B {
		t.Fatalf("reading written B returned different value")
	}
}
