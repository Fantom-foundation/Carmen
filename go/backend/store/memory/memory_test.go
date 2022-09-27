package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

func TestMemoryStoreImplements(t *testing.T) {
	var s Memory[common.Value]
	var _ store.Store[uint32, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoringIntoMemoryStore(t *testing.T) {
	defaultItem := common.Value{}
	memory := NewMemory[common.Value](common.ValueSerializer{}, defaultItem, 32, 3)
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

	if memory.Get(5) != defaultItem {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if memory.Get(0) != A {
		t.Errorf("reading written A returned different value")
	}
	if memory.Get(1) != B {
		t.Errorf("reading written B returned different value")
	}
	if memory.Get(2) != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	defaultItem := common.Value{}
	memory := NewMemory[common.Value](common.ValueSerializer{}, defaultItem, 32, 3)
	defer memory.Close()

	err := memory.Set(5, A)
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

	if memory.Get(1) != defaultItem {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if memory.Get(5) != A {
		t.Errorf("reading written A returned different value")
	}
	if memory.Get(4) != B {
		t.Errorf("reading written B returned different value")
	}
	if memory.Get(9) != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInMemoryStore(t *testing.T) {
	defaultItem := common.Value{}
	memory := NewMemory[common.Value](common.ValueSerializer{}, defaultItem, 32, 3)
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
		t.Errorf("setting into the store have not changed the hash %x %x", initialHast, newHash)
	}
}
