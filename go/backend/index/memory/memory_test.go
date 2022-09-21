package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/state"
	"testing"
)

var (
	A = state.Address{0x01}
	B = state.Address{0x02}
)

func TestImplements(t *testing.T) {
	var memory Memory[state.Address]
	var _ index.Index[state.Address, uint64] = &memory
}

func TestStoringIntoMemoryIndex(t *testing.T) {
	memory := NewMemory[state.Address]()
	defer memory.Close()

	indexA, err := memory.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A; %s", err)
		return
	}
	if indexA != 0 {
		t.Errorf("first inserted is not 0")
		return
	}
	indexB, err := memory.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed add of address B; %s", err)
		return
	}
	if indexB != 1 {
		t.Errorf("second inserted is not 1")
		return
	}

	if !memory.Contains(A) {
		t.Errorf("memory does not contains inserted A")
		return
	}
	if !memory.Contains(B) {
		t.Errorf("memory does not contains inserted B")
		return
	}

	indexA2, err := memory.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed second add of address A; %s", err)
		return
	}
	if indexA != indexA2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}

	indexB2, err := memory.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed second add of address B; %s", err)
		return
	}
	if indexB != indexB2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}
}
