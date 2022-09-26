package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0x01}
	B = common.Address{0x02}
)

const (
	HashA  = "21fc3f955c14305ed66b2f6064de082e8447f29048da3ab7c5c01090c1b722ab"
	HashAB = "e2f6dad46dbab4a98b5f5502b171c63780b94cade5d38badce241c9eecea4573"
)

func TestImplements(t *testing.T) {
	var memory Memory[*common.Address]
	var _ index.Index[*common.Address, uint64] = &memory
}

func TestStoringIntoMemoryIndex(t *testing.T) {
	memory := NewMemory[common.Address](common.AddressSerializer{})
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

func TestHash(t *testing.T) {
	memory := NewMemory[common.Address](common.AddressSerializer{})
	defer memory.Close()

	// the hash is the default one first
	h0, _ := memory.GetStateHash()

	if (h0 != common.Hash{}) {
		t.Errorf("The hash does not match the default one")
	}

	// the hash must change when adding a new item
	_, _ = memory.GetOrAdd(A)
	ha1, _ := memory.GetStateHash()

	if h0 == ha1 {
		t.Errorf("The hash has not changed")
	}

	// the hash remains the same when getting an existing item
	_, _ = memory.GetOrAdd(A)
	ha2, _ := memory.GetStateHash()

	if ha1 != ha2 {
		t.Errorf("The hash has changed")
	}

	if fmt.Sprintf("%x\n", ha1) != fmt.Sprintf("%s\n", HashA) {
		t.Errorf("Hash is %x and not %s", ha1, HashA)
	}

	// try recursive hash with B and already indexed A
	_, _ = memory.GetOrAdd(B)
	hb1, _ := memory.GetStateHash()

	if fmt.Sprintf("%x\n", hb1) != fmt.Sprintf("%s\n", HashAB) {
		t.Errorf("Hash is %x and not %s", hb1, HashAB)
	}

	// The hash must remain the same when adding still the same key
	_, _ = memory.GetOrAdd(B)
	hb2, _ := memory.GetStateHash()

	if hb1 != hb2 {
		t.Errorf("The hash has changed")
	}
}
