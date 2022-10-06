package state

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestCppStateIsState(t *testing.T) {
	cpp_state, _ := NewCppState()
	defer cpp_state.Release()
	var _ State = cpp_state
}

func TestReadUninitializedSlot(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	value, err := state.GetStorage(address1, key1)
	if err != nil {
		t.Fatalf("Error fetching storage slot: %v", err)
	}
	if (value != common.Value{}) {
		t.Errorf("Initial value is not zero, got %v", value)
	}
}

func TestWriteAndReadSlot(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	err = state.SetStorage(address1, key1, val1)
	if err != nil {
		t.Fatalf("Error updating storage: %v", err)
	}
	value, err := state.GetStorage(address1, key1)
	if err != nil {
		t.Fatalf("Error fetching storage slot: %v", err)
	}
	if value != val1 {
		t.Errorf("Invalid value read, got %v, wanted %v", value, val1)
	}
}
