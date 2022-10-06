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

func TestReadUninitializedBalance(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	balance, err := state.GetBalance(address1)
	if err != nil {
		t.Fatalf("Error fetching balance: %v", err)
	}
	if (balance != common.Balance{}) {
		t.Errorf("Initial balance is not zero, got %v", balance)
	}
}

func TestWriteAndReadBalance(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	err = state.SetBalance(address1, balance1)
	if err != nil {
		t.Fatalf("Error updating balance: %v", err)
	}
	balance, err := state.GetBalance(address1)
	if err != nil {
		t.Fatalf("Error fetching balance: %v", err)
	}
	if balance != balance1 {
		t.Errorf("Invalid balance read, got %v, wanted %v", balance, balance1)
	}
}

func TestReadUninitializedNonce(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	nonce, err := state.GetNonce(address1)
	if err != nil {
		t.Fatalf("Error fetching nonce: %v", err)
	}
	if (nonce != common.Nonce{}) {
		t.Errorf("Initial nonce is not zero, got %v", nonce)
	}
}

func TestWriteAndReadNonce(t *testing.T) {
	state, err := NewCppState()
	if err != nil {
		t.Fatalf("Failed to create C++ state instance: %v", err)
	}
	defer state.Release()

	err = state.SetNonce(address1, nonce1)
	if err != nil {
		t.Fatalf("Error updating nonce: %v", err)
	}
	nonce, err := state.GetNonce(address1)
	if err != nil {
		t.Fatalf("Error fetching nonce: %v", err)
	}
	if nonce != nonce1 {
		t.Errorf("Invalid nonce read, got %v, wanted %v", nonce, nonce1)
	}
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
