package state

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAccountsAreInitiallyUnknown(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			account_state := state.GetAccountState(address1)
			if account_state != common.Unknown {
				t.Errorf("Initial account is not unknown, got %v", account_state)
			}
		})
	}
}

func TestAccountsCanBeCreated(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			state.CreateAccount(address1)
			account_state := state.GetAccountState(address1)
			if account_state != common.Exists {
				t.Errorf("Created account does not exist, got %v", account_state)
			}
		})
	}
}

func TestAccountsCanBeDeleted(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			state.CreateAccount(address1)
			state.DeleteAccount(address1)
			account_state := state.GetAccountState(address1)
			if account_state != common.Deleted {
				t.Errorf("Deleted account is not deleted, got %v", account_state)
			}
		})
	}
}

func TestReadUninitializedBalance(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			balance, err := state.GetBalance(address1)
			if err != nil {
				t.Fatalf("Error fetching balance: %v", err)
			}
			if (balance != common.Balance{}) {
				t.Errorf("Initial balance is not zero, got %v", balance)
			}
		})
	}
}

func TestWriteAndReadBalance(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			err := state.SetBalance(address1, balance1)
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
		})
	}
}

func TestReadUninitializedNonce(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			nonce, err := state.GetNonce(address1)
			if err != nil {
				t.Fatalf("Error fetching nonce: %v", err)
			}
			if (nonce != common.Nonce{}) {
				t.Errorf("Initial nonce is not zero, got %v", nonce)
			}
		})
	}
}

func TestWriteAndReadNonce(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			err := state.SetNonce(address1, nonce1)
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
		})
	}
}

func TestReadUninitializedSlot(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			value, err := state.GetStorage(address1, key1)
			if err != nil {
				t.Fatalf("Error fetching storage slot: %v", err)
			}
			if (value != common.Value{}) {
				t.Errorf("Initial value is not zero, got %v", value)
			}
		})
	}
}

func TestWriteAndReadSlot(t *testing.T) {
	for _, named_state := range initStates(t) {
		name := named_state.name
		state := named_state.state
		t.Run(name, func(t *testing.T) {
			err := state.SetStorage(address1, key1, val1)
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
		})
	}
}

type namedState struct {
	name  string
	state State
}

func initStates(t *testing.T) []namedState {
	in_memory, err := NewCppInMemoryState()
	if err != nil {
		t.Fatalf("Failed to create in-memory store: %v", err)
	}
	t.Cleanup(func() { in_memory.Release() })
	file_based, err := NewCppFileBasedState(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create in-memory store: %v", err)
	}
	t.Cleanup(func() { file_based.Release() })
	return []namedState{
		{"InMemory", in_memory},
		{"FileBased", file_based},
	}
}
