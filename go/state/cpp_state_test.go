package state

import (
	"bytes"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAccountsAreInitiallyUnknown(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			account_state, _ := state.GetAccountState(address1)
			if account_state != common.Unknown {
				t.Errorf("Initial account is not unknown, got %v", account_state)
			}
		})
	}
}

func TestAccountsCanBeCreated(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			state.createAccount(address1)
			account_state, _ := state.GetAccountState(address1)
			if account_state != common.Exists {
				t.Errorf("Created account does not exist, got %v", account_state)
			}
		})
	}
}

func TestAccountsCanBeDeleted(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			state.createAccount(address1)
			state.deleteAccount(address1)
			account_state, _ := state.GetAccountState(address1)
			if account_state != common.Unknown {
				t.Errorf("Deleted account is not deleted, got %v", account_state)
			}
		})
	}
}

func TestReadUninitializedBalance(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

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
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			err = state.setBalance(address1, balance1)
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
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

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
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			err = state.setNonce(address1, nonce1)
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
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

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
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			err = state.setStorage(address1, key1, val1)
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

func getTestCodeOfLength(size int) []byte {
	res := make([]byte, size)
	for i := 0; i < size; i++ {
		res[i] = byte(i)
	}
	return res
}

func getTestCodes() [][]byte {
	return [][]byte{
		nil,
		{},
		{0xAC},
		{0xAC, 0xDC},
		getTestCodeOfLength(100),
		getTestCodeOfLength(1000),
		getTestCodeOfLength(24577),
	}
}

func TestSetAndGetCode(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			for _, code := range getTestCodes() {
				err := state.setCode(address1, code)
				if err != nil {
					t.Fatalf("Error setting code: %v", err)
				}
				value, err := state.GetCode(address1)
				if err != nil {
					t.Fatalf("Error fetching code: %v", err)
				}
				if !bytes.Equal(code, value) {
					t.Errorf("Invalid value read, got %v, wanted %v", value, code)
				}
				size, err := state.GetCodeSize(address1)
				if err != nil {
					t.Fatalf("Error fetching code size: %v", err)
				}
				if size != len(code) {
					t.Errorf("Invalid value size read, got %v, wanted %v", size, len(code))
				}
			}
		})
	}
}

func TestSetAndGetCodeHash(t *testing.T) {
	for _, config := range initCppStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			for _, code := range getTestCodes() {
				err := state.setCode(address1, code)
				if err != nil {
					t.Fatalf("Error setting code: %v", err)
				}
				hash, err := state.GetCodeHash(address1)
				if err != nil {
					t.Fatalf("Error fetching code: %v", err)
				}
				want := common.GetKeccak256Hash(code)
				if hash != want {
					t.Errorf("Invalid code hash, got %v, wanted %v", hash, want)
				}
			}
		})
	}
}

func initCppStates() []namedStateConfig {
	return []namedStateConfig{
		{"InMemory", castToDirectUpdateState(NewCppInMemoryState)},
		{"FileBased", castToDirectUpdateState(NewCppFileBasedState)},
		{"LevelDbBased", castToDirectUpdateState(NewCppLevelDbBasedState)},
	}
}
