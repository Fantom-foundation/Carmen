package state

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAccountsAreInitiallyUnknown(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		account_state, _ := state.Exists(address1)
		if account_state != false {
			t.Errorf("Initial account is not unknown, got %v", account_state)
		}
	})
}

func TestAccountsCanBeCreated(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		state.CreateAccount(address1)
		account_state, _ := state.Exists(address1)
		if account_state != true {
			t.Errorf("Created account does not exist, got %v", account_state)
		}
	})
}

func TestAccountsCanBeDeleted(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		state.CreateAccount(address1)
		state.DeleteAccount(address1)
		account_state, _ := state.Exists(address1)
		if account_state != false {
			t.Errorf("Deleted account is not deleted, got %v", account_state)
		}
	})
}

func TestReadUninitializedBalance(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		balance, err := state.GetBalance(address1)
		if err != nil {
			t.Fatalf("Error fetching balance: %v", err)
		}
		if (balance != common.Balance{}) {
			t.Errorf("Initial balance is not zero, got %v", balance)
		}
	})
}

func TestWriteAndReadBalance(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
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

func TestReadUninitializedNonce(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		nonce, err := state.GetNonce(address1)
		if err != nil {
			t.Fatalf("Error fetching nonce: %v", err)
		}
		if (nonce != common.Nonce{}) {
			t.Errorf("Initial nonce is not zero, got %v", nonce)
		}
	})
}

func TestWriteAndReadNonce(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
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

func TestReadUninitializedSlot(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		value, err := state.GetStorage(address1, key1)
		if err != nil {
			t.Fatalf("Error fetching storage slot: %v", err)
		}
		if (value != common.Value{}) {
			t.Errorf("Initial value is not zero, got %v", value)
		}
	})
}

func TestWriteAndReadSlot(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
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
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		for _, code := range getTestCodes() {
			err := state.SetCode(address1, code)
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

func TestSetAndGetCodeHash(t *testing.T) {
	runForEachCppConfig(t, func(t *testing.T, state directUpdateState) {
		for _, code := range getTestCodes() {
			err := state.SetCode(address1, code)
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

func initCppStates() []namedStateConfig {
	list := []namedStateConfig{}
	for _, s := range GetAllSchemas() {
		list = append(list, []namedStateConfig{
			{"memory", s, "cpp-memory"},
			{"file", s, "cpp-file"},
			{"leveldb", s, "cpp-ldb"},
		}...)
	}
	return list
}

func runForEachCppConfig(t *testing.T, test func(*testing.T, directUpdateState)) {
	for _, config := range initCppStates() {
		config := config
		t.Run(config.name, func(t *testing.T) {
			t.Parallel()
			state, err := config.createState(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("failed to initialize state %s: %v", config.name, err)
				} else {
					t.Fatalf("failed to initialize state %s: %v", config.name, err)
				}
			}
			defer state.Close()
			test(t, state)
		})
	}
}
