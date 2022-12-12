package state

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type namedStateConfig struct {
	name        string
	createState func(tmpDir string) (State, error)
}

func initStates() []namedStateConfig {
	var res []namedStateConfig
	for _, s := range initCppStates() {
		res = append(res, namedStateConfig{name: "cpp-" + s.name, createState: s.createState})
	}
	for _, s := range initGoStates() {
		res = append(res, namedStateConfig{name: "go-" + s.name, createState: s.createState})
	}
	return res
}

func testEachConfiguration(t *testing.T, test func(t *testing.T, s State)) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			test(t, state)
		})
	}
}

func testHashAfterModification(t *testing.T, mod func(s State)) {
	ref, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to create reference state: %v", err)
	}
	mod(ref)
	want, err := ref.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash of reference state: %v", err)
	}
	testEachConfiguration(t, func(t *testing.T, state State) {
		mod(state)
		got, err := state.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}
		if want != got {
			t.Errorf("Invalid hash, wanted %v, got %v", want, got)
		}
	})
}

func TestEmptyHash(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		// nothing
	})
}

func TestAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
	})
}

func TestMultipleAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestDeletedAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.DeleteAccount(address1)
		s.DeleteAccount(address2)
	})
}

func TestStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetStorage(address1, key2, val3)
	})
}

func TestMultipleStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetStorage(address1, key2, val3)
		s.SetStorage(address2, key3, val1)
		s.SetStorage(address3, key1, val2)
	})
}

func TestBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetBalance(address1, balance1)
	})
}

func TestMultipleBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetBalance(address1, balance1)
		s.SetBalance(address2, balance2)
		s.SetBalance(address3, balance3)
	})
}

func TestNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetNonce(address1, nonce1)
	})
}

func TestMultipleNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetNonce(address1, nonce1)
		s.SetNonce(address2, nonce2)
		s.SetNonce(address3, nonce3)
	})
}

func TestCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetCode(address1, []byte{1})
	})
}

func TestMultipleCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetCode(address1, []byte{1})
		s.SetCode(address2, []byte{1, 2})
		s.SetCode(address3, []byte{1, 2, 3})
	})
}

func TestLargeStateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		for i := 0; i < 100; i++ {
			address := common.Address{byte(i)}
			s.CreateAccount(address)
			for j := 0; j < 100; j++ {
				key := common.Key{byte(j)}
				s.SetStorage(address, key, common.Value{byte(i), 0, 0, byte(j)})
			}
			if i%21 == 0 {
				s.DeleteAccount(address)
			}
			s.SetBalance(address, common.Balance{byte(i)})
			s.SetNonce(address, common.Nonce{byte(i + 1)})
			s.SetCode(address, []byte{byte(i), byte(i * 2), byte(i*3 + 2)})
		}
	})
}

func TestCanComputeNonEmptyMemoryFootprint(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s State) {
		fp := s.GetMemoryFootprint()
		if fp == nil {
			t.Fatalf("state produces invalid footprint: %v", fp)
		}
		if fp.Total() <= 0 {
			t.Errorf("memory footprint should not be empty")
		}
		if _, err := fp.ToString("top"); err != nil {
			t.Errorf("Unable to print footprint: %v", err)
		}
	})
}

func TestCodeHashesMatchCodes(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s State) {
		hashOfEmptyCode := common.GetKeccak256Hash([]byte{})

		// For a non-existing account the code is empty and the hash should match.
		hash, err := s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfEmptyCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfEmptyCode, hash)
		}

		// Creating an account should not change this.
		s.CreateAccount(address1)
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfEmptyCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfEmptyCode, hash)
		}

		// Update code to non-empty code updates hash accordingly.
		code := []byte{1, 2, 3, 4}
		hashOfTestCode := common.GetKeccak256Hash(code)
		s.SetCode(address1, code)
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfTestCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfTestCode, hash)
		}

		// Reset code to empty code updates hash accordingly.
		s.SetCode(address1, []byte{})
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfEmptyCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfEmptyCode, hash)
		}
	})
}

func TestDeleteNotExistingAccount(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s State) {
		if err := s.CreateAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}
		if err := s.DeleteAccount(address2); err != nil { // deleting never-existed account
			t.Fatalf("Error: %s", err)
		}

		if newState, err := s.GetAccountState(address1); err != nil || newState != common.Exists {
			t.Errorf("Unrelated existing state: %d, Error: %s", newState, err)
		}
		if newState, err := s.GetAccountState(address2); err != nil || newState != common.Unknown {
			t.Errorf("Delete never-existing state: %d, Error: %s", newState, err)
		}
	})
}
