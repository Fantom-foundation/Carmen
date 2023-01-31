package state

import (
	"bytes"
	"flag"
	"os/exec"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type namedStateConfig struct {
	name    string
	factory func(params Parameters) (directUpdateState, error)
}

func (c *namedStateConfig) createState(directory string) (directUpdateState, error) {
	return c.factory(Parameters{Directory: directory})
}

func (c *namedStateConfig) createStateWithArchive(directory string) (directUpdateState, error) {
	return c.factory(Parameters{Directory: directory, WithArchive: true})
}

func castToDirectUpdateState(factory func(params Parameters) (State, error)) func(params Parameters) (directUpdateState, error) {
	return func(params Parameters) (directUpdateState, error) {
		state, err := factory(params)
		if err != nil {
			return nil, err
		}
		return state.(directUpdateState), nil
	}
}

func initStates() []namedStateConfig {
	var res []namedStateConfig
	for _, s := range initCppStates() {
		res = append(res, namedStateConfig{name: "cpp-" + s.name, factory: s.factory})
	}
	for _, s := range initGoStates() {
		res = append(res, namedStateConfig{name: "go-" + s.name, factory: s.factory})
	}
	return res
}

func testEachConfiguration(t *testing.T, test func(t *testing.T, s directUpdateState)) {
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

func testHashAfterModification(t *testing.T, mod func(s directUpdateState)) {
	ref, err := NewGoMemoryState(Parameters{})
	if err != nil {
		t.Fatalf("failed to create reference state: %v", err)
	}
	mod(ref.(directUpdateState))
	want, err := ref.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash of reference state: %v", err)
	}
	testEachConfiguration(t, func(t *testing.T, state directUpdateState) {
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
	testHashAfterModification(t, func(s directUpdateState) {
		// nothing
	})
}

func TestAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.createAccount(address1)
	})
}

func TestMultipleAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.createAccount(address1)
		s.createAccount(address2)
		s.createAccount(address3)
	})
}

func TestDeletedAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.createAccount(address1)
		s.createAccount(address2)
		s.createAccount(address3)
		s.deleteAccount(address1)
		s.deleteAccount(address2)
	})
}

func TestStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setStorage(address1, key2, val3)
	})
}

func TestMultipleStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setStorage(address1, key2, val3)
		s.setStorage(address2, key3, val1)
		s.setStorage(address3, key1, val2)
	})
}

func TestBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setBalance(address1, balance1)
	})
}

func TestMultipleBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setBalance(address1, balance1)
		s.setBalance(address2, balance2)
		s.setBalance(address3, balance3)
	})
}

func TestNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setNonce(address1, nonce1)
	})
}

func TestMultipleNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setNonce(address1, nonce1)
		s.setNonce(address2, nonce2)
		s.setNonce(address3, nonce3)
	})
}

func TestCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setCode(address1, []byte{1})
	})
}

func TestMultipleCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.setCode(address1, []byte{1})
		s.setCode(address2, []byte{1, 2})
		s.setCode(address3, []byte{1, 2, 3})
	})
}

func TestLargeStateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		for i := 0; i < 100; i++ {
			address := common.Address{byte(i)}
			s.createAccount(address)
			for j := 0; j < 100; j++ {
				key := common.Key{byte(j)}
				s.setStorage(address, key, common.Value{byte(i), 0, 0, byte(j)})
			}
			if i%21 == 0 {
				s.deleteAccount(address)
			}
			s.setBalance(address, common.Balance{byte(i)})
			s.setNonce(address, common.Nonce{byte(i + 1)})
			s.setCode(address, []byte{byte(i), byte(i * 2), byte(i*3 + 2)})
		}
	})
}

func TestCanComputeNonEmptyMemoryFootprint(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s directUpdateState) {
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
	testEachConfiguration(t, func(t *testing.T, s directUpdateState) {
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
		s.createAccount(address1)
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
		s.setCode(address1, code)
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfTestCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfTestCode, hash)
		}

		// Reset code to empty code updates hash accordingly.
		s.setCode(address1, []byte{})
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
	testEachConfiguration(t, func(t *testing.T, s directUpdateState) {
		if err := s.createAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}
		if err := s.deleteAccount(address2); err != nil { // deleting never-existed account
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

func TestCreatingAccountClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s directUpdateState) {
		zero := common.Value{}
		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != zero {
			t.Errorf("storage slot are initially not zero")
		}

		if err = s.setStorage(address1, key1, val1); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err = s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.createAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}

		val, err = s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != zero {
			t.Errorf("account creation did not clear storage slots")
		}
	})
}

func TestDeleteAccountClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, s directUpdateState) {
		zero := common.Value{}

		if err := s.setStorage(address1, key1, val1); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.deleteAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}

		val, err = s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != zero {
			t.Errorf("account deletion did not clear storage slots")
		}
	})
}

// TestArchive inserts data into the state and tries to obtain the history from the archive.
func TestArchive(t *testing.T) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {

			// skip in-memory (we don't have an in-memory archive implementation)
			if config.name == "cpp-InMemory" || config.name == "go-Memory" {
				return
			}

			dir := t.TempDir()
			s, err := config.createStateWithArchive(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer s.Close()

			if err := s.Apply(1, common.Update{
				CreatedAccounts: []common.Address{address1},
				Balances: []common.BalanceUpdate{
					{address1, common.Balance{0x12}},
				},
				Codes:  nil,
				Nonces: nil,
				Slots: []common.SlotUpdate{
					{address1, common.Key{0x05}, common.Value{0x47}},
				},
			}); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := s.Apply(2, common.Update{
				Balances: []common.BalanceUpdate{
					{address1, common.Balance{0x34}},
				},
				Codes: []common.CodeUpdate{
					{address1, []byte{0x12, 0x23}},
				},
				Nonces: []common.NonceUpdate{
					{address1, common.Nonce{0x54}},
				},
				Slots: []common.SlotUpdate{
					{address1, common.Key{0x05}, common.Value{0x89}},
				},
			}); err != nil {
				t.Fatalf("failed to add block 5; %s", err)
			}

			// TODO check data in the archive (when an interface to access the archive will be available)
		})
	}
}

// TestPersistentState inserts data into the state and closes it first, then the state
// is re-opened in another process, and it is tested that data are available, i.e. all was successfully persisted
func TestPersistentState(t *testing.T) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {

			// skip in-memory
			if config.name == "cpp-InMemory" || config.name == "go-Memory" {
				return
			}

			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}

			// init state data
			if err := s.createAccount(address1); err != nil {
				t.Errorf("Error to init state: %v", err)
			}
			if err := s.setBalance(address1, balance1); err != nil {
				t.Errorf("Error to init state: %v", err)
			}
			if err := s.setNonce(address1, nonce1); err != nil {
				t.Errorf("Error to init state: %v", err)
			}
			if err := s.setStorage(address1, key1, val1); err != nil {
				t.Errorf("Error to init state: %v", err)
			}
			if err := s.setCode(address1, []byte{1, 2, 3}); err != nil {
				t.Errorf("Error to init state: %v", err)
			}

			if err := s.Close(); err != nil {
				t.Errorf("Cannot close state: %e", err)
			}

			execSubProcessTest(t, dir, config.name, "TestStateRead")
		})
	}
}

var stateDir = flag.String("statedir", "DEFAULT", "directory where the state is persisted")
var stateImpl = flag.String("stateimpl", "DEFAULT", "name of the state implementation")

// TestReadState verifies data are available in a state.
// The given state reads the data from the given directory and verifies the data are present.
// Name of the index and directory is provided as command line arguments
func TestStateRead(t *testing.T) {
	// do not runt this test stand-alone
	if *stateDir == "DEFAULT" {
		return
	}

	s := createState(t, *stateImpl, *stateDir)
	defer func() {
		_ = s.Close()
	}()

	if state, err := s.GetAccountState(address1); err != nil || state != common.Exists {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", state, common.Exists, err)
	}
	if balance, err := s.GetBalance(address1); err != nil || balance != balance1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", balance, balance1, err)
	}
	if nonce, err := s.GetNonce(address1); err != nil || nonce != nonce1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", nonce, nonce1, err)
	}
	if storage, err := s.GetStorage(address1, key1); err != nil || storage != val1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", storage, val1, err)
	}
	if code, err := s.GetCode(address1); err != nil || bytes.Compare(code, []byte{1, 2, 3}) != 0 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", code, []byte{1, 2, 3}, err)
	}
}

func execSubProcessTest(t *testing.T, dir, stateImpl, execTestName string) {
	cmd := exec.Command("go", "test", "-v", "-run", execTestName, "-args", "-statedir="+dir, "-stateimpl="+stateImpl)

	errBuf := new(bytes.Buffer)
	cmd.Stderr = errBuf
	stdBuf := new(bytes.Buffer)
	cmd.Stdout = stdBuf

	if err := cmd.Run(); err != nil {
		t.Errorf("Subprocess finished with error: %v\n stdout:\n%s stderr:\n%s", err, stdBuf.String(), errBuf.String())
	}
}

// createState creates a state with the given name and directory
func createState(t *testing.T, name, dir string) State {
	for _, s := range initStates() {
		if s.name == name {
			state, err := s.createState(dir)
			if err != nil {
				t.Fatalf("Cannot init state: %s, err: %v", name, err)
			}
			return state
		}
	}

	t.Fatalf("State with name %s not found", name)
	return nil
}
