package state

import (
	"bytes"
	"flag"
	"fmt"
	"math/big"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type namedStateConfig struct {
	name    string
	schema  StateSchema
	factory func(params Parameters) (directUpdateState, error)
}

func (c *namedStateConfig) createState(directory string) (directUpdateState, error) {
	return c.factory(Parameters{Directory: directory, Schema: c.schema})
}

func (c *namedStateConfig) createStateWithArchive(directory string, archiveType ArchiveType) (directUpdateState, error) {
	return c.factory(Parameters{Directory: directory, Archive: archiveType, Schema: c.schema})
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
		res = append(res, namedStateConfig{name: fmt.Sprintf("cpp-%s/s%d", s.name, s.schema), schema: s.schema, factory: s.factory})
	}
	for _, s := range initGoStates() {
		res = append(res, namedStateConfig{name: fmt.Sprintf("go-%s/s%d", s.name, s.schema), schema: s.schema, factory: s.factory})
	}
	return res
}

func testEachConfiguration(t *testing.T, test func(t *testing.T, config *namedStateConfig, s directUpdateState)) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			test(t, &config, state)
		})
	}
}

func testHashAfterModification(t *testing.T, mod func(s directUpdateState)) {
	want := map[StateSchema]common.Hash{}
	for _, s := range GetAllSchemas() {
		ref, err := NewCppInMemoryState(Parameters{Directory: t.TempDir(), Schema: s})
		if err != nil {
			t.Fatalf("failed to create reference state: %v", err)
		}
		mod(ref.(directUpdateState))
		hash, err := ref.GetHash()
		if err != nil {
			t.Fatalf("failed to get hash of reference state: %v", err)
		}
		want[s] = hash
	}

	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, state directUpdateState) {
		mod(state)
		got, err := state.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}
		if want[config.schema] != got {
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
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
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

func TestCodeCanBeUpdated(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
		// Initially, the code of an account is empty.
		code, err := s.GetCode(address1)
		if err != nil {
			t.Fatalf("failed to fetch initial code: %v", err)
		}
		if len(code) != 0 {
			t.Errorf("initial code is not empty")
		}
		if size, err := s.GetCodeSize(address1); err != nil || size != 0 {
			t.Errorf("reported code size is not zero")
		}
		expected_hash := common.GetKeccak256Hash([]byte{})
		if hash, err := s.GetCodeHash(address1); err != nil || hash != expected_hash {
			t.Errorf("hash of code does not match, expected %v, got %v", expected_hash, hash)
		}

		// Set the code to a new value.
		code1 := []byte{0, 1, 2, 3, 4}
		if err := s.setCode(address1, code1); err != nil {
			t.Fatalf("failed to update code: %v", err)
		}
		code, err = s.GetCode(address1)
		if err != nil || !bytes.Equal(code, code1) {
			t.Errorf("failed to set code for address")
		}
		if size, err := s.GetCodeSize(address1); err != nil || size != len(code1) {
			t.Errorf("reported code size is not %d, got %d", len(code1), size)
		}
		expected_hash = common.GetKeccak256Hash(code1)
		if hash, err := s.GetCodeHash(address1); err != nil || hash != expected_hash {
			t.Errorf("hash of code does not match, expected %v, got %v", expected_hash, hash)
		}

		// Update code again should be fine.
		code2 := []byte{5, 4, 3, 2, 1}
		if err := s.setCode(address1, code2); err != nil {
			t.Fatalf("failed to update code: %v", err)
		}
		code, err = s.GetCode(address1)
		if err != nil || !bytes.Equal(code, code2) {
			t.Errorf("failed to update code for address")
		}
		if size, err := s.GetCodeSize(address1); err != nil || size != len(code2) {
			t.Errorf("reported code size is not %d, got %d", len(code2), size)
		}
		expected_hash = common.GetKeccak256Hash(code2)
		if hash, err := s.GetCodeHash(address1); err != nil || hash != expected_hash {
			t.Errorf("hash of code does not match, expected %v, got %v", expected_hash, hash)
		}
	})
}

func TestCodeHashesMatchCodes(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
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
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
		if err := s.createAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}
		if err := s.deleteAccount(address2); err != nil { // deleting never-existed account
			t.Fatalf("Error: %s", err)
		}

		if newState, err := s.Exists(address1); err != nil || newState != true {
			t.Errorf("Unrelated existing state: %t, Error: %s", newState, err)
		}
		if newState, err := s.Exists(address2); err != nil || newState != false {
			t.Errorf("Delete never-existing state: %t, Error: %s", newState, err)
		}
	})
}

func TestCreatingAccountClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
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
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
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
		for _, archiveType := range []ArchiveType{LevelDbArchive, SqliteArchive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					t.Fatalf("failed to initialize state %s; %s", config.name, err)
				}
				defer s.Close()

				balance12, _ := common.ToBalance(big.NewInt(0x12))
				balance34, _ := common.ToBalance(big.NewInt(0x34))

				if err := s.Apply(1, common.Update{
					CreatedAccounts: []common.Address{address1},
					Balances: []common.BalanceUpdate{
						{address1, balance12},
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
						{address1, balance34},
						{address2, balance12},
						{address3, balance12},
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
					t.Fatalf("failed to add block 2; %s", err)
				}

				if err := s.Flush(); err != nil {
					t.Fatalf("failed to flush updates, %s", err)
				}

				state1, err := s.GetArchiveState(1)
				if err != nil {
					t.Fatalf("failed to get state of block 1; %s", err)
				}

				state2, err := s.GetArchiveState(2)
				if err != nil {
					t.Fatalf("failed to get state of block 2; %s", err)
				}

				if as, err := state1.Exists(address1); err != nil || as != true {
					t.Errorf("invalid account state at block 1: %t, %s", as, err)
				}
				if as, err := state2.Exists(address1); err != nil || as != true {
					t.Errorf("invalid account state at block 2: %t, %s", as, err)
				}
				if balance, err := state1.GetBalance(address1); err != nil || balance != balance12 {
					t.Errorf("invalid balance at block 1: %s, %s", balance.ToBigInt(), err)
				}
				if balance, err := state2.GetBalance(address1); err != nil || balance != balance34 {
					t.Errorf("invalid balance at block 2: %s, %s", balance.ToBigInt(), err)
				}
				if code, err := state1.GetCode(address1); err != nil || code != nil {
					t.Errorf("invalid code at block 1: %s, %s", code, err)
				}
				if code, err := state2.GetCode(address1); err != nil || !bytes.Equal(code, []byte{0x12, 0x23}) {
					t.Errorf("invalid code at block 2: %s, %s", code, err)
				}
				if nonce, err := state1.GetNonce(address1); err != nil || nonce != (common.Nonce{}) {
					t.Errorf("invalid nonce at block 1: %s, %s", nonce, err)
				}
				if nonce, err := state2.GetNonce(address1); err != nil || nonce != (common.Nonce{0x54}) {
					t.Errorf("invalid nonce at block 2: %s, %s", nonce, err)
				}
				if value, err := state1.GetStorage(address1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
					t.Errorf("invalid slot value at block 1: %s, %s", value, err)
				}
				if value, err := state2.GetStorage(address1, common.Key{0x05}); err != nil || value != (common.Value{0x89}) {
					t.Errorf("invalid slot value at block 2: %s, %s", value, err)
				}

				hash1, err := state1.GetHash()
				if err != nil || fmt.Sprintf("%x", hash1) != "69ec5bcbe6fd0da76107d64b6e9589a465ecccf5a90a3cb07de1f9cb91e0a28a" {
					t.Errorf("unexpected archive state hash at block 1: %x, %s", hash1, err)
				}
				hash2, err := state2.GetHash()
				if err != nil || fmt.Sprintf("%x", hash2) != "bfafc906d048e39ab3bdd9cf0732a41ce752ce2f9448757d36cc9eb07dd78f29" {
					t.Errorf("unexpected archive state hash at block 2: %x, %s", hash2, err)
				}
			})
		}
	}
}

// TestPersistentState inserts data into the state and closes it first, then the state
// is re-opened in another process, and it is tested that data are available, i.e. all was successfully persisted
func TestPersistentState(t *testing.T) {
	for _, config := range initStates() {
		// skip in-memory
		if strings.HasPrefix(config.name, "cpp-memory") || strings.HasPrefix(config.name, "go-Memory") {
			continue
		}
		for _, archiveType := range []ArchiveType{LevelDbArchive, SqliteArchive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {

				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					t.Fatalf("failed to initialize state %s", t.Name())
				}

				// init state data
				update := common.Update{}
				update.AppendCreateAccount(address1)
				update.AppendBalanceUpdate(address1, balance1)
				update.AppendNonceUpdate(address1, nonce1)
				update.AppendSlotUpdate(address1, key1, val1)
				update.AppendCodeUpdate(address1, []byte{1, 2, 3})
				if err := s.Apply(1, update); err != nil {
					t.Errorf("Error to init state: %v", err)
				}

				if err := s.Close(); err != nil {
					t.Errorf("Cannot close state: %e", err)
				}

				execSubProcessTest(t, dir, config.name, archiveType, "TestStateRead")
			})
		}
	}
}

var stateDir = flag.String("statedir", "DEFAULT", "directory where the state is persisted")
var stateImpl = flag.String("stateimpl", "DEFAULT", "name of the state implementation")
var archiveImpl = flag.Int("archiveimpl", 0, "number of the archive implementation")

// TestReadState verifies data are available in a state.
// The given state reads the data from the given directory and verifies the data are present.
// Name of the index and directory is provided as command line arguments
func TestStateRead(t *testing.T) {
	// do not runt this test stand-alone
	if *stateDir == "DEFAULT" {
		return
	}

	s := createState(t, *stateImpl, *stateDir, *archiveImpl)
	defer func() {
		_ = s.Close()
	}()

	if state, err := s.Exists(address1); err != nil || state != true {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", state, true, err)
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

	as, err := s.GetArchiveState(1)
	if as == nil || err != nil {
		t.Fatalf("Unable to get archive state, err: %v", err)
	}
	if state, err := as.Exists(address1); err != nil || state != true {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", state, true, err)
	}
	if balance, err := as.GetBalance(address1); err != nil || balance != balance1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", balance, balance1, err)
	}
	if nonce, err := as.GetNonce(address1); err != nil || nonce != nonce1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", nonce, nonce1, err)
	}
	if storage, err := as.GetStorage(address1, key1); err != nil || storage != val1 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", storage, val1, err)
	}
	if code, err := as.GetCode(address1); err != nil || bytes.Compare(code, []byte{1, 2, 3}) != 0 {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", code, []byte{1, 2, 3}, err)
	}
}

func execSubProcessTest(t *testing.T, dir string, stateImpl string, archiveImpl ArchiveType, execTestName string) {
	cmd := exec.Command("go", "test", "-v", "-run", execTestName, "-args", "-statedir="+dir, "-stateimpl="+stateImpl, "-archiveimpl="+strconv.FormatInt(int64(archiveImpl), 10))

	errBuf := new(bytes.Buffer)
	cmd.Stderr = errBuf
	stdBuf := new(bytes.Buffer)
	cmd.Stdout = stdBuf

	if err := cmd.Run(); err != nil {
		t.Errorf("Subprocess finished with error: %v\n stdout:\n%s stderr:\n%s", err, stdBuf.String(), errBuf.String())
	}
}

// createState creates a state with the given name and directory
func createState(t *testing.T, name, dir string, archiveImpl int) State {
	for _, config := range initStates() {
		if config.name == name {
			state, err := config.createStateWithArchive(dir, ArchiveType(archiveImpl))
			if err != nil {
				t.Fatalf("Cannot init state: %s, err: %v", name, err)
			}
			return state
		}
	}

	t.Fatalf("State with name %s not found", name)
	return nil
}
