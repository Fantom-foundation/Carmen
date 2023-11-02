package state

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

type namedStateConfig struct {
	name    string
	schema  StateSchema
	variant Variant
}

func (c *namedStateConfig) createState(directory string) (directUpdateState, error) {
	st, err := NewState(Parameters{Directory: directory, Variant: c.variant, Schema: c.schema})
	if err != nil {
		return nil, err
	}
	return st.(directUpdateState), nil
}

func (c *namedStateConfig) createStateWithArchive(directory string, archiveType ArchiveType) (directUpdateState, error) {
	st, err := NewState(Parameters{Directory: directory, Variant: c.variant, Schema: c.schema, Archive: archiveType})
	if err != nil {
		return nil, err
	}
	return st.(directUpdateState), nil
}

func initStates() []namedStateConfig {
	var res []namedStateConfig
	for _, s := range initCppStates() {
		res = append(res, namedStateConfig{name: fmt.Sprintf("cpp-%s/s%d", s.name, s.schema), schema: s.schema, variant: s.variant})
	}
	for _, s := range initGoStates() {
		res = append(res, namedStateConfig{name: fmt.Sprintf("go-%s/s%d", s.name, s.schema), schema: s.schema, variant: s.variant})
	}
	return res
}

func testEachConfiguration(t *testing.T, test func(t *testing.T, config *namedStateConfig, s directUpdateState)) {
	for _, config := range initStates() {
		config := config
		t.Run(config.name, func(t *testing.T) {
			t.Parallel()
			state, err := config.createState(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s: %v", config.name, err)
				} else {
					t.Fatalf("failed to initialize state %s: %v", config.name, err)
				}
			}
			defer state.Close()

			test(t, &config, state)
		})
	}
}

func getReferenceStateFor(params Parameters) (State, error) {
	if params.Schema == 4 {
		return newGoMemoryS4State(params)
	}
	if params.Schema == 5 {
		return newGoMemoryS5State(params)
	}
	return newGoMemoryState(params)
}

func testHashAfterModification(t *testing.T, mod func(s directUpdateState)) {
	want := map[StateSchema]common.Hash{}
	for _, s := range GetAllSchemas() {
		ref, err := getReferenceStateFor(Parameters{Directory: t.TempDir(), Schema: s})
		if err != nil {
			t.Fatalf("failed to create reference state: %v", err)
		}
		defer ref.Close()
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
			t.Errorf("Invalid hash, wanted %v, got %v", want[config.schema], got)
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
		s.CreateAccount(address1)
	})
}

func TestMultipleAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestDeletedAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.DeleteAccount(address1)
		s.DeleteAccount(address2)
	})
}

func TestStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetStorage(address1, key2, val3)
	})
}

func TestMultipleStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetStorage(address1, key2, val3)
		s.SetStorage(address2, key3, val1)
		s.SetStorage(address3, key1, val2)
	})
}

func TestBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetBalance(address1, balance1)
	})
}

func TestMultipleBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetBalance(address1, balance1)
		s.SetBalance(address2, balance2)
		s.SetBalance(address3, balance3)
	})
}

func TestNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetNonce(address1, nonce1)
	})
}

func TestMultipleNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetNonce(address1, nonce1)
		s.SetNonce(address2, nonce2)
		s.SetNonce(address3, nonce3)
	})
}

func TestCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetCode(address1, []byte{1})
	})
}

func TestMultipleCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
		s.SetCode(address1, []byte{1})
		s.SetCode(address2, []byte{1, 2})
		s.SetCode(address3, []byte{1, 2, 3})
	})
}

func TestLargeStateHashes(t *testing.T) {
	testHashAfterModification(t, func(s directUpdateState) {
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
		if err := s.SetCode(address1, code1); err != nil {
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
		if err := s.SetCode(address1, code2); err != nil {
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
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
		if err := s.CreateAccount(address1); err != nil {
			t.Fatalf("Error: %s", err)
		}
		if err := s.DeleteAccount(address2); err != nil { // deleting never-existed account
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
		if err := s.CreateAccount(address1); err != nil {
			t.Errorf("failed to create account: %v", err)
		}

		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != zero {
			t.Errorf("storage slot are initially not zero")
		}

		if err = s.SetStorage(address1, key1, val1); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err = s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.CreateAccount(address1); err != nil {
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

func TestDeletingAccountsClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedStateConfig, s directUpdateState) {
		zero := common.Value{}
		if err := s.CreateAccount(address1); err != nil {
			t.Errorf("failed to create account: %v", err)
		}

		if err := s.SetStorage(address1, key1, val1); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.DeleteAccount(address1); err != nil {
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
		for _, archiveType := range allArchiveTypes {
			if archiveType == NoArchive {
				continue
			}
			config := config
			archiveType := archiveType
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					if errors.Is(err, UnsupportedConfiguration) {
						t.Skipf("unsupported state %s; %s", config.name, err)
					} else {
						t.Fatalf("failed to initialize state %s; %s", config.name, err)
					}
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
					t.Fatalf("failed to add block 2; %v", err)
				}

				if err := s.Flush(); err != nil {
					t.Fatalf("failed to flush updates, %v", err)
				}

				state1, err := s.GetArchiveState(1)
				if err != nil {
					t.Fatalf("failed to get state of block 1; %v", err)
				}

				state2, err := s.GetArchiveState(2)
				if err != nil {
					t.Fatalf("failed to get state of block 2; %v", err)
				}

				if as, err := state1.Exists(address1); err != nil || as != true {
					t.Errorf("invalid account state at block 1: %t, %v", as, err)
				}
				if as, err := state2.Exists(address1); err != nil || as != true {
					t.Errorf("invalid account state at block 2: %t, %v", as, err)
				}
				if balance, err := state1.GetBalance(address1); err != nil || balance != balance12 {
					t.Errorf("invalid balance at block 1: %v, %v", balance.ToBigInt(), err)
				}
				if balance, err := state2.GetBalance(address1); err != nil || balance != balance34 {
					t.Errorf("invalid balance at block 2: %v, %v", balance.ToBigInt(), err)
				}
				if code, err := state1.GetCode(address1); err != nil || code != nil {
					t.Errorf("invalid code at block 1: %v, %v", code, err)
				}
				if code, err := state2.GetCode(address1); err != nil || !bytes.Equal(code, []byte{0x12, 0x23}) {
					t.Errorf("invalid code at block 2: %v, %v", code, err)
				}
				if nonce, err := state1.GetNonce(address1); err != nil || nonce != (common.Nonce{}) {
					t.Errorf("invalid nonce at block 1: %v, %v", nonce, err)
				}
				if nonce, err := state2.GetNonce(address1); err != nil || nonce != (common.Nonce{0x54}) {
					t.Errorf("invalid nonce at block 2: %v, %v", nonce, err)
				}
				if value, err := state1.GetStorage(address1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
					t.Errorf("invalid slot value at block 1: %v, %v", value, err)
				}
				if value, err := state2.GetStorage(address1, common.Key{0x05}); err != nil || value != (common.Value{0x89}) {
					t.Errorf("invalid slot value at block 2: %v, %v", value, err)
				}

				if archiveType != S4Archive && archiveType != S5Archive {
					hash1, err := state1.GetHash()
					if err != nil || fmt.Sprintf("%x", hash1) != "69ec5bcbe6fd0da76107d64b6e9589a465ecccf5a90a3cb07de1f9cb91e0a28a" {
						t.Errorf("unexpected archive state hash at block 1: %x, %s", hash1, err)
					}
					hash2, err := state2.GetHash()
					if err != nil || fmt.Sprintf("%x", hash2) != "bfafc906d048e39ab3bdd9cf0732a41ce752ce2f9448757d36cc9eb07dd78f29" {
						t.Errorf("unexpected archive state hash at block 2: %x, %s", hash2, err)
					}
				}
			})
		}
	}
}

// TestLastArchiveBlock tests obtaining the state at the last (highest) block in the archive.
func TestLastArchiveBlock(t *testing.T) {
	for _, config := range initStates() {
		for _, archiveType := range allArchiveTypes {
			if archiveType == NoArchive {
				continue
			}
			config := config
			archiveType := archiveType

			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				if config.name[0:3] == "cpp" {
					t.Skipf("GetArchiveBlockHeight not supported by the cpp state")
				}
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					if errors.Is(err, UnsupportedConfiguration) {
						t.Skipf("unsupported state %s; %s", config.name, err)
					} else {
						t.Fatalf("failed to initialize state %s; %s", config.name, err)
					}
				}
				defer s.Close()

				_, empty, err := s.GetArchiveBlockHeight()
				if err != nil {
					t.Fatalf("obtaining the last block from an empty archive failed: %v", err)
				}
				if !empty {
					t.Fatalf("empty archive is not reporting lack of blocks")
				}

				if err := s.Apply(1, common.Update{
					CreatedAccounts: []common.Address{address1},
				}); err != nil {
					t.Fatalf("failed to add block 1; %s", err)
				}

				if err := s.Apply(2, common.Update{
					CreatedAccounts: []common.Address{address2},
				}); err != nil {
					t.Fatalf("failed to add block 2; %s", err)
				}

				if err := s.Flush(); err != nil {
					t.Fatalf("failed to flush updates, %s", err)
				}

				lastBlockHeight, empty, err := s.GetArchiveBlockHeight()
				if err != nil {
					t.Fatalf("failed to get the last available block height; %s", err)
				}
				if empty || lastBlockHeight != 2 {
					t.Errorf("invalid last available block height %d (expected 2); empty: %t", lastBlockHeight, empty)
				}

				state2, err := s.GetArchiveState(lastBlockHeight)
				if err != nil {
					t.Fatalf("failed to get state at the last block in the archive; %s", err)
				}

				if as, err := state2.Exists(address1); err != nil || as != true {
					t.Errorf("invalid account state at the last block: %t, %s", as, err)
				}
				if as, err := state2.Exists(address2); err != nil || as != true {
					t.Errorf("invalid account state at the last block: %t, %s", as, err)
				}

				_, err = s.GetArchiveState(lastBlockHeight + 1)
				if err == nil {
					t.Errorf("obtainig a block higher than the last one (%d) did not failed", lastBlockHeight)
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
		for _, archiveType := range allArchiveTypes {
			if archiveType == NoArchive {
				continue
			}
			archiveType := archiveType
			config := config
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				t.Parallel()

				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					if errors.Is(err, UnsupportedConfiguration) {
						t.Skipf("unsupported state %s; %s", t.Name(), err)
					} else {
						t.Fatalf("failed to initialize state %s; %s", t.Name(), err)
					}
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

func fillStateForSnapshotting(state directUpdateState) {
	state.SetBalance(address1, common.Balance{12})
	state.SetNonce(address2, common.Nonce{14})
	state.SetCode(address3, []byte{0, 8, 15})
	state.SetStorage(address1, key1, val1)
}

func TestSnapshotCanBeCreatedAndRestored(t *testing.T) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {
			original, err := config.createState(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s; %s", config.name, err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name, err)
				}
			}
			defer original.Close()

			fillStateForSnapshotting(original)

			snapshot, err := original.CreateSnapshot()
			if err == backend.ErrSnapshotNotSupported {
				t.Skipf("configuration '%v' skipped since snapshotting is not supported", config.name)
			}
			if err != nil {
				t.Errorf("failed to create snapshot: %v", err)
				return
			}

			recovered, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer recovered.Close()

			if err := recovered.(backend.Snapshotable).Restore(snapshot.GetData()); err != nil {
				t.Errorf("failed to sync to snapshot: %v", err)
				return
			}

			if got, err := recovered.GetBalance(address1); err != nil || got != (common.Balance{12}) {
				if err != nil {
					t.Errorf("failed to fetch balance for account %v: %v", address1, err)
				} else {
					t.Errorf("failed to recover balance for account %v - wanted %v, got %v", address1, (common.Balance{12}), got)
				}
			}

			if got, err := recovered.GetNonce(address2); err != nil || got != (common.Nonce{14}) {
				if err != nil {
					t.Errorf("failed to fetch nonce for account %v: %v", address1, err)
				} else {
					t.Errorf("failed to recover nonce for account %v - wanted %v, got %v", address1, (common.Nonce{14}), got)
				}
			}

			code := []byte{0, 8, 15}
			if got, err := recovered.GetCode(address3); err != nil || !bytes.Equal(got, code) {
				if err != nil {
					t.Errorf("failed to fetch code for account %v: %v", address1, err)
				} else {
					t.Errorf("failed to recover code for account %v - wanted %v, got %v", address1, code, got)
				}
			}

			codeHash := common.GetHash(sha3.NewLegacyKeccak256(), code)
			if got, err := recovered.GetCodeHash(address3); err != nil || got != codeHash {
				if err != nil {
					t.Errorf("failed to fetch code hash for account %v: %v", address1, err)
				} else {
					t.Errorf("failed to recover code hash for account %v - wanted %v, got %v", address1, codeHash, got)
				}
			}

			if got, err := recovered.GetStorage(address1, key1); err != nil || got != val1 {
				if err != nil {
					t.Errorf("failed to fetch storage for account %v: %v", address1, err)
				} else {
					t.Errorf("failed to recover storage for account %v - wanted %v, got %v", address1, val1, got)
				}
			}

			want, err := original.GetHash()
			if err != nil {
				t.Errorf("failed to fetch hash for state: %v", err)
			}

			got, err := recovered.GetHash()
			if err != nil {
				t.Errorf("failed to fetch hash for state: %v", err)
			}

			if want != got {
				t.Errorf("hash of recovered state does not match source hash: %v vs %v", got, want)
			}

			if err := snapshot.Release(); err != nil {
				t.Errorf("failed to release snapshot: %v", err)
			}
		})
	}
}

func TestSnapshotCanBeCreatedAndVerified(t *testing.T) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {
			original, err := config.createState(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s; %s", config.name, err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name, err)
				}
			}
			defer original.Close()

			fillStateForSnapshotting(original)

			snapshot, err := original.CreateSnapshot()
			if err == backend.ErrSnapshotNotSupported {
				t.Skipf("configuration '%v' skipped since snapshotting is not supported", config.name)
			}
			if err != nil {
				t.Errorf("failed to create snapshot: %v", err)
				return
			}

			// The root proof should be equivalent.
			want, err := original.GetProof()
			if err != nil {
				t.Errorf("failed to get root proof from data structure")
			}

			have := snapshot.GetRootProof()
			if !want.Equal(have) {
				t.Errorf("root proof of snapshot does not match proof of data structure")
			}

			metadata, err := snapshot.GetData().GetMetaData()
			if err != nil {
				t.Fatalf("failed to obtain metadata from snapshot")
			}

			verifier, err := original.GetSnapshotVerifier(metadata)
			if err != nil {
				t.Fatalf("failed to obtain snapshot verifier")
			}

			if proof, err := verifier.VerifyRootProof(snapshot.GetData()); err != nil || !proof.Equal(want) {
				t.Errorf("snapshot invalid, inconsistent proofs: %v, want %v, got %v", err, want, proof)
			}

			// Verify all pages
			for i := 0; i < snapshot.GetNumParts(); i++ {
				want, err := snapshot.GetProof(i)
				if err != nil {
					t.Errorf("failed to fetch proof of part %d", i)
				}
				part, err := snapshot.GetPart(i)
				if err != nil || part == nil {
					t.Errorf("failed to fetch part %d", i)
				}
				if part != nil && verifier.VerifyPart(i, want.ToBytes(), part.ToBytes()) != nil {
					t.Errorf("failed to verify content of part %d", i)
				}
			}

			if err := snapshot.Release(); err != nil {
				t.Errorf("failed to release snapshot: %v", err)
			}
		})
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
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to resolve path to test binary: %v", err)
	}

	cmd := exec.Command(path, "-test.run", execTestName, "-statedir="+dir, "-stateimpl="+stateImpl, "-archiveimpl="+strconv.FormatInt(int64(archiveImpl), 10))
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
