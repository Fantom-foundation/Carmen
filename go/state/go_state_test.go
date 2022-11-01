package state

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}

	key1 = common.Key{0x01}
	key2 = common.Key{0x02}
	key3 = common.Key{0x03}

	val0 = common.Value{0x00}
	val1 = common.Value{0x01}
	val2 = common.Value{0x02}
	val3 = common.Value{0x03}

	balance1 = common.Balance{0x01}
	balance2 = common.Balance{0x02}
	balance3 = common.Balance{0x03}

	nonce1 = common.Nonce{0x01}
	nonce2 = common.Nonce{0x02}
	nonce3 = common.Nonce{0x03}
)

func initGoStates(t *testing.T) []namedStateConfig {
	memState, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	ldbFileState, err := NewLeveLIndexFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	cachedLdbFileState, err := NewCachedLeveLIndexFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	cachedTransactLdbFileState, err := NewCachedTransactLeveLIndexFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	ldbState, err := NewLeveLIndexAndStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	cachedLdbState, err := NewCachedLeveLIndexAndStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}
	cachedTransactLdbState, err := NewTransactCachedLeveLIndexAndStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to init state: %v", err)
	}

	t.Cleanup(func() {
		_ = memState.Close()
		_ = ldbFileState.Close()
		_ = cachedLdbFileState.Close()
		_ = cachedTransactLdbFileState.Close()
		_ = ldbState.Close()
		_ = cachedLdbState.Close()
		_ = cachedTransactLdbState.Close()
	})

	return []namedStateConfig{
		{"Memory", memState},
		{"LevelDB Index, File Store", ldbFileState},
		{"Cached LevelDB Index, File Store", cachedLdbFileState},
		{"Cached Transact LevelDB, Index File Store", cachedTransactLdbFileState},
		{"LevelDB Index and Store", ldbState},
		{"Cached LevelDB Index and Store", cachedLdbState},
		//{"Cached Transact LevelDB Index and Store", cachedTransactLdbState},  // cannot combine transact and non-transact access
	}
}

func TestMissingKeys(t *testing.T) {
	for _, config := range initGoStates(t) {
		t.Run(config.name, func(t *testing.T) {
			state := config.state
			accountState, err := state.GetAccountState(address1)
			if err != nil || accountState != common.Unknown {
				t.Errorf("Account state must be Unknown. It is: %s, err: %s", accountState, err)
			}
			balance, err := state.GetBalance(address1)
			if (err != nil || balance != common.Balance{}) {
				t.Errorf("Balance must be empty. It is: %s, err: %s", balance, err)
			}
			nonce, err := state.GetNonce(address1)
			if (err != nil || nonce != common.Nonce{}) {
				t.Errorf("Nonce must be empty. It is: %s, err: %s", nonce, err)
			}
			value, err := state.GetStorage(address1, key1)
			if (err != nil || value != common.Value{}) {
				t.Errorf("Value must be empty. It is: %s, err: %s", value, err)
			}
			code, err := state.GetCode(address1)
			if err != nil || code != nil {
				t.Errorf("Value must be empty. It is: %s, err: %s", value, err)
			}
		})
	}
}

func TestBasicOperations(t *testing.T) {
	for _, config := range initGoStates(t) {
		t.Run(config.name, func(t *testing.T) {
			state := config.state
			// fill-in values
			if err := state.CreateAccount(address1); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.SetNonce(address1, common.Nonce{123}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.SetBalance(address2, common.Balance{45}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.SetStorage(address3, key1, common.Value{67}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.SetCode(address1, []byte{0x12, 0x34}); err != nil {
				t.Errorf("Error: %s", err)
			}

			// fetch values
			if val, err := state.GetAccountState(address1); err != nil || val != common.Exists {
				t.Errorf("Created account does not exists: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetNonce(address1); (err != nil || val != common.Nonce{123}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetBalance(address2); (err != nil || val != common.Balance{45}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetStorage(address3, key1); (err != nil || val != common.Value{67}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetCode(address1); err != nil || !bytes.Equal(val, []byte{0x12, 0x34}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}

			// delete account
			if err := state.DeleteAccount(address1); err != nil {
				t.Errorf("Error: %s", err)
			}
			if val, err := state.GetAccountState(address1); err != nil || val != common.Deleted {
				t.Errorf("Deleted account is not deleted: Val: %s, Err: %s", val, err)
			}

			// fetch wrong combinations
			if val, err := state.GetStorage(address1, key1); (err != nil || val != common.Value{}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
		})
	}
}

func TestMoreInserts(t *testing.T) {
	for _, config := range initGoStates(t) {
		t.Run(config.name, func(t *testing.T) {
			state := config.state
			// insert more combinations, so we do not have only zero-indexes everywhere
			_ = state.SetStorage(address1, key1, val1)
			_ = state.SetStorage(address1, key2, val2)
			_ = state.SetStorage(address1, key3, val3)

			_ = state.SetStorage(address2, key1, val1)
			_ = state.SetStorage(address2, key2, val2)
			_ = state.SetStorage(address2, key3, val3)

			_ = state.SetStorage(address3, key1, val1)
			_ = state.SetStorage(address3, key2, val2)
			_ = state.SetStorage(address3, key3, val3)

			if val, err := state.GetStorage(address1, key3); err != nil || val != val3 {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetStorage(address2, key1); err != nil || val != val1 {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
			if val, err := state.GetStorage(address3, key2); err != nil || val != val2 {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
		})
	}
}

func TestHashing(t *testing.T) {
	var hashes []common.Hash
	for _, config := range initGoStates(t) {
		t.Run(config.name, func(t *testing.T) {
			state := config.state
			initialHash, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}

			_ = state.SetStorage(address1, key1, val1)
			hash1, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash1 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.SetBalance(address1, balance1)
			hash2, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash2 || hash1 == hash2 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.CreateAccount(address1)
			hash3, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash3 || hash1 == hash3 || hash2 == hash3 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.SetCode(address1, []byte{0x12, 0x34, 0x56, 0x78})
			hash4, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash4 || hash3 == hash4 {
				t.Errorf("hash of changed state not changed; %s", err)
			}
			hashes = append(hashes, hash4) // store the last hash
		})
	}

	// check all final hashes are the same
	for i := 0; i < len(hashes)-1; i++ {
		if hashes[i] != hashes[i+1] {
			t.Errorf("hashes differ ")
		}
	}
}

var testingErr = fmt.Errorf("testing error")

type failingStore[I common.Identifier, V any] struct {
	store.Store[I, V]
}

func (m failingStore[I, V]) Get(id I) (value V, err error) {
	err = testingErr
	return
}

type failingIndex[K comparable, I common.Identifier] struct {
	index.Index[K, I]
}

func (m failingIndex[K, I]) Get(key K) (id I, err error) {
	err = testingErr
	return
}

func TestFailingStore(t *testing.T) {
	state, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	state.balancesStore = failingStore[uint32, common.Balance]{state.balancesStore}
	state.noncesStore = failingStore[uint32, common.Nonce]{state.noncesStore}
	state.valuesStore = failingStore[uint32, common.Value]{state.valuesStore}

	_ = state.SetBalance(address1, common.Balance{})
	_ = state.SetNonce(address1, common.Nonce{})
	_ = state.SetStorage(address1, key1, common.Value{})

	_, err = state.GetBalance(address1)
	if err != testingErr {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = state.GetNonce(address1)
	if err != testingErr {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = state.GetStorage(address1, key1)
	if err != testingErr {
		t.Errorf("State service does not return the store err; returned %s", err)
	}
}

func TestFailingIndex(t *testing.T) {
	state, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	state.addressIndex = failingIndex[common.Address, uint32]{state.addressIndex}

	_, err = state.GetBalance(address1)
	if err != testingErr {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = state.GetNonce(address1)
	if err != testingErr {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = state.GetStorage(address1, key1)
	if err != testingErr {
		t.Errorf("State service does not return the index err; returned %s", err)
	}
}
