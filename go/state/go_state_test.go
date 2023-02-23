package state

import (
	"bytes"
	"fmt"
	"strings"
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

func initGoStates() []namedStateConfig {
	return []namedStateConfig{
		{"Memory", 1, castToDirectUpdateState(NewGoMemoryState)},
		{"File Index and Store", 1, castToDirectUpdateState(NewGoFileState)},
		{"Cached File Index and Store", 1, castToDirectUpdateState(NewGoCachedFileState)},
		{"LevelDB Index and Store", 1, castToDirectUpdateState(NewGoLeveLIndexAndStoreState)},
		{"Cached LevelDB Index and Store", 1, castToDirectUpdateState(NewGoCachedLeveLIndexAndStoreState)},
	}
}

func TestMissingKeys(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			accountState, err := state.Exists(address1)
			if err != nil || accountState != false {
				t.Errorf("Account must not exist in the initial state, but it exists. err: %s", err)
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
			size, err := state.GetCodeSize(address1)
			if err != nil || size != 0 {
				t.Errorf("Value must be 0. It is: %d, err: %s", value, err)
			}
		})
	}
}

func TestBasicOperations(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			// fill-in values
			if err := state.createAccount(address1); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.setNonce(address1, common.Nonce{123}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.setBalance(address2, common.Balance{45}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.setStorage(address3, key1, common.Value{67}); err != nil {
				t.Errorf("Error: %s", err)
			}
			if err := state.setCode(address1, []byte{0x12, 0x34}); err != nil {
				t.Errorf("Error: %s", err)
			}

			// fetch values
			if val, err := state.Exists(address1); err != nil || val != true {
				t.Errorf("Created account does not exists: Val: %t, Err: %s", val, err)
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
			if val, err := state.GetCodeSize(address1); err != nil || val != 2 {
				t.Errorf("Invalid code size or error returned: Val: %d, Err: %s", val, err)
			}

			// delete account
			if err := state.deleteAccount(address1); err != nil {
				t.Errorf("Error: %s", err)
			}
			if val, err := state.Exists(address1); err != nil || val != false {
				t.Errorf("Deleted account is not deleted: Val: %t, Err: %s", val, err)
			}

			// fetch wrong combinations
			if val, err := state.GetStorage(address1, key1); (err != nil || val != common.Value{}) {
				t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
			}
		})
	}
}

func TestMoreInserts(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			// insert more combinations, so we do not have only zero-indexes everywhere
			_ = state.setStorage(address1, key1, val1)
			_ = state.setStorage(address1, key2, val2)
			_ = state.setStorage(address1, key3, val3)

			_ = state.setStorage(address2, key1, val1)
			_ = state.setStorage(address2, key2, val2)
			_ = state.setStorage(address2, key3, val3)

			_ = state.setStorage(address3, key1, val1)
			_ = state.setStorage(address3, key2, val2)
			_ = state.setStorage(address3, key3, val3)

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
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			initialHash, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}

			_ = state.setStorage(address1, key1, val1)
			hash1, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash1 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.setBalance(address1, balance1)
			hash2, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash2 || hash1 == hash2 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.createAccount(address1)
			hash3, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %s", err)
			}
			if initialHash == hash3 || hash1 == hash3 || hash2 == hash3 {
				t.Errorf("hash of changed state not changed; %s", err)
			}

			_ = state.setCode(address1, []byte{0x12, 0x34, 0x56, 0x78})
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
	state, err := NewGoMemoryState(Parameters{Schema: 1})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goState := state.(*GoState)
	goState.balancesStore = failingStore[uint32, common.Balance]{goState.balancesStore}
	goState.noncesStore = failingStore[uint32, common.Nonce]{goState.noncesStore}
	goState.valuesStore = failingStore[uint32, common.Value]{goState.valuesStore}

	_ = goState.setBalance(address1, common.Balance{})
	_ = goState.setNonce(address1, common.Nonce{})
	_ = goState.setStorage(address1, key1, common.Value{})

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
	state, err := NewGoMemoryState(Parameters{Schema: 1})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goState := state.(*GoState)
	goState.addressIndex = failingIndex[common.Address, uint32]{goState.addressIndex}

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

func TestGetMemoryFootprint(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			memoryFootprint := state.(*GoState).GetMemoryFootprint()
			str, err := memoryFootprint.ToString("state")
			if err != nil {
				t.Fatalf("failed to get state memory footprint; %s", err)
			}
			if !strings.Contains(str, "hashTree") {
				t.Errorf("memory footprint string does not contain any hashTree")
			}
		})
	}
}
