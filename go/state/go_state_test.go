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

func initGoStates() []namedStateConfig {
	return []namedStateConfig{
		{"Memory 1", 1, castToDirectUpdateState(NewGoMemoryState)},
		{"Memory 2", 2, castToDirectUpdateState(NewGoMemoryState)},
		{"Memory 3", 3, castToDirectUpdateState(NewGoMemoryState)},
		{"File Index and Store 1", 1, castToDirectUpdateState(NewGoFileState)},
		{"File Index and Store 2", 2, castToDirectUpdateState(NewGoFileState)},
		{"File Index and Store 3", 3, castToDirectUpdateState(NewGoFileState)},
		{"Cached File Index and Store 1", 1, castToDirectUpdateState(NewGoCachedFileState)},
		{"Cached File Index and Store 2", 2, castToDirectUpdateState(NewGoCachedFileState)},
		{"Cached File Index and Store 3", 3, castToDirectUpdateState(NewGoCachedFileState)},
		{"LevelDB Index and Store 1", 1, castToDirectUpdateState(NewGoLeveLIndexAndStoreState)},
		{"LevelDB Index and Store 2", 2, castToDirectUpdateState(NewGoLeveLIndexAndStoreState)},
		{"LevelDB Index and Store 3", 3, castToDirectUpdateState(NewGoLeveLIndexAndStoreState)},
		{"Cached LevelDB Index and Store 1", 1, castToDirectUpdateState(NewGoCachedLeveLIndexAndStoreState)},
		{"Cached LevelDB Index and Store 2", 2, castToDirectUpdateState(NewGoCachedLeveLIndexAndStoreState)},
		{"Cached LevelDB Index and Store 3", 3, castToDirectUpdateState(NewGoCachedLeveLIndexAndStoreState)},
		{"File Index and Store 3 Ldb Archive", 3, castToDirectUpdateStatePathOnly(NewScheme3GoFileLdbArchiveState)},
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
	var hashes = [][]common.Hash{nil, nil, nil, nil}
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
			hashes[int(config.schema)] = append(hashes[int(config.schema)], hash4) // store the last hash
		})
	}

	// check all final hashes for each schema are the same
	for schema := 0; schema < len(hashes); schema++ {
		for i := 0; i < len(hashes[schema])-1; i++ {
			if hashes[schema][i] != hashes[schema][i+1] {
				t.Errorf("hashes differ in schema %d", schema)
			}
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
	goSchema := state.(*syncedState).state.(*GoState).GoSchema.(*GoSchema1)
	goSchema.balancesStore = failingStore[uint32, common.Balance]{goSchema.balancesStore}
	goSchema.noncesStore = failingStore[uint32, common.Nonce]{goSchema.noncesStore}
	goSchema.valuesStore = failingStore[uint32, common.Value]{goSchema.valuesStore}

	_ = goSchema.setBalance(address1, common.Balance{})
	_ = goSchema.setNonce(address1, common.Nonce{})
	_ = goSchema.setStorage(address1, key1, common.Value{})

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
	goSchema := state.(*syncedState).state.(*GoState).GoSchema.(*GoSchema1)
	goSchema.addressIndex = failingIndex[common.Address, uint32]{goSchema.addressIndex}

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

			memoryFootprint := state.GetMemoryFootprint()
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
