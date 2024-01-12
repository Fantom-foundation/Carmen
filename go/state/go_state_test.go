package state

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}
	address4 = common.Address{0x04}

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
		{"Memory 1", 1, GoMemory},
		{"Memory 2", 2, GoMemory},
		{"Memory 3", 3, GoMemory},
		{"Memory 4", 4, GoMemory},
		{"Memory 5", 5, GoMemory},
		{"File Index and Store 1", 1, GoFileNoCache},
		{"File Index and Store 2", 2, GoFileNoCache},
		{"File Index and Store 3", 3, GoFileNoCache},
		{"File 4", 4, GoFileNoCache},
		{"File 5", 5, GoFileNoCache},
		{"Cached File Index and Store 1", 1, GoFile},
		{"Cached File Index and Store 2", 2, GoFile},
		{"Cached File Index and Store 3", 3, GoFile},
		{"LevelDB Index and Store 1", 1, GoLevelDbNoCache},
		{"LevelDB Index and Store 2", 2, GoLevelDbNoCache},
		{"LevelDB Index and Store 3", 3, GoLevelDbNoCache},
		{"Cached LevelDB Index and Store 1", 1, GoLevelDb},
		{"Cached LevelDB Index and Store 2", 2, GoLevelDb},
		{"Cached LevelDB Index and Store 3", 3, GoLevelDb},
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
			err = state.Apply(12, common.Update{
				CreatedAccounts: []common.Address{address1},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{123}}},
				Balances:        []common.BalanceUpdate{{Account: address2, Balance: common.Balance{45}}},
				Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: common.Value{67}}},
				Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{0x12, 0x34}}},
			})
			if err != nil {
				t.Errorf("Error: %s", err)
			}

			// fetch values
			if val, err := state.Exists(address1); err != nil || val != true {
				t.Errorf("Created account does not exists: Val: %t, Err: %v", val, err)
			}
			if val, err := state.GetNonce(address1); (err != nil || val != common.Nonce{123}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetBalance(address2); (err != nil || val != common.Balance{45}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetStorage(address1, key1); (err != nil || val != common.Value{67}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetCode(address1); err != nil || !bytes.Equal(val, []byte{0x12, 0x34}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetCodeSize(address1); err != nil || val != 2 {
				t.Errorf("Invalid code size or error returned: Val: %d, Err: %v", val, err)
			}

			// delete account
			err = state.Apply(14, common.Update{DeletedAccounts: []common.Address{address1}})
			if err != nil {
				t.Errorf("Error: %s", err)
			}
			if val, err := state.Exists(address1); err != nil || val != false {
				t.Errorf("Deleted account is not deleted: Val: %t, Err: %s", val, err)
			}

			// fetch wrong combinations
			if val, err := state.GetStorage(address1, key1); (err != nil || val != common.Value{}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
		})
	}
}

func TestDeletingAccounts(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			// fill-in values
			update := common.Update{
				CreatedAccounts: []common.Address{address1},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{123}}},
				Balances:        []common.BalanceUpdate{{Account: address2, Balance: common.Balance{45}}},
				Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{0x12, 0x34}}},
			}
			if err := state.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			// fetch values
			if val, err := state.Exists(address1); err != nil || val != true {
				t.Errorf("Created account does not exists: Val: %t, Err: %v", val, err)
			}

			// delete account
			update = common.Update{
				DeletedAccounts: []common.Address{address1},
			}
			if err := state.Apply(2, update); err != nil {
				t.Errorf("failed to apply update: %v", err)
			}
			if val, err := state.Exists(address1); err != nil || val != false {
				t.Errorf("Deleted account is not deleted: Val: %t, Err: %s", val, err)
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

			update := common.Update{
				// create accounts since setting values to non-existing accounts may be ignored
				Nonces: []common.NonceUpdate{
					{Account: address1, Nonce: common.ToNonce(12)},
					{Account: address2, Nonce: common.ToNonce(12)},
					{Account: address3, Nonce: common.ToNonce(12)},
				},
				// insert more combinations, so we do not have only zero-indexes everywhere
				Slots: []common.SlotUpdate{
					{Account: address1, Key: key1, Value: val1},
					{Account: address1, Key: key2, Value: val2},
					{Account: address1, Key: key3, Value: val3},

					{Account: address2, Key: key1, Value: val1},
					{Account: address2, Key: key2, Value: val2},
					{Account: address2, Key: key3, Value: val3},

					{Account: address3, Key: key1, Value: val1},
					{Account: address3, Key: key2, Value: val2},
					{Account: address3, Key: key3, Value: val3},
				},
			}

			if err := state.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			if val, err := state.GetStorage(address1, key3); err != nil || val != val3 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetStorage(address2, key1); err != nil || val != val1 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := state.GetStorage(address3, key2); err != nil || val != val2 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
		})
	}
}

func TestRecreatingAccountsPreservesEverythingButTheStorage(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name, err)
			}
			defer state.Close()

			code1 := []byte{1, 2, 3}

			// create an account and set some of its properties
			update := common.Update{
				CreatedAccounts: []common.Address{address1},
				Balances:        []common.BalanceUpdate{{Account: address1, Balance: balance1}},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: nonce1}},
				Codes:           []common.CodeUpdate{{Account: address1, Code: code1}},
				Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
			}
			if err := state.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			if exists, err := state.Exists(address1); !exists || err != nil {
				t.Errorf("account does not exist, err %v", err)
			}

			if got, err := state.GetBalance(address1); got != balance1 || err != nil {
				t.Errorf("unexpected balance, wanted %v, got %v, err %v", balance1, got, err)
			}

			if got, err := state.GetNonce(address1); got != nonce1 || err != nil {
				t.Errorf("unexpected nonce, wanted %v, got %v, err %v", nonce1, got, err)
			}

			if got, err := state.GetCode(address1); !bytes.Equal(got, code1) || err != nil {
				t.Errorf("unexpected code, wanted %v, got %v, err %v", code1, got, err)
			}

			if got, err := state.GetStorage(address1, key1); got != val1 || err != nil {
				t.Errorf("unexpected storage, wanted %v, got %v, err %v", val1, got, err)
			}

			// re-creating the account preserves everything but the state.
			if err := state.Apply(2, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
				t.Errorf("failed to recreate account: %v", err)
			}

			if exists, err := state.Exists(address1); !exists || err != nil {
				t.Errorf("account should still exist, err %v", err)
			}
			if got, err := state.GetBalance(address1); got != balance1 || err != nil {
				t.Errorf("unexpected balance, wanted %v, got %v, err %v", balance1, got, err)
			}
			if got, err := state.GetNonce(address1); got != nonce1 || err != nil {
				t.Errorf("unexpected nonce, wanted %v, got %v, err %v", nonce1, got, err)
			}
			if got, err := state.GetCode(address1); !bytes.Equal(got, code1) || err != nil {
				t.Errorf("unexpected code, wanted %v, got %v, err %v", code1, got, err)
			}
			if got, err := state.GetStorage(address1, key1); got != val0 || err != nil {
				t.Errorf("failed to clear the storage, wanted %v, got %v, err %v", val0, got, err)
			}

		})
	}
}

func TestHashing(t *testing.T) {
	var hashes = [][]common.Hash{nil, nil, nil, nil, nil, nil}
	for _, config := range initGoStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %v", config.name, err)
			}
			defer state.Close()

			initialHash, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}

			state.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}})
			hash1, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}

			state.Apply(2, common.Update{Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}}})
			hash2, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}
			if initialHash == hash1 {
				t.Errorf("hash of changed state not changed")
			}

			state.Apply(3, common.Update{Balances: []common.BalanceUpdate{{Account: address1, Balance: balance1}}})
			hash3, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}
			if initialHash == hash2 || hash1 == hash2 {
				t.Errorf("hash of changed state not changed")
			}

			if initialHash == hash3 || hash1 == hash3 || hash2 == hash3 {
				t.Errorf("hash of changed state not changed")
			}

			state.Apply(4, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: []byte{0x12, 0x34, 0x56, 0x78}}}})
			hash4, err := state.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}
			if initialHash == hash4 || hash3 == hash4 {
				t.Errorf("hash of changed state not changed")
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
	state, err := newGoMemoryState(Parameters{Schema: 1})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := state.(*syncedState).state.(*GoState).live.(*GoSchema1)
	goSchema.balancesStore = failingStore[uint32, common.Balance]{goSchema.balancesStore}
	goSchema.noncesStore = failingStore[uint32, common.Nonce]{goSchema.noncesStore}
	goSchema.valuesStore = failingStore[uint32, common.Value]{goSchema.valuesStore}

	_ = goSchema.SetBalance(address1, common.Balance{})
	_ = goSchema.SetNonce(address1, common.Nonce{})
	_ = goSchema.SetStorage(address1, key1, common.Value{})

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
	state, err := newGoMemoryState(Parameters{Schema: 1})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := state.(*syncedState).state.(*GoState).live.(*GoSchema1)
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
			str := memoryFootprint.ToString("state")
			if config.schema <= 3 && !strings.Contains(str, "hashTree") {
				t.Errorf("memory footprint string does not contain any hashTree")
			}
		})
	}
}

func TestGoState_FlushFlushesLiveDbAndArchive(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := NewMockLiveDB(ctrl)
	archive := archive.NewMockArchive(ctrl)

	live.EXPECT().Flush()
	archive.EXPECT().Flush()

	state := newGoState(live, archive, nil)
	state.Flush()
}

func TestGoState_CloseClosesLiveDbAndArchive(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := NewMockLiveDB(ctrl)
	archive := archive.NewMockArchive(ctrl)

	gomock.InOrder(
		live.EXPECT().Flush(),
		live.EXPECT().Close(),
	)
	gomock.InOrder(
		archive.EXPECT().Flush(),
		archive.EXPECT().Close(),
	)

	state := newGoState(live, archive, nil)
	state.Close()
}
