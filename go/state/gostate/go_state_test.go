// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package gostate

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/state"
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

	balance1 = amount.New(1)
	balance2 = amount.New(2)
	balance3 = amount.New(3)

	nonce1 = common.Nonce{0x01}
	nonce2 = common.Nonce{0x02}
	nonce3 = common.Nonce{0x03}
)

type namedStateConfig struct {
	config  state.Configuration
	factory state.StateFactory
}

func (c *namedStateConfig) name() string {
	return c.config.String()
}

func (c *namedStateConfig) createState(dir string) (state.State, error) {
	return c.factory(state.Parameters{
		Variant:   c.config.Variant,
		Schema:    c.config.Schema,
		Archive:   c.config.Archive,
		Directory: dir,
	})
}

func initGoStates() []namedStateConfig {
	res := []namedStateConfig{}
	for config, factory := range state.GetAllRegisteredStateFactories() {
		res = append(res, namedStateConfig{
			config:  config,
			factory: factory,
		})
	}
	return res
}

func TestMissingKeys(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer state.Close()

			accountState, err := state.Exists(address1)
			if err != nil || accountState != false {
				t.Errorf("Account must not exist in the initial state, but it exists. err: %s", err)
			}
			balance, err := state.GetBalance(address1)
			if err != nil || !balance.IsZero() {
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
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer state.Close()

			// fill-in values
			err = state.Apply(12, common.Update{
				CreatedAccounts: []common.Address{address1},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{123}}},
				Balances:        []common.BalanceUpdate{{Account: address2, Balance: amount.New(45)}},
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
			if val, err := state.GetBalance(address2); err != nil || val != amount.New(45) {
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
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer state.Close()

			// fill-in values
			update := common.Update{
				CreatedAccounts: []common.Address{address1},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{123}}},
				Balances:        []common.BalanceUpdate{{Account: address2, Balance: amount.New(45)}},
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
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
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
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
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
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %v", config.name(), err)
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
			hashes[int(config.config.Schema)] = append(hashes[int(config.config.Schema)], hash4) // store the last hash
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

var errInjectedByTest = fmt.Errorf("testing error")

type failingStore[I common.Identifier, V any] struct {
	store.Store[I, V]
}

func (m failingStore[I, V]) Get(id I) (value V, err error) {
	err = errInjectedByTest
	return
}

type failingIndex[K comparable, I common.Identifier] struct {
	index.Index[K, I]
}

func (m failingIndex[K, I]) Get(key K) (id I, err error) {
	err = errInjectedByTest
	return
}

func TestFailingStore(t *testing.T) {
	db, err := newGoMemoryState(state.Parameters{
		Directory: t.TempDir(),
		Schema:    1,
		Archive:   state.NoArchive,
	})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := state.UnsafeUnwrapSyncedState(db).(*GoState).live.(*GoSchema1)
	goSchema.balancesStore = failingStore[uint32, amount.Amount]{goSchema.balancesStore}
	goSchema.noncesStore = failingStore[uint32, common.Nonce]{goSchema.noncesStore}
	goSchema.valuesStore = failingStore[uint32, common.Value]{goSchema.valuesStore}

	_ = goSchema.SetBalance(address1, amount.New())
	_ = goSchema.SetNonce(address1, common.Nonce{})
	_ = goSchema.SetStorage(address1, key1, common.Value{})

	_, err = db.GetBalance(address1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = db.GetNonce(address1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = db.GetStorage(address1, key1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the store err; returned %s", err)
	}
}

func TestFailingIndex(t *testing.T) {
	db, err := newGoMemoryState(state.Parameters{
		Directory: t.TempDir(),
		Schema:    1,
		Archive:   state.NoArchive,
	})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := state.UnsafeUnwrapSyncedState(db).(*GoState).live.(*GoSchema1)
	goSchema.addressIndex = failingIndex[common.Address, uint32]{goSchema.addressIndex}

	_, err = db.GetBalance(address1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = db.GetNonce(address1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = db.GetStorage(address1, key1)
	if !errors.Is(err, errInjectedByTest) {
		t.Errorf("State service does not return the index err; returned %s", err)
	}
}

func TestGetMemoryFootprint(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer state.Close()

			memoryFootprint := state.GetMemoryFootprint()
			str := memoryFootprint.ToString("state")
			if config.config.Schema <= 3 && !strings.Contains(str, "hashTree") {
				t.Errorf("memory footprint string does not contain any hashTree")
			}
		})
	}
}

func TestGoState_FlushFlushesLiveDbAndArchive(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := state.NewMockLiveDB(ctrl)
	archive := archive.NewMockArchive(ctrl)

	live.EXPECT().Flush()
	archive.EXPECT().Flush()

	state := newGoState(live, archive, nil)
	state.Flush()
}

func TestGoState_CloseClosesLiveDbAndArchive(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := state.NewMockLiveDB(ctrl)
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

func TestStateDB_AddBlock_Errors_Propagated_MultipleStateInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)

	liveDB.EXPECT().Exists(gomock.Any()).AnyTimes()

	injectedErr := fmt.Errorf("injectedError")
	// The First attempt to call Apply() will fail
	// while next calls will not happen at all.
	// The first call is triggered from stateA
	// while the other call from stateB
	// will not be executed because the
	// state is already corrupted.
	liveDB.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, injectedErr)

	db := newGoState(liveDB, nil, []func(){})

	stateA := state.CreateStateDBUsing(db)
	runAddBlock(0, stateA)
	if err := stateA.Check(); !errors.Is(err, injectedErr) {
		t.Errorf("first operation should fail: %v", err)
	}

	stateB := state.CreateStateDBUsing(db)
	runAddBlock(1, stateB)
	if err := stateB.Check(); !errors.Is(err, injectedErr) {
		t.Errorf("second operation should fail: %v", err)
	}
}

func TestStateDB_AddBlock_Errors_Propagated_From_Archive_MultipleStateInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)
	archiveDB := archive.NewMockArchive(ctrl)

	liveDB.EXPECT().Exists(gomock.Any()).AnyTimes()
	liveDB.EXPECT().Apply(gomock.Any(), gomock.Any())

	archiveDB.EXPECT().Flush().AnyTimes()

	injectedErr := fmt.Errorf("injectedError")
	// The First attempt to call Add() will fail
	// while next calls will not happen at all.
	// The first call is triggered from stateA
	// while the other call from stateB
	// will not be executed because the
	// state is already corrupted.
	archiveDB.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).Return(injectedErr)

	db := newGoState(liveDB, archiveDB, []func(){})
	flush := func() {
		state.UnsafeUnwrapSyncedState(db).(*GoState).archiveWriter <- archiveUpdate{}
		<-state.UnsafeUnwrapSyncedState(db).(*GoState).archiveWriterFlushDone
	}

	stateA := state.CreateStateDBUsing(db)

	runAddBlock(0, stateA)
	flush() // flush db to propagate errors from archive
	if err := stateA.Check(); !errors.Is(err, injectedErr) {
		t.Errorf("first operation should fail: %v", err)
	}

	stateB := state.CreateStateDBUsing(db)
	runAddBlock(1, stateB)
	flush() // flush db to propagate errors from archive
	if err := stateB.Check(); !errors.Is(err, injectedErr) {
		t.Errorf("second operation should fail: %v", err)
	}
}

func TestStateDB_AddBlock_CannotCallRepeatedly_OnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)

	liveDB.EXPECT().Exists(gomock.Any()).AnyTimes()

	injectedErr := fmt.Errorf("injectedError")

	// will be called only once as repeated calls will not get triggered.
	liveDB.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, injectedErr)

	db := newGoState(liveDB, nil, []func(){})

	stateDB := state.CreateStateDBUsing(db)
	for i := 0; i < 10; i++ {
		runAddBlock(uint64(i), stateDB)
		if err := stateDB.Check(); !errors.Is(err, injectedErr) {
			t.Errorf("each operation should fail: %v", err)
		}
	}
}

func TestState_Flush_Or_Close_Corrupted_State_Detected(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)

	liveDB.EXPECT().Exists(gomock.Any()).AnyTimes()
	liveDB.EXPECT().Flush().AnyTimes()
	liveDB.EXPECT().Close().AnyTimes()

	injectedErr := fmt.Errorf("injectedError")

	// will be called only once as repeated calls will not get triggered.
	liveDB.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, injectedErr)

	db := newGoState(liveDB, nil, []func(){})

	update := common.Update{
		CreatedAccounts: []common.Address{{0xA}},
		Balances:        []common.BalanceUpdate{{common.Address{0xA}, amount.New(10)}},
	}

	// the same result many times
	for i := 0; i < 10; i++ {
		if err := db.Apply(uint64(i), update); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
		if err := db.Check(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
		if err := db.Flush(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
		if err := db.Close(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
	}
}

func TestState_Flush_Or_Close_Corrupted_Archive_Detected(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)
	archiveDB := archive.NewMockArchive(ctrl)

	injectedErr := fmt.Errorf("injectedError")
	liveDB.EXPECT().Flush().AnyTimes()
	liveDB.EXPECT().Close().AnyTimes()
	archiveDB.EXPECT().Flush().AnyTimes()
	archiveDB.EXPECT().Close().Return(injectedErr).AnyTimes()

	db := newGoState(liveDB, archiveDB, []func(){})

	// the same result many times
	for i := 0; i < 10; i++ {
		if err := db.Close(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
		if err := db.Check(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
		if err := db.Flush(); !errors.Is(err, injectedErr) {
			t.Errorf("operation should fail: %v", err)
		}
	}
}

func TestState_Apply_CannotCallRepeatedly_OnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	liveDB := state.NewMockLiveDB(ctrl)

	liveDB.EXPECT().Exists(gomock.Any()).AnyTimes()

	injectedErr := fmt.Errorf("injectedError")

	// will be called only once as repeated calls will not get triggered.
	liveDB.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, injectedErr)

	db := newGoState(liveDB, nil, []func(){})

	for i := 0; i < 10; i++ {
		update := common.Update{
			CreatedAccounts: []common.Address{{0xA}},
			Balances:        []common.BalanceUpdate{{common.Address{0xA}, amount.New(10)}},
		}
		if err := db.Apply(uint64(i), update); !errors.Is(err, injectedErr) {
			t.Errorf("each operation should fail: %v", err)
		}

		if err := db.Check(); !errors.Is(err, injectedErr) {
			t.Errorf("each operation should fail: %v", err)
		}
	}
}

func TestState_All_Live_Operations_May_Cause_Failure(t *testing.T) {
	addr := common.Address{0xA}
	key := common.Key{0xB}
	injectedErr := fmt.Errorf("injectedError")

	const loops = 10
	for i := 0; i < loops; i++ {
		i := i
		t.Run(fmt.Sprintf("operation_%d", i), func(t *testing.T) {
			t.Parallel()
			results := make([]error, loops)
			results[i] = injectedErr

			ctrl := gomock.NewController(t)
			liveDB := state.NewMockLiveDB(ctrl)
			liveDB.EXPECT().Exists(addr).Return(false, results[0]).AnyTimes()
			liveDB.EXPECT().GetBalance(addr).Return(amount.New(), results[1]).AnyTimes()
			liveDB.EXPECT().GetNonce(addr).Return(common.Nonce{}, results[2]).AnyTimes()
			liveDB.EXPECT().GetStorage(addr, key).Return(common.Value{}, results[3]).AnyTimes()
			liveDB.EXPECT().GetCode(addr).Return(make([]byte, 0), results[4]).AnyTimes()
			liveDB.EXPECT().GetCodeSize(addr).Return(0, results[5]).AnyTimes()
			liveDB.EXPECT().GetCodeHash(addr).Return(common.Hash{}, results[6]).AnyTimes()
			liveDB.EXPECT().GetHash().Return(common.Hash{}, results[7]).AnyTimes()
			liveDB.EXPECT().Flush().Return(results[8]).AnyTimes()
			liveDB.EXPECT().Close().Return(results[9]).AnyTimes()

			db := newGoState(liveDB, nil, []func(){})
			// calls must succeed until the first failure,
			// repeated calls must all fail
			var shouldFail bool
			for i := 0; i < 2; i++ {
				shouldFail = shouldFail || errors.Is(results[0], injectedErr)
				if _, err := db.Exists(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[1], injectedErr)
				if _, err := db.GetBalance(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[2], injectedErr)
				if _, err := db.GetNonce(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[3], injectedErr)
				if _, err := db.GetStorage(addr, key); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[4], injectedErr)
				if _, err := db.GetCode(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[5], injectedErr)
				if _, err := db.GetCodeSize(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[6], injectedErr)
				if _, err := db.GetCodeHash(addr); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[7], injectedErr)
				if _, err := db.GetHash(); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
				shouldFail = shouldFail || errors.Is(results[8], injectedErr)
				if err := db.Flush(); shouldFail && !errors.Is(err, injectedErr) {
					t.Errorf("operation should fail")
				}
			}

			if err := db.Close(); !errors.Is(err, injectedErr) {
				t.Errorf("operation should fail")
			}
		})
	}
}

func TestState_All_Archive_Operations_May_Cause_Failure(t *testing.T) {
	injectedErr := fmt.Errorf("injectedError")
	ctrl := gomock.NewController(t)

	liveDB := state.NewMockLiveDB(ctrl)
	liveDB.EXPECT().Flush().AnyTimes()
	liveDB.EXPECT().Close().AnyTimes()

	archiveDB := archive.NewMockArchive(ctrl)
	archiveDB.EXPECT().GetBlockHeight().Return(uint64(0), false, injectedErr).Times(2)
	archiveDB.EXPECT().Flush().AnyTimes()
	archiveDB.EXPECT().Close().AnyTimes()

	db := newGoState(liveDB, archiveDB, []func(){})
	// repeated calls must all fail
	for i := 0; i < 2; i++ {
		if _, err := db.GetArchiveState(0); !errors.Is(err, injectedErr) {
			t.Errorf("calling archive should fail")
		}
		if _, _, err := db.GetArchiveBlockHeight(); !errors.Is(err, injectedErr) {
			t.Errorf("calling archive should fail")
		}
	}
	// swap calls
	db = newGoState(liveDB, archiveDB, []func(){})
	for i := 0; i < 2; i++ {
		if _, _, err := db.GetArchiveBlockHeight(); !errors.Is(err, injectedErr) {
			t.Errorf("calling archive should fail")
		}
		if _, err := db.GetArchiveState(0); !errors.Is(err, injectedErr) {
			t.Errorf("calling archive should fail")
		}
	}

	if err := db.Close(); !errors.Is(err, injectedErr) {
		t.Errorf("closing databse should fail")
	}
}

func TestGoState_CloseIsCalledIfFlushFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := state.NewMockLiveDB(ctrl)
	injectedErr := errors.New("error")

	gomock.InOrder(
		live.EXPECT().Flush().Return(injectedErr),
		live.EXPECT().Close(),
	)

	state := newGoState(live, nil, nil)
	if err := state.Close(); !errors.Is(err, injectedErr) {
		t.Errorf("unexpected error")
	}
}

func runAddBlock(block uint64, stateDB state.StateDB) {
	addr := common.Address{byte(block)}
	key := common.Key{0xA}
	stateDB.BeginBlock()
	stateDB.BeginTransaction()
	stateDB.CreateAccount(addr)
	stateDB.AddBalance(addr, amount.New(100))
	stateDB.SetState(addr, key, common.Value{123})
	stateDB.SetCode(addr, make([]byte, 80))
	stateDB.SetNonce(addr, 1)
	stateDB.EndTransaction()
	stateDB.EndBlock(block)
}
