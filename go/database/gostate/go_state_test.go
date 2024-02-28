package gostate

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database"
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

type namedDatabaseConfig struct {
	config  database.Configuration
	factory database.DatabaseFactory
}

func (c *namedDatabaseConfig) name() string {
	return c.config.String()
}

func (c *namedDatabaseConfig) createState(dir string) (database.Database, error) {
	return c.factory(database.Parameters{
		Variant:   c.config.Variant,
		Schema:    c.config.Schema,
		Archive:   c.config.Archive,
		Directory: dir,
	})
}

func initGoStates() []namedDatabaseConfig {
	res := []namedDatabaseConfig{}
	for config, factory := range database.GetAllRegisteredDatabaseFactories() {
		res = append(res, namedDatabaseConfig{
			config:  config,
			factory: factory,
		})
	}
	return res
}

func TestMissingKeys(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

			accountState, err := database.Exists(address1)
			if err != nil || accountState != false {
				t.Errorf("Account must not exist in the initial state, but it exists. err: %s", err)
			}
			balance, err := database.GetBalance(address1)
			if (err != nil || balance != common.Balance{}) {
				t.Errorf("Balance must be empty. It is: %s, err: %s", balance, err)
			}
			nonce, err := database.GetNonce(address1)
			if (err != nil || nonce != common.Nonce{}) {
				t.Errorf("Nonce must be empty. It is: %s, err: %s", nonce, err)
			}
			value, err := database.GetStorage(address1, key1)
			if (err != nil || value != common.Value{}) {
				t.Errorf("Value must be empty. It is: %s, err: %s", value, err)
			}
			code, err := database.GetCode(address1)
			if err != nil || code != nil {
				t.Errorf("Value must be empty. It is: %s, err: %s", value, err)
			}
			size, err := database.GetCodeSize(address1)
			if err != nil || size != 0 {
				t.Errorf("Value must be 0. It is: %d, err: %s", value, err)
			}
		})
	}
}

func TestBasicOperations(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

			// fill-in values
			err = database.Apply(12, common.Update{
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
			if val, err := database.Exists(address1); err != nil || val != true {
				t.Errorf("Created account does not exists: Val: %t, Err: %v", val, err)
			}
			if val, err := database.GetNonce(address1); (err != nil || val != common.Nonce{123}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetBalance(address2); (err != nil || val != common.Balance{45}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetStorage(address1, key1); (err != nil || val != common.Value{67}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetCode(address1); err != nil || !bytes.Equal(val, []byte{0x12, 0x34}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetCodeSize(address1); err != nil || val != 2 {
				t.Errorf("Invalid code size or error returned: Val: %d, Err: %v", val, err)
			}

			// delete account
			err = database.Apply(14, common.Update{DeletedAccounts: []common.Address{address1}})
			if err != nil {
				t.Errorf("Error: %s", err)
			}
			if val, err := database.Exists(address1); err != nil || val != false {
				t.Errorf("Deleted account is not deleted: Val: %t, Err: %s", val, err)
			}

			// fetch wrong combinations
			if val, err := database.GetStorage(address1, key1); (err != nil || val != common.Value{}) {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
		})
	}
}

func TestDeletingAccounts(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

			// fill-in values
			update := common.Update{
				CreatedAccounts: []common.Address{address1},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{123}}},
				Balances:        []common.BalanceUpdate{{Account: address2, Balance: common.Balance{45}}},
				Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{0x12, 0x34}}},
			}
			if err := database.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			// fetch values
			if val, err := database.Exists(address1); err != nil || val != true {
				t.Errorf("Created account does not exists: Val: %t, Err: %v", val, err)
			}

			// delete account
			update = common.Update{
				DeletedAccounts: []common.Address{address1},
			}
			if err := database.Apply(2, update); err != nil {
				t.Errorf("failed to apply update: %v", err)
			}
			if val, err := database.Exists(address1); err != nil || val != false {
				t.Errorf("Deleted account is not deleted: Val: %t, Err: %s", val, err)
			}
		})
	}
}

func TestMoreInserts(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

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

			if err := database.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			if val, err := database.GetStorage(address1, key3); err != nil || val != val3 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetStorage(address2, key1); err != nil || val != val1 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
			if val, err := database.GetStorage(address3, key2); err != nil || val != val2 {
				t.Errorf("Invalid value or error returned: Val: %v, Err: %v", val, err)
			}
		})
	}
}

func TestRecreatingAccountsPreservesEverythingButTheStorage(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

			code1 := []byte{1, 2, 3}

			// create an account and set some of its properties
			update := common.Update{
				CreatedAccounts: []common.Address{address1},
				Balances:        []common.BalanceUpdate{{Account: address1, Balance: balance1}},
				Nonces:          []common.NonceUpdate{{Account: address1, Nonce: nonce1}},
				Codes:           []common.CodeUpdate{{Account: address1, Code: code1}},
				Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
			}
			if err := database.Apply(1, update); err != nil {
				t.Errorf("failed to update state: %v", err)
			}

			if exists, err := database.Exists(address1); !exists || err != nil {
				t.Errorf("account does not exist, err %v", err)
			}

			if got, err := database.GetBalance(address1); got != balance1 || err != nil {
				t.Errorf("unexpected balance, wanted %v, got %v, err %v", balance1, got, err)
			}

			if got, err := database.GetNonce(address1); got != nonce1 || err != nil {
				t.Errorf("unexpected nonce, wanted %v, got %v, err %v", nonce1, got, err)
			}

			if got, err := database.GetCode(address1); !bytes.Equal(got, code1) || err != nil {
				t.Errorf("unexpected code, wanted %v, got %v, err %v", code1, got, err)
			}

			if got, err := database.GetStorage(address1, key1); got != val1 || err != nil {
				t.Errorf("unexpected storage, wanted %v, got %v, err %v", val1, got, err)
			}

			// re-creating the account preserves everything but the database.
			if err := database.Apply(2, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
				t.Errorf("failed to recreate account: %v", err)
			}

			if exists, err := database.Exists(address1); !exists || err != nil {
				t.Errorf("account should still exist, err %v", err)
			}
			if got, err := database.GetBalance(address1); got != balance1 || err != nil {
				t.Errorf("unexpected balance, wanted %v, got %v, err %v", balance1, got, err)
			}
			if got, err := database.GetNonce(address1); got != nonce1 || err != nil {
				t.Errorf("unexpected nonce, wanted %v, got %v, err %v", nonce1, got, err)
			}
			if got, err := database.GetCode(address1); !bytes.Equal(got, code1) || err != nil {
				t.Errorf("unexpected code, wanted %v, got %v, err %v", code1, got, err)
			}
			if got, err := database.GetStorage(address1, key1); got != val0 || err != nil {
				t.Errorf("failed to clear the storage, wanted %v, got %v, err %v", val0, got, err)
			}

		})
	}
}

func TestHashing(t *testing.T) {
	var hashes = [][]common.Hash{nil, nil, nil, nil, nil, nil}
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %v", config.name(), err)
			}
			defer database.Close()

			initialHash, err := database.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}

			database.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}})
			hash1, err := database.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}

			database.Apply(2, common.Update{Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}}})
			hash2, err := database.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}
			if initialHash == hash1 {
				t.Errorf("hash of changed state not changed")
			}

			database.Apply(3, common.Update{Balances: []common.BalanceUpdate{{Account: address1, Balance: balance1}}})
			hash3, err := database.GetHash()
			if err != nil {
				t.Fatalf("unable to get state hash; %v", err)
			}
			if initialHash == hash2 || hash1 == hash2 {
				t.Errorf("hash of changed state not changed")
			}

			if initialHash == hash3 || hash1 == hash3 || hash2 == hash3 {
				t.Errorf("hash of changed state not changed")
			}

			database.Apply(4, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: []byte{0x12, 0x34, 0x56, 0x78}}}})
			hash4, err := database.GetHash()
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
	db, err := newGoMemoryState(database.Parameters{
		Directory: t.TempDir(),
		Schema:    1,
		Archive:   database.NoArchive,
	})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := database.UnsafeUnwrapSyncedDatabase(db).(*GoState).live.(*GoSchema1)
	goSchema.balancesStore = failingStore[uint32, common.Balance]{goSchema.balancesStore}
	goSchema.noncesStore = failingStore[uint32, common.Nonce]{goSchema.noncesStore}
	goSchema.valuesStore = failingStore[uint32, common.Value]{goSchema.valuesStore}

	_ = goSchema.SetBalance(address1, common.Balance{})
	_ = goSchema.SetNonce(address1, common.Nonce{})
	_ = goSchema.SetStorage(address1, key1, common.Value{})

	_, err = db.GetBalance(address1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = db.GetNonce(address1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the store err; returned %s", err)
	}

	_, err = db.GetStorage(address1, key1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the store err; returned %s", err)
	}
}

func TestFailingIndex(t *testing.T) {
	db, err := newGoMemoryState(database.Parameters{
		Directory: t.TempDir(),
		Schema:    1,
		Archive:   database.NoArchive,
	})
	if err != nil {
		t.Fatalf("failed to create in-memory state; %s", err)
	}
	goSchema := database.UnsafeUnwrapSyncedDatabase(db).(*GoState).live.(*GoSchema1)
	goSchema.addressIndex = failingIndex[common.Address, uint32]{goSchema.addressIndex}

	_, err = db.GetBalance(address1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = db.GetNonce(address1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the index err; returned %s", err)
	}

	_, err = db.GetStorage(address1, key1)
	if err != errInjectedByTest {
		t.Errorf("State service does not return the index err; returned %s", err)
	}
}

func TestGetMemoryFootprint(t *testing.T) {
	for _, config := range initGoStates() {
		t.Run(config.name(), func(t *testing.T) {
			database, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer database.Close()

			memoryFootprint := database.GetMemoryFootprint()
			str := memoryFootprint.ToString("state")
			if config.config.Schema <= 3 && !strings.Contains(str, "hashTree") {
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

	database := newGoState(live, archive, nil)
	database.Flush()
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

	database := newGoState(live, archive, nil)
	database.Close()
}
