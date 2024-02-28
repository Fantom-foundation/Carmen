package database_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/maps"

	_ "github.com/Fantom-foundation/Carmen/go/database/cppstate"
	"github.com/Fantom-foundation/Carmen/go/database/gostate"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}

	key1 = common.Key{0x01}
	key2 = common.Key{0x02}
	key3 = common.Key{0x03}

	val1 = common.Value{0x01}
	val2 = common.Value{0x02}
	val3 = common.Value{0x03}

	balance1 = common.Balance{0x01}
	balance2 = common.Balance{0x02}
	balance3 = common.Balance{0x03}

	nonce1 = common.Nonce{0x01}
	nonce2 = common.Nonce{0x02}
	nonce3 = common.Nonce{0x03}

	UnsupportedConfiguration = database.UnsupportedConfiguration
)

type namedDatabaseConfig struct {
	config  database.Configuration
	factory database.DatabaseFactory
}

func (c *namedDatabaseConfig) name() string {
	return c.config.String()
}

func (c *namedDatabaseConfig) createDatabase(directory string) (database.Database, error) {
	return database.NewDatabase(database.Parameters{
		Directory: directory,
		Variant:   c.config.Variant,
		Schema:    c.config.Schema,
		Archive:   c.config.Archive,
	})
}

func getDatabaseConfigurations() []namedDatabaseConfig {
	var res []namedDatabaseConfig
	for config, factory := range database.GetAllRegisteredDatabaseFactories() {
		res = append(res, namedDatabaseConfig{config, factory})
	}
	return res
}

func getAllSchemas() []database.Schema {
	schemas := map[database.Schema]struct{}{}
	for config := range database.GetAllRegisteredDatabaseFactories() {
		schemas[config.Schema] = struct{}{}
	}
	return maps.Keys(schemas)
}

func testEachConfiguration(t *testing.T, test func(t *testing.T, config *namedDatabaseConfig, s database.Database)) {
	for _, config := range getDatabaseConfigurations() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			state, err := config.createDatabase(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s: %v", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s: %v", config.name(), err)
				}
			}
			defer state.Close()

			test(t, &config, state)
		})
	}
}

func getReferenceStateFor(t *testing.T, params database.Parameters) (database.Database, error) {
	// The Go In-Memory implementation is the reference for all states.
	referenceConfig := params
	referenceConfig.Variant = gostate.VariantGoMemory
	referenceConfig.Directory = t.TempDir()
	return database.NewDatabase(referenceConfig)
}

func testHashAfterModification(t *testing.T, mod func(s database.Database)) {
	want := map[database.Schema]common.Hash{}
	for _, s := range getAllSchemas() {
		ref, err := getReferenceStateFor(t, database.Parameters{Schema: s})
		if err != nil {
			t.Fatalf("failed to create reference state: %v", err)
		}
		mod(ref)
		hash, err := ref.GetHash()
		if err != nil {
			t.Fatalf("failed to get hash of reference state: %v", err)
		}
		want[s] = hash
		ref.Close()
	}

	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
		mod(s)
		got, err := s.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}
		if want[config.config.Schema] != got {
			t.Errorf("Invalid hash, wanted %v, got %v", want[config.config.Schema], got)
		}
	})
}

func TestDatabase_EmptyHash(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		// nothing
	})
}

func TestDatabase_AddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			CreatedAccounts: []common.Address{address1},
		})
	})
}

func TestDatabase_MultipleAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			CreatedAccounts: []common.Address{address1, address2, address3},
		})
	})
}

func TestDatabase_DeletedAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			CreatedAccounts: []common.Address{address1, address2, address3},
			DeletedAccounts: []common.Address{address1, address2},
		})
	})
}

func TestDatabase_StorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Slots: []common.SlotUpdate{{Account: address1, Key: key2, Value: val3}},
		})
	})
}

func TestDatabase_MultipleStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Slots: []common.SlotUpdate{
				{Account: address1, Key: key2, Value: val3},
				{Account: address2, Key: key3, Value: val1},
				{Account: address3, Key: key1, Value: val2},
			},
		})
	})
}

func TestDatabase_BalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Balances: []common.BalanceUpdate{
				{Account: address1, Balance: balance1},
			},
		})
	})
}

func TestDatabase_MultipleBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Balances: []common.BalanceUpdate{
				{Account: address1, Balance: balance1},
				{Account: address2, Balance: balance2},
				{Account: address3, Balance: balance3},
			},
		})
	})
}

func TestDatabase_NonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Nonces: []common.NonceUpdate{
				{Account: address1, Nonce: nonce1},
			},
		})
	})
}

func TestDatabase_MultipleNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Nonces: []common.NonceUpdate{
				{Account: address1, Nonce: nonce1},
				{Account: address2, Nonce: nonce2},
				{Account: address3, Nonce: nonce3},
			},
		})
	})
}

func TestDatabase_CodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Codes: []common.CodeUpdate{
				{Account: address1, Code: []byte{1}},
			},
		})
	})
}

func TestDatabase_MultipleCodeUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		s.Apply(12, common.Update{
			Codes: []common.CodeUpdate{
				{Account: address1, Code: []byte{1}},
				{Account: address2, Code: []byte{1, 2}},
				{Account: address3, Code: []byte{1, 2, 3}},
			},
		})
	})
}

func TestDatabase_LargeStateHashes(t *testing.T) {
	testHashAfterModification(t, func(s database.Database) {
		update := common.Update{}
		for i := 0; i < 100; i++ {
			address := common.Address{byte(i)}
			update.CreatedAccounts = append(update.CreatedAccounts, address)
			for j := 0; j < 100; j++ {
				key := common.Key{byte(j)}
				update.Slots = append(update.Slots, common.SlotUpdate{Account: address, Key: key, Value: common.Value{byte(i), 0, 0, byte(j)}})
			}
			if i%21 == 0 {
				update.DeletedAccounts = append(update.DeletedAccounts, address)
			}
			update.Balances = append(update.Balances, common.BalanceUpdate{Account: address, Balance: common.Balance{byte(i)}})
			update.Nonces = append(update.Nonces, common.NonceUpdate{Account: address, Nonce: common.Nonce{byte(i + 1)}})
			update.Codes = append(update.Codes, common.CodeUpdate{Account: address, Code: []byte{byte(i), byte(i * 2), byte(i*3 + 2)}})
		}
		s.Apply(12, update)
	})
}

func TestDatabase_CanComputeNonEmptyMemoryFootprint(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
		fp := s.GetMemoryFootprint()
		if fp == nil {
			t.Fatalf("state produces invalid footprint: %v", fp)
		}
		if fp.Total() <= 0 {
			t.Errorf("memory footprint should not be empty")
		}
		if len(fp.ToString("top")) == 0 {
			t.Errorf("footprint text empty: %v", fp.ToString("top"))
		}
	})
}

func TestDatabase_CodeCanBeUpdated(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
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
		if err := s.Apply(1, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: code1}}}); err != nil {
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
		if err := s.Apply(2, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: code2}}}); err != nil {
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

func TestDatabase_CodeHashesMatchCodes(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
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
		s.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}})
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
		s.Apply(2, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: code}}})
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfTestCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfTestCode, hash)
		}

		// Reset code to empty code updates hash accordingly.
		s.Apply(3, common.Update{Codes: []common.CodeUpdate{{Account: address1, Code: []byte{}}}})
		hash, err = s.GetCodeHash(address1)
		if err != nil {
			t.Fatalf("error fetching code hash: %v", err)
		}
		if hash != hashOfEmptyCode {
			t.Errorf("Invalid hash, wanted %v, got %v", hashOfEmptyCode, hash)
		}
	})
}

func TestDatabase_DeleteNotExistingAccount(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
		if err := s.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
			t.Fatalf("Error: %s", err)
		}
		if err := s.Apply(2, common.Update{DeletedAccounts: []common.Address{address2}}); err != nil { // deleting never-existed account
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

func TestDatabase_CreatingAccountClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
		zero := common.Value{}
		if err := s.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
			t.Errorf("failed to create account: %v", err)
		}

		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != zero {
			t.Errorf("storage slot are initially not zero")
		}

		if err = s.Apply(2, common.Update{Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}}}); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err = s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.Apply(3, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
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

func TestDatabase_DeletingAccountsClearsStorage(t *testing.T) {
	testEachConfiguration(t, func(t *testing.T, config *namedDatabaseConfig, s database.Database) {
		zero := common.Value{}
		if err := s.Apply(1, common.Update{CreatedAccounts: []common.Address{address1}}); err != nil {
			t.Errorf("failed to create account: %v", err)
		}

		if err := s.Apply(2, common.Update{Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}}}); err != nil {
			t.Errorf("failed to update storage slot: %v", err)
		}

		val, err := s.GetStorage(address1, key1)
		if err != nil {
			t.Errorf("failed to fetch storage value: %v", err)
		}
		if val != val1 {
			t.Errorf("storage slot update did not take effect")
		}

		if err := s.Apply(3, common.Update{DeletedAccounts: []common.Address{address1}}); err != nil {
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

// TestDatabase_Archive inserts data into the state and tries to obtain the history from the archive.
func TestDatabase_ArchiveBasicProperties(t *testing.T) {
	for _, config := range getDatabaseConfigurations() {
		if config.config.Archive == database.NoArchive {
			continue
		}
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createDatabase(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer s.Close()

			balance12, _ := common.ToBalance(big.NewInt(0x12))
			balance34, _ := common.ToBalance(big.NewInt(0x34))

			if err := s.Apply(1, common.Update{
				CreatedAccounts: []common.Address{address1},
				Balances: []common.BalanceUpdate{
					{Account: address1, Balance: balance12},
				},
				Codes:  nil,
				Nonces: nil,
				Slots: []common.SlotUpdate{
					{Account: address1, Key: common.Key{0x05}, Value: common.Value{0x47}},
				},
			}); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := s.Apply(2, common.Update{
				Balances: []common.BalanceUpdate{
					{Account: address1, Balance: balance34},
					{Account: address2, Balance: balance12},
					{Account: address3, Balance: balance12},
				},
				Codes: []common.CodeUpdate{
					{Account: address1, Code: []byte{0x12, 0x23}},
				},
				Nonces: []common.NonceUpdate{
					{Account: address1, Nonce: common.Nonce{0x54}},
				},
				Slots: []common.SlotUpdate{
					{Account: address1, Key: common.Key{0x05}, Value: common.Value{0x89}},
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

			archiveType := config.config.Archive
			if archiveType != database.S4Archive && archiveType != database.S5Archive {
				hash1, err := state1.GetHash()
				if err != nil || fmt.Sprintf("%x", hash1) != "9f4836302c2a2e89ca09e38e77f6a57b3f09ce94dbbeecd865b841307186e8e5" {
					t.Errorf("unexpected archive state hash at block 1: %x, %v", hash1, err)
				}
				hash2, err := state2.GetHash()
				if err != nil || fmt.Sprintf("%x", hash2) != "f69f1e69a6512f15b702094c762c5ef5d7d712d9f35d7948d690df9abd192dd3" {
					t.Errorf("unexpected archive state hash at block 2: %x, %v", hash2, err)
				}
			}
		})
	}
}

// TestDatabase_LastArchiveBlock tests obtaining the state at the last (highest) block in the archive.
func TestDatabase_LastArchiveBlock(t *testing.T) {
	for _, config := range getDatabaseConfigurations() {
		if config.config.Archive == database.NoArchive {
			continue
		}
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if config.name()[0:3] == "cpp" {
				t.Skipf("GetArchiveBlockHeight not supported by the cpp state")
			}
			s, err := config.createDatabase(dir)
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
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
				t.Errorf("obtaining a block higher than the last one (%d) did not failed", lastBlockHeight)
			}
		})
	}
}

// TestDatabase_PersistentState inserts data into the state and closes it first, then the state
// is re-opened in another process, and it is tested that data are available, i.e. all was successfully persisted
func TestDatabase_PersistentState(t *testing.T) {
	for _, config := range getDatabaseConfigurations() {

		// skip setups without archive
		if config.config.Archive == database.NoArchive {
			continue
		}
		// skip in-memory
		if strings.HasPrefix(config.name(), "cpp-memory") || strings.HasPrefix(config.name(), "go-memory") {
			continue
		}
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			s, err := config.createDatabase(dir)
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

			common.ExecuteTestInSubProcess(t, "TestDatabase_StateRead", "-db-dir="+dir, "-db-impl="+config.name())
		})
	}
}

func fillStateForSnapshotting(db database.Database) {
	db.Apply(12, common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1, Balance: common.Balance{12}}},
		Nonces:          []common.NonceUpdate{{Account: address2, Nonce: common.Nonce{14}}},
		Codes:           []common.CodeUpdate{{Account: address3, Code: []byte{0, 8, 15}}},
		Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
	})
}

func TestDatabase_SnapshotCanBeCreatedAndRestored(t *testing.T) {
	for _, config := range getDatabaseConfigurations() {
		t.Run(config.name(), func(t *testing.T) {
			original, err := config.createDatabase(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
				}
			}
			defer original.Close()

			fillStateForSnapshotting(original)

			snapshot, err := original.CreateSnapshot()
			if err == backend.ErrSnapshotNotSupported {
				t.Skipf("configuration '%v' skipped since snapshotting is not supported", config.name())
			}
			if err != nil {
				t.Errorf("failed to create snapshot: %v", err)
				return
			}

			recovered, err := config.createDatabase(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
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

func TestDatabase_SnapshotCanBeCreatedAndVerified(t *testing.T) {
	for _, config := range getDatabaseConfigurations() {
		t.Run(config.name(), func(t *testing.T) {
			original, err := config.createDatabase(t.TempDir())
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("unsupported state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
				}
			}
			defer original.Close()

			fillStateForSnapshotting(original)

			snapshot, err := original.CreateSnapshot()
			if err == backend.ErrSnapshotNotSupported {
				t.Skipf("configuration '%v' skipped since snapshotting is not supported", config.name())
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

var databaseDir = flag.String("db-dir", "DEFAULT", "directory where the state is persisted")
var databaseImpl = flag.String("db-impl", "DEFAULT", "name of the state implementation")

// TestReadState verifies data are available in a database.
// The given state reads the data from the given directory and verifies the data are present.
// Name of the index and directory is provided as command line arguments
func TestDatabase_StateRead(t *testing.T) {
	// do not runt this test stand-alone
	if *databaseDir == "DEFAULT" {
		return
	}

	s := createDatabase(t, *databaseImpl, *databaseDir)
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
	if code, err := s.GetCode(address1); err != nil || !bytes.Equal(code, []byte{1, 2, 3}) {
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
	if code, err := as.GetCode(address1); err != nil || !bytes.Equal(code, []byte{1, 2, 3}) {
		t.Errorf("Unexpected value or err, val: %v != %v, err:  %v", code, []byte{1, 2, 3}, err)
	}
}

// createDatabase creates a state with the given name and directory
func createDatabase(t *testing.T, name, dir string) database.Database {
	for _, config := range getDatabaseConfigurations() {
		if config.name() == name {
			state, err := config.createDatabase(dir)
			if err != nil {
				t.Fatalf("Cannot init database: %s, err: %v", name, err)
			}
			return state
		}
	}

	t.Fatalf("Configuration with name %s not found", name)
	return nil
}
