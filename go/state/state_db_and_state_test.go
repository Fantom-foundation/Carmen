// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package state_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/state"

	_ "github.com/Fantom-foundation/Carmen/go/state/cppstate"
	_ "github.com/Fantom-foundation/Carmen/go/state/gostate"
)

func TestCarmen_CanHandleMaximumBalance(t *testing.T) {
	addr1 := common.Address{1}
	addr2 := common.Address{2}
	addr3 := common.Address{3}

	minBalance := amount.New()
	maxBalance := amount.Max()

	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			store, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			db := state.CreateCustomStateDBUsing(store, 100000)
			defer db.Close()

			// First block: set up some balances.
			db.BeginBlock()
			db.BeginTransaction()
			db.SetNonce(addr1, 1)
			db.SetNonce(addr2, 1)
			db.SetNonce(addr3, 1)
			db.AddBalance(addr1, minBalance)
			db.AddBalance(addr2, minBalance)
			db.AddBalance(addr3, maxBalance)
			db.EndTransaction()
			db.EndBlock(1)

			// Second block: check balances and modify them.
			db.BeginBlock()
			db.BeginTransaction()

			if want, got := minBalance, db.GetBalance(addr1); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}
			if want, got := minBalance, db.GetBalance(addr2); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}
			if want, got := maxBalance, db.GetBalance(addr3); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}

			db.AddBalance(addr2, maxBalance)
			db.SubBalance(addr3, maxBalance)

			db.EndTransaction()
			db.EndBlock(2)

			// Third block: check modified balances.
			db.BeginBlock()
			db.BeginTransaction()

			if want, got := minBalance, db.GetBalance(addr1); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}
			if want, got := maxBalance, db.GetBalance(addr2); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}
			if want, got := minBalance, db.GetBalance(addr3); want != got {
				t.Errorf("unexpected balance, wanted %v, got %v", want, got)
			}

			db.EndTransaction()
			db.EndBlock(3)

			if err := db.Flush(); err != nil {
				t.Fatalf("failed to flush the DB: %v", err)
			}

			// Ignore cases without an archive.
			if config.config.Archive == state.NoArchive {
				return
			}

			// Check that the archives managed to record the balances.
			expectations := []struct {
				block   uint64
				account common.Address
				balance amount.Amount
			}{
				{1, addr1, minBalance},
				{1, addr2, minBalance},
				{1, addr3, maxBalance},
				{2, addr1, minBalance},
				{2, addr2, maxBalance},
				{2, addr3, minBalance},
				{3, addr1, minBalance},
				{3, addr2, maxBalance},
				{3, addr3, minBalance},
			}

			for _, expectation := range expectations {
				block, err := store.GetArchiveState(expectation.block)
				if err != nil {
					t.Fatalf("failed to fetch block %d from archive: %v", expectation.block, err)
				}

				got, err := block.GetBalance(expectation.account)
				if err != nil {
					t.Errorf("failed to fetch balance for account %v: %v", expectation.account, err)
				}

				if got != expectation.balance {
					t.Errorf("unexpected balance of account %v at block %d: wanted %v, got %v",
						expectation.account,
						expectation.block,
						expectation.balance,
						got,
					)
				}
			}

		})
	}
}

func TestCarmenThereCanBeMultipleBulkLoadPhasesOnRealState(t *testing.T) {
	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			store, err := config.createState(dir)
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("failed to initialize state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
				}
			}
			db := state.CreateCustomStateDBUsing(store, 100000)
			defer db.Close()

			for i := 0; i < 10; i++ {
				load := db.StartBulkLoad(uint64(i))
				load.CreateAccount(address1)
				load.SetNonce(address1, uint64(i))
				if err := load.Close(); err != nil {
					t.Errorf("bulk-insert failed: %v", err)
				}
			}
		})
	}
}

func TestCarmenBulkLoadsCanBeInterleavedWithRegularUpdates(t *testing.T) {
	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			store, err := config.createState(dir)
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("failed to initialize state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
				}
			}
			db := state.CreateCustomStateDBUsing(store, 100000)
			defer db.Close()

			for i := 0; i < 5; i++ {
				// Run a bulk-load update (creates one block)
				load := db.StartBulkLoad(uint64(i * 2))
				load.CreateAccount(address1)
				load.SetNonce(address1, uint64(i))
				if err := load.Close(); err != nil {
					t.Errorf("bulk-insert failed: %v", err)
				}

				// Run a regular block.
				db.BeginBlock()
				db.BeginTransaction()
				if !db.Exist(address1) {
					t.Errorf("account 1 should exist")
				}
				db.Suicide(address1)
				db.EndTransaction()
				db.EndBlock(uint64(i*2 + 1))
			}
		})
	}
}

func testCarmenStateDbHashAfterModification(t *testing.T, mod func(s state.StateDB)) {
	want := map[state.Schema]common.Hash{}
	for _, s := range getAllSchemas() {
		ref_state, err := getReferenceStateFor(t, state.Parameters{
			Schema:  s,
			Archive: state.NoArchive,
		})
		if err != nil {
			t.Fatalf("failed to create reference state: %v", err)
		}
		ref := state.CreateCustomStateDBUsing(ref_state, 100000)
		defer ref.Close()
		mod(ref)
		ref.EndTransaction()
		ref.EndBlock(1)
		want[s] = ref.GetHash()
	}
	for i := 0; i < 3; i++ {
		for _, config := range initStates() {
			config := config
			t.Run(fmt.Sprintf("%v/run=%d", config.name(), i), func(t *testing.T) {
				t.Parallel()
				store, err := config.createState(t.TempDir())
				if err != nil {
					if errors.Is(err, UnsupportedConfiguration) {
						t.Skipf("failed to initialize state %s: %v", config.name(), err)
					} else {
						t.Fatalf("failed to initialize state %s: %v", config.name(), err)
					}
				}
				stateDb := state.CreateCustomStateDBUsing(store, 100000)
				defer stateDb.Close()

				mod(stateDb)
				stateDb.EndTransaction()
				stateDb.EndBlock(1)
				if got := stateDb.GetHash(); want[config.config.Schema] != got {
					t.Errorf("Invalid hash, wanted %v, got %v", want, got)
				}
			})
		}
	}
}

func TestCarmenStateHashIsDeterministicForEmptyState(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		// nothing
	})
}

func TestCarmenStateHashIsDeterministicForSingleUpdate(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.SetState(address1, key1, val1)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleUpdate(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.SetState(address1, key1, val1)
		s.SetState(address2, key2, val2)
		s.SetState(address3, key3, val3)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleAccountCreations(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleAccountModifications(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.Suicide(address2)
		s.Suicide(address1)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleBalanceUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.AddBalance(address1, amount.New(12))
		s.AddBalance(address2, amount.New(14))
		s.AddBalance(address3, amount.New(16))
		s.SubBalance(address3, amount.New(8))
	})
}

func TestCarmenStateHashIsDeterministicForMultipleNonceUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.SetNonce(address1, 12)
		s.SetNonce(address2, 14)
		s.SetNonce(address3, 18)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleCodeUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s state.StateDB) {
		s.SetCode(address1, []byte{0xAC})
		s.SetCode(address2, []byte{0xDC})
		s.SetCode(address3, []byte{0x20})
	})
}

const numSlots = 1000

// TestPersistentStateDB modifies stateDB first, then it is closed and is re-opened in another process,
// and it is tested that data are available, i.e. all was successfully persisted
func TestPersistentStateDB(t *testing.T) {
	for _, config := range initStates() {
		// skip in-memory
		if strings.HasPrefix(config.name(), "cpp-memory") || strings.HasPrefix(config.name(), "go-memory") {
			continue
		}
		// skip setups without archive
		if config.config.Archive == state.NoArchive {
			continue
		}
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("failed to initialize state %s: %v", t.Name(), err)
				} else {
					t.Fatalf("failed to initialize state %s: %v", t.Name(), err)
				}
			}

			stateDb := state.CreateStateDBUsing(s)

			stateDb.BeginEpoch()
			stateDb.BeginBlock()
			stateDb.BeginTransaction()

			// init state DB data
			stateDb.CreateAccount(address1)
			stateDb.AddBalance(address1, amount.New(153))
			stateDb.SetNonce(address1, 58)
			stateDb.SetCode(address1, []byte{1, 2, 3})

			// insert number of slots to address 1
			for i := 0; i < numSlots; i++ {
				val := toVal(uint64(i))
				stateDb.SetState(address1, toKey(uint64(i)), val)
			}

			stateDb.EndTransaction()
			stateDb.EndBlock(1)
			stateDb.BeginBlock()
			stateDb.BeginTransaction()

			stateDb.CreateAccount(address2)
			stateDb.AddBalance(address2, amount.New(6789))
			stateDb.SetNonce(address2, 91)
			stateDb.SetCode(address2, []byte{3, 2, 1})

			// insert number of slots to address 2
			for i := 0; i < numSlots; i++ {
				val := toVal(uint64(i + numSlots))
				stateDb.SetState(address2, toKey(uint64(i)), val)
			}

			stateDb.EndTransaction()
			stateDb.EndBlock(2)
			stateDb.EndEpoch(1)

			if err := stateDb.Close(); err != nil {
				t.Errorf("Cannot close state: %v", err)
			}

			execSubProcessTest(t, dir, config.name(), "TestStateDBRead")
		})
	}
}

// TestStateDBRead verifies data are available in a stateDB.
// The given state reads the data from the given directory and verifies the data are present.
// Name of the index and directory is provided as command line arguments
func TestStateDBRead(t *testing.T) {
	// do not run this test stand-alone
	if *stateDir == "DEFAULT" {
		return
	}

	s := createState(t, *stateImpl, *stateDir)
	defer func() {
		_ = s.Close()
	}()

	stateDb := state.CreateStateDBUsing(s)

	if state := stateDb.Exist(address1); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}
	if state := stateDb.Exist(address2); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}

	if balance := stateDb.GetBalance(address1); balance != amount.New(153) {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := stateDb.GetBalance(address2); balance != amount.New(6789) {
		t.Errorf("Unexpected value, val: %v != %v", balance, 6789)
	}

	if nonce := stateDb.GetNonce(address1); nonce != 58 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 58)
	}
	if nonce := stateDb.GetNonce(address2); nonce != 91 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 91)
	}

	if code := stateDb.GetCode(address1); !bytes.Equal(code, []byte{1, 2, 3}) {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{1, 2, 3})
	}
	if code := stateDb.GetCode(address2); !bytes.Equal(code, []byte{3, 2, 1}) {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{3, 2, 1})
	}

	// slots in address 1
	for i := 0; i < numSlots; i++ {
		val := toVal(uint64(i))
		key := toKey(uint64(i))
		if storage := stateDb.GetState(address1, key); storage != val {
			t.Fatalf("Unexpected value, val: %v != %v", storage, val)
		}
	}

	// slots in address 2
	for i := 0; i < numSlots; i++ {
		val := toVal(uint64(i + numSlots))
		key := toKey(uint64(i))
		if storage := stateDb.GetState(address2, key); storage != val {
			t.Errorf("Unexpected value, val: %v != %v", storage, val)
		}
	}

	// state in archive
	as1, err := stateDb.GetArchiveStateDB(1)
	if as1 == nil || err != nil {
		t.Fatalf("Unable to get archive stateDB, err: %v", err)
	}
	if state := as1.Exist(address1); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}
	if state := as1.Exist(address2); state != false {
		t.Errorf("Unexpected value, val: %v != %v", state, false)
	}
	if balance := as1.GetBalance(address1); balance != amount.New(153) {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := as1.GetBalance(address2); !balance.IsZero() {
		t.Errorf("Unexpected value, val: %v != %v", balance, 0)
	}

	as2, err := stateDb.GetArchiveStateDB(2)
	if as2 == nil || err != nil {
		t.Fatalf("Unable to get archive stateDB, err: %v", err)
	}
	if state := as2.Exist(address1); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}
	if state := as2.Exist(address2); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}
	if balance := as2.GetBalance(address1); balance != amount.New(153) {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := as2.GetBalance(address2); balance != amount.New(6789) {
		t.Errorf("Unexpected value, val: %v != %v", balance, 6789)
	}
	if nonce := as2.GetNonce(address1); nonce != 58 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 58)
	}
	if nonce := as2.GetNonce(address2); nonce != 91 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 91)
	}
	if code := as2.GetCode(address1); !bytes.Equal(code, []byte{1, 2, 3}) {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{1, 2, 3})
	}
	if code := as2.GetCode(address2); !bytes.Equal(code, []byte{3, 2, 1}) {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{3, 2, 1})
	}
}

func TestStateDBArchive(t *testing.T) {

	for _, config := range initStates() {
		// skip configurations without an archive
		if config.config.Archive == state.NoArchive {
			continue
		}
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skipf("failed to initialize state %s; %s", config.name(), err)
				} else {
					t.Fatalf("failed to initialize state %s; %s", config.name(), err)
				}
			}
			defer s.Close()
			stateDb := state.CreateStateDBUsing(s)

			stateDb.AddBalance(address2, amount.New(22))

			bl := stateDb.StartBulkLoad(0)
			bl.CreateAccount(address1)
			bl.SetBalance(address1, amount.New(12))
			if err := bl.Close(); err != nil {
				t.Fatalf("failed to bulk-load StateDB with archive; %s", err)
			}

			stateDb.BeginBlock()
			stateDb.AddBalance(address1, amount.New(22))
			stateDb.EndBlock(2)

			if err := stateDb.Flush(); err != nil { // wait until archives are written
				t.Fatalf("failed to flush StateDB; %s", err)
			}

			state1, err := stateDb.GetArchiveStateDB(1)
			if err != nil {
				t.Fatalf("failed to get state of block 1; %s", err)
			}

			state2, err := stateDb.GetArchiveStateDB(2)
			if err != nil {
				t.Fatalf("failed to get state of block 2; %s", err)
			}

			if exist := state1.Exist(address1); err != nil || exist != true {
				t.Errorf("invalid account state at block 1: %t", exist)
			}
			if exist := state2.Exist(address1); err != nil || exist != true {
				t.Errorf("invalid account state at block 2: %t", exist)
			}
			if balance := state1.GetBalance(address1); balance != amount.New(12) {
				t.Errorf("invalid balance at block 1: %s", balance)
			}
			if balance := state2.GetBalance(address1); balance != amount.New(34) {
				t.Errorf("invalid balance at block 2: %s", balance)
			}
		})
	}
}

func TestStateDBSupportsConcurrentAccesses(t *testing.T) {
	const N = 10  // number of concurrent goroutines
	const M = 100 // number of updates per goroutine
	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			defer func() {
				s.Close()
			}()

			// Have multiple goroutines access the state concurrently.
			ready := sync.WaitGroup{}
			ready.Add(N)
			done := sync.WaitGroup{}
			done.Add(N)
			for i := 0; i < N; i++ {
				isPrimary := i == 0
				go func() {
					defer done.Done()
					// Create a state and wait for other go-routines to be ready.
					var stateDb state.VmStateDB
					if isPrimary {
						stateDb = state.CreateStateDBUsing(s)
					} else {
						stateDb = state.CreateNonCommittableStateDBUsing(s)
					}
					ready.Done()
					ready.Wait()

					// Perform concurrent accesses.
					block := 0
					for j := 0; j < M; j++ {
						if isPrimary {
							stateDb.(state.StateDB).BeginBlock()
						}
						stateDb.BeginTransaction()
						// Perform a read + update operation.
						stateDb.AddBalance(address1, amount.New(1))
						stateDb.EndTransaction()
						if isPrimary {
							stateDb.(state.StateDB).EndBlock(uint64(block))
							block++
						} else {
							stateDb.(state.NonCommittableStateDB).Release()
							stateDb = state.CreateNonCommittableStateDBUsing(s)
						}
					}
				}()
			}
			done.Wait()

			balance, err := s.GetBalance(address1)
			if err != nil {
				t.Fatalf("reading the final balance failed")
			}
			if got, want := balance, amount.New(M); got != want {
				t.Fatalf("invalid final balance, wanted %d, got %d", want, got)
			}
		})
	}
}

func toVal(key uint64) common.Value {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint64(keyBytes, key)
	return common.ValueSerializer{}.FromBytes(keyBytes)
}

func toKey(key uint64) common.Key {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint64(keyBytes, key)
	return common.KeySerializer{}.FromBytes(keyBytes)
}
