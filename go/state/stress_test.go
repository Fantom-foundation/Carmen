//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

//go:build stress_test
// +build stress_test

package state_test

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

func TestStress_CanHandleLargeBlock(t *testing.T) {
	const N = 1_000_000 // the number of changes in a single block
	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			// to safe processing time only S5 is tested
			if config.config.Schema != 5 {
				t.Skip()
			}
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			db := state.CreateStateDBUsing(s)

			db.BeginBlock()
			db.BeginTransaction()

			// Create a lot of accounts.
			for i := 0; i < N; i++ {
				addr := common.Address{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
				db.CreateAccount(addr)
				db.SetNonce(addr, 12)
			}

			db.EndTransaction()
			db.EndBlock(12)

			if err := db.Check(); err != nil {
				t.Errorf("update failed with unexpected error: %v", err)
			}

			if err := db.Close(); err != nil {
				t.Errorf("failed to close DB: %v", err)
			}
		})
	}
}

func TestStress_CanHandleDeleteOfLargeAccount(t *testing.T) {
	// the number of slots in the account (larger than what could be filled in a single block)
	const N = 10_000_000
	for _, config := range initStates() {
		config := config
		t.Run(config.name(), func(t *testing.T) {
			// to safe processing time only S5 is tested
			if config.config.Schema != 5 {
				t.Skip()
			}
			t.Parallel()
			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s; %s", config.name(), err)
			}
			db := state.CreateStateDBUsing(s)

			addr := address1

			// Fill an account with N slots.
			db.BeginBlock()
			db.BeginTransaction()
			db.CreateAccount(addr)
			db.SetNonce(addr, 5)
			db.EndTransaction()
			db.EndBlock(0)

			// Fill slots in batches.
			block := uint64(1)
			counter := uint64(0)
			for counter < N {
				db.BeginBlock()
				db.BeginTransaction()
				for i := 0; i < 1000; i++ {
					db.SetState(addr, toKey(counter), val1)
					counter++
				}
				db.EndTransaction()
				db.EndBlock(block)
				block++
			}

			// have a final block deleting one large account
			db.BeginBlock()
			db.BeginTransaction()
			db.Suicide(addr)
			db.EndTransaction()
			db.EndBlock(block)
			block++

			db.BeginBlock()
			db.BeginTransaction()
			if want, got := false, db.Exist(addr); want != got {
				t.Errorf("unexpected exist result, wanted %v, got %v", want, got)
			}
			db.EndTransaction()
			db.EndBlock(block)
			block++

			if err := db.Check(); err != nil {
				t.Errorf("update failed with unexpected error: %v", err)
			}

			if err := db.Close(); err != nil {
				t.Errorf("failed to close DB: %v", err)
			}
		})
	}
}
