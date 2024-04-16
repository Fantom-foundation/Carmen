//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package carmen

import (
	"errors"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/Fantom-foundation/Carmen/go/state/gostate"
)

var testConfig = Configuration{
	Variant: Variant(gostate.VariantGoMemory),
	Schema:  5,
	Archive: Archive(state.S5Archive),
}

var testNonArchiveConfig = Configuration{
	Variant: Variant(gostate.VariantGoMemory),
	Schema:  5,
	Archive: Archive(state.NoArchive),
}

func TestCarmen_DatabaseLiveCycle(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestCarmen_BlockProcessing(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)

	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	block, err := db.BeginBlock(1)
	if err != nil {
		t.Fatalf("failed to start block: %v", err)
	}

	tx, err := block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	tx.CreateAccount(Address{1})
	tx.SetNonce(Address{1}, 12)
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to finish transaction: %v", err)
	}

	tx, err = block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	if want, got := uint64(12), tx.GetNonce(Address{1}); want != got {
		t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to finish transaction: %v", err)
	}

	if err := block.Commit(); err != nil {
		t.Fatalf("failed to commit block changes: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestCarmen_HeadBlockQuery(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	getNonce := func() uint64 {
		res := uint64(0)
		err := db.QueryHeadState(func(ctxt QueryContext) {
			res = ctxt.GetNonce(Address{})
		})
		if err != nil {
			t.Fatalf("failed to retrieve nonce: %v", err)
		}
		return res
	}

	block := uint64(0)
	incrementNonce := func() {
		err := db.AddBlock(block, func(ctxt HeadBlockContext) error {
			return ctxt.RunTransaction(func(ctxt TransactionContext) error {
				nonce := ctxt.GetNonce(Address{})
				ctxt.SetNonce(Address{}, nonce+1)
				return nil
			})
		})
		if err != nil {
			t.Fatalf("failed to increment nonce: %v", err)
		}
		block++
	}

	if want, got := uint64(0), getNonce(); want != got {
		t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
	}

	incrementNonce()

	if want, got := uint64(1), getNonce(); want != got {
		t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
	}

	incrementNonce()

	if want, got := uint64(2), getNonce(); want != got {
		t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
	}
}

func TestCarmen_ArchiveQuery(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Insert content into DB using functional interface.
	err = errors.Join(
		db.AddBlock(1, func(ctxt HeadBlockContext) error {
			return errors.Join(
				ctxt.RunTransaction(func(ctxt TransactionContext) error {
					ctxt.CreateAccount(Address{1})
					ctxt.SetNonce(Address{1}, 12)
					return nil
				}),
				ctxt.RunTransaction(func(ctxt TransactionContext) error {
					ctxt.CreateAccount(Address{2})
					ctxt.SetNonce(Address{2}, 14)
					return nil
				}),
			)
		}),
		db.AddBlock(3, func(ctxt HeadBlockContext) error {
			return errors.Join(
				ctxt.RunTransaction(func(ctxt TransactionContext) error {
					ctxt.CreateAccount(Address{3})
					ctxt.SetNonce(Address{3}, 16)
					return nil
				}),
			)
		}),
	)
	if err != nil {
		t.Fatalf("failed to add content to database: %v", err)
	}

	// Make sure the archive is synced (is updated asynchronously).
	if err := db.Flush(); err != nil {
		t.Errorf("failed to flush database: %v", err)
	}

	// Query archive height.
	height, err := db.GetArchiveBlockHeight()
	if err != nil {
		t.Errorf("failed to fetch block height: %v", err)
	}
	if want, got := int64(3), height; want != got {
		t.Fatalf("unexpected block height, wanted %d, got %d", want, got)
	}

	// Query archive state information using explicit contexts.
	block, err := db.GetHistoricContext(2)
	if err != nil {
		t.Fatalf("failed to get historic block context: %v", err)
	}
	transaction, err := block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start transaction in historic block: %v", err)
	}
	if want, got := uint64(12), transaction.GetNonce(Address{1}); want != got {
		t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
	}
	if err := transaction.Abort(); err != nil {
		t.Errorf("error aborting transaction: %v", err)
	}
	if err := block.Close(); err != nil {
		t.Errorf("error closing historic context")
	}

	// Query archive state information (functional style).
	err = errors.Join(
		db.QueryBlock(0, func(ctxt HistoricBlockContext) error {
			return ctxt.RunTransaction(func(ctxt TransactionContext) error {
				if want, got := uint64(0), ctxt.GetNonce(Address{1}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(0), ctxt.GetNonce(Address{2}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(0), ctxt.GetNonce(Address{3}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				return nil
			})
		}),
		db.QueryBlock(1, func(ctxt HistoricBlockContext) error {
			return ctxt.RunTransaction(func(ctxt TransactionContext) error {
				if want, got := uint64(12), ctxt.GetNonce(Address{1}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(14), ctxt.GetNonce(Address{2}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(0), ctxt.GetNonce(Address{3}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				return nil
			})
		}),
		db.QueryBlock(2, func(ctxt HistoricBlockContext) error {
			return ctxt.RunTransaction(func(ctxt TransactionContext) error {
				if want, got := uint64(12), ctxt.GetNonce(Address{1}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(14), ctxt.GetNonce(Address{2}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(0), ctxt.GetNonce(Address{3}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				return nil
			})
		}),
		db.QueryBlock(3, func(ctxt HistoricBlockContext) error {
			return ctxt.RunTransaction(func(ctxt TransactionContext) error {
				if want, got := uint64(12), ctxt.GetNonce(Address{1}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(14), ctxt.GetNonce(Address{2}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				if want, got := uint64(16), ctxt.GetNonce(Address{3}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
				return nil
			})
		}),
	)
	if err != nil {
		t.Fatalf("error during queries: %v", err)
	}
}
