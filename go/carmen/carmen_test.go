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

	tx, err := block.BeginTransaction(1)
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	tx.CreateAccount(Address{1})
	tx.SetNonce(Address{1}, 12)
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to finish transaction: %v", err)
	}

	tx, err = block.BeginTransaction(2)
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

func TestCarmen_ArchiveQuery(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Insert content into DB using functional interface.
	err = errors.Join(
		db.AddBlock(1, func(ctxt HeadBlockContext) error {
			return errors.Join(
				ctxt.RunTransaction(1, func(ctxt TransactionContext) error {
					ctxt.CreateAccount(Address{1})
					ctxt.SetNonce(Address{1}, 12)
					return nil
				}),
				ctxt.RunTransaction(3, func(ctxt TransactionContext) error {
					ctxt.CreateAccount(Address{2})
					ctxt.SetNonce(Address{2}, 14)
					return nil
				}),
			)
		}),
		db.AddBlock(3, func(ctxt HeadBlockContext) error {
			return errors.Join(
				ctxt.RunTransaction(1, func(ctxt TransactionContext) error {
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
	height, err := db.GetBlockHeight()
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
	transaction, err := block.BeginTransaction(0)
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
			return ctxt.RunTransaction(0, func(ctxt TransactionContext) error {
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
			return ctxt.RunTransaction(0, func(ctxt TransactionContext) error {
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
			return ctxt.RunTransaction(0, func(ctxt TransactionContext) error {
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
			return ctxt.RunTransaction(0, func(ctxt TransactionContext) error {
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
