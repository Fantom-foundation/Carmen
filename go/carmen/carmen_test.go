package carmen

import (
	"errors"
	"math/big"
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

func TestCarmen_Archive_And_Live_Must_Be_InSync(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	addBlock := func(block uint64, db Database) {
		if err := db.AddBlock(block, func(context HeadBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				context.CreateAccount(Address{byte(block)})
				context.AddBalance(Address{byte(block)}, big.NewInt(int64(block)))
				return nil
			}); err != nil {
				t.Fatalf("cannot create transaction: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	const blocks = 10
	for i := 0; i < blocks; i++ {
		addBlock(uint64(i), db)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// open as non-archive
	noArchiveConfig := Configuration{
		Variant: testConfig.Variant,
		Schema:  testConfig.Schema,
		Archive: Archive(state.NoArchive),
	}

	db, err = OpenDatabase(dir, noArchiveConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	for i := 0; i < blocks; i++ {
		addBlock(uint64(i+blocks), db)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// opening archive should fail as archive and non-archive is not in-sync
	if _, err := OpenDatabase(dir, testConfig, nil); err == nil {
		t.Errorf("opening database should fail")
	}
}

func TestCarmen_Empty_Archive_And_Live_Must_Be_InSync(t *testing.T) {

	dir := t.TempDir()
	// open as non-archive
	noArchiveConfig := Configuration{
		Variant: testConfig.Variant,
		Schema:  testConfig.Schema,
		Archive: Archive(state.NoArchive),
	}

	db, err := OpenDatabase(dir, noArchiveConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	const blocks = 10
	for i := 0; i < blocks; i++ {
		if err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				context.CreateAccount(Address{byte(i)})
				context.AddBalance(Address{byte(i)}, big.NewInt(int64(i)))
				return nil
			}); err != nil {
				t.Fatalf("cannot create transaction: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("cannot close database: %v", err)
	}

	// opening archive should fail as archive and non-archive is not in-sync
	if _, err := OpenDatabase(dir, testConfig, nil); err == nil {
		t.Errorf("opening database should fail")
	}
}
