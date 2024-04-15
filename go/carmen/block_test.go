//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package carmen

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

func TestBlockContext_CanCreateSequenceOfTransactions(t *testing.T) {
	for name, factory := range initBlockContexts() {
		t.Run(name, func(t *testing.T) {
			block := factory(t)
			for i := 0; i < 10; i++ {
				tx, err := block.BeginTransaction()
				if err != nil {
					t.Fatalf("failed to create transaction %d: %v", i, err)
				}
				if i%2 == 0 {
					if err := tx.Abort(); err != nil {
						t.Fatalf("failed to abort transaction %d: %v", i, err)
					}
				} else {
					if err := tx.Commit(); err != nil {
						t.Fatalf("failed to commit transaction %d: %v", i, err)
					}
				}
			}
		})
	}
}

func TestBlockContext_CanOnlyCreateOneTransactionAtATime(t *testing.T) {
	for name, factory := range initBlockContexts() {
		t.Run(name, func(t *testing.T) {
			block := factory(t)
			tx, err := block.BeginTransaction()
			if err != nil {
				t.Fatalf("failed to create transaction: %v", err)
			}

			_, err = block.BeginTransaction()
			if err == nil {
				t.Fatalf("expected an error, got nothing")
			}

			if err := tx.Abort(); err != nil {
				t.Fatalf("failed to abort transaction: %v", err)
			}
		})
	}
}

func TestBlockContext_RunTransaction_Parallel(t *testing.T) {
	for name, factory := range initBlockContexts() {
		t.Run(name, func(t *testing.T) {
			block := factory(t)
			const loops = 100
			success := &atomic.Int32{}
			wg := &sync.WaitGroup{}
			wg.Add(loops)

			// non-sync counter as only one txs can run at a time
			var counter int32
			for i := 0; i < loops; i++ {
				go func(i int) {
					defer wg.Done()
					if err := block.RunTransaction(func(context TransactionContext) error {
						counter++
						return nil
					}); err == nil {
						success.Add(1)
					}
				}(i)
			}

			wg.Wait()

			if success.Load() == 0 {
				t.Errorf("no successful transaction")
			}
			if got, want := success.Load(), counter; got != want {
				t.Errorf("counters do not match: %d != %d", got, want)
			}
		})
	}
}

func TestBlockContext_BeginTransaction_Parallel(t *testing.T) {
	for name, factory := range initBlockContexts() {
		t.Run(name, func(t *testing.T) {
			block := factory(t)
			const loops = 100
			success := &atomic.Int32{}
			wg := &sync.WaitGroup{}
			wg.Add(loops)

			for i := 0; i < loops; i++ {
				go func(i int) {
					defer wg.Done()
					ctx, err := block.BeginTransaction()
					if err == nil {
						success.Add(1)
						if err := ctx.Commit(); err != nil {
							t.Errorf("cannot commit transaction: %v", err)
						}
					}
				}(i)
			}

			wg.Wait()

			if success.Load() == 0 {
				t.Errorf("no successful transaction")
			}
		})
	}
}

func TestHeadBlockContext_RunTransaction_ProducesError(t *testing.T) {
	for name, factory := range initBlockContexts() {
		t.Run(name, func(t *testing.T) {
			block := factory(t)

			injectedErr := fmt.Errorf("injectedError")
			if err := block.RunTransaction(func(context TransactionContext) error {
				return injectedErr
			}); !errors.Is(err, injectedErr) {
				t.Errorf("running transaction should fail: %v", err)
			}
		})
	}
}

func TestHeadBlockContext_BeginTransaction_ClosedBlock_Fail(t *testing.T) {
	block := initHeadBlockContext(t)

	if err := block.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if _, err := block.BeginTransaction(); err == nil {
		t.Errorf("starting transaction on closed block should fail")
	}
}

func TestHeadBlockContext_RunTransaction_ClosedBlock_Fail(t *testing.T) {
	block := initHeadBlockContext(t)

	if err := block.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := block.RunTransaction(func(context TransactionContext) error {
		return nil
	}); err == nil {
		t.Errorf("starting transaction on closed block should fail")
	}
}

func TestHeadBlockContext_CannotCommitTwice(t *testing.T) {
	block := initHeadBlockContext(t)

	if err := block.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := block.Commit(); err == nil {
		t.Errorf("second commit should fail")
	}
}

func TestHeadBlockContext_Abort_After_Commit_Noop(t *testing.T) {
	block := initHeadBlockContext(t)

	if err := block.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := block.Abort(); err != nil {
		t.Errorf("abort after commit should be a no-op: %v", err)
	}
}

func TestHeadBlockContext_CannotCommit_WhenTransactionRunning(t *testing.T) {
	block := initHeadBlockContext(t)
	tx, err := block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	if err := block.Commit(); !errors.Is(err, errTransactionRunning) {
		t.Errorf("commit should fail when transaction is not commited")
	}

	if err := tx.Abort(); err != nil {
		t.Errorf("cannot abort transaction: %v", err)
	}

	// can commit now
	if err := block.Commit(); err != nil {
		t.Errorf("cannot commit block: %v", err)
	}
}

func TestHistoricBlockContext_BeginTransaction_ClosedBlock_Fail(t *testing.T) {
	block := initHistoricBlockContext(t)

	if err := block.Close(); err != nil {
		t.Fatalf("cannot close block: %v", err)
	}

	if _, err := block.BeginTransaction(); err == nil {
		t.Errorf("starting transaction on closed block should fail")
	}
}

func TestHistoricBlockContext_RunTransaction_ClosedBlock_Fail(t *testing.T) {
	block := initHistoricBlockContext(t)

	if err := block.Close(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := block.RunTransaction(func(context TransactionContext) error {
		return nil
	}); err == nil {
		t.Errorf("starting transaction on closed block should fail")
	}
}

func TestHistoricBlockContext_SecondClose_Noop(t *testing.T) {
	block := initHistoricBlockContext(t)

	if err := block.Close(); err != nil {
		t.Fatalf("cannot close block: %v", err)
	}

	if err := block.Close(); err != nil {
		t.Fatalf("cannot close block: %v", err)
	}
}

func TestDatabase_AbortedBlocksHaveNoEffect(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close database: %v", err)
		}
	}()

	block, err := db.BeginBlock(1)
	if err != nil {
		t.Fatalf("failed to start new block: %v", err)
	}

	tx, err := block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start a transaction: %v", err)
	}
	tx.SetNonce(Address{}, 12)
	if err := tx.Commit(); err != nil {
		t.Fatalf("unexpected error during commit: %v", err)
	}
	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort block: %v", err)
	}

	block, err = db.BeginBlock(1)
	if err != nil {
		t.Fatalf("failed to start new block: %v", err)
	}

	tx, err = block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start second transaction: %v", err)
	}
	if want, got := uint64(0), tx.GetNonce(Address{}); want != got {
		t.Errorf("unexpected result after abort, want %d, got %d", want, got)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("unexpected error during commit: %v", err)
	}
	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort block: %v", err)
	}

}

func TestBlockContext_PanickingCommitsReleaseQueryLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := state.NewMockStateDB(ctrl)
	context := &headBlockContext{
		commonContext: commonContext{
			db: &database{},
		},
		state: state,
	}

	injectedPanic := "injected panic"
	state.EXPECT().EndBlock(gomock.Any()).Do(func(any) {
		panic(injectedPanic)
	})

	defer func() {
		msg := recover()
		if got, want := msg, injectedPanic; got != want {
			t.Fatalf("unexpected panic, wanted %v, got %v", want, got)
		}
		if !context.db.headStateCommitLock.TryLock() {
			t.Errorf("commit lock was not released after panic, which may lead to a deadlock")
		} else {
			context.db.headStateCommitLock.Unlock()
		}
	}()

	context.Commit()
}

func initBlockContexts() map[string]func(t *testing.T) blockContext {
	return map[string]func(t *testing.T) blockContext{
		"headBlockContext": func(t *testing.T) blockContext {
			return initHeadBlockContext(t)
		},
		"historicBlockContext": func(t *testing.T) blockContext {
			return initHistoricBlockContext(t)
		},
	}
}

func initHeadBlockContext(t *testing.T) HeadBlockContext {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	stateDB.EXPECT().Check().Return(nil).AnyTimes()
	stateDB.EXPECT().EndTransaction().AnyTimes()
	stateDB.EXPECT().EndBlock(gomock.Any()).AnyTimes()
	stateDB.EXPECT().AbortTransaction().AnyTimes()
	stateDB.EXPECT().ResetBlockContext().AnyTimes()
	st := state.NewMockState(ctrl)

	return &headBlockContext{
		commonContext: commonContext{
			db: &database{
				db: st,
			},
		},
		state: stateDB,
	}
}

func initHistoricBlockContext(t *testing.T) HistoricBlockContext {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockNonCommittableStateDB(ctrl)
	stateDB.EXPECT().Check().Return(nil).AnyTimes()
	stateDB.EXPECT().EndTransaction().AnyTimes()
	stateDB.EXPECT().Release().AnyTimes()
	stateDB.EXPECT().AbortTransaction().AnyTimes()

	return &archiveBlockContext{
		commonContext: commonContext{
			db: &database{},
		},
		state: stateDB,
	}
}
