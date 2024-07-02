// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

// openTestDatabase creates database with test configuration in a test directory.
func openTestDatabase(t *testing.T) (Database, error) {
	return OpenDatabase(t.TempDir(), testConfig, testProperties)
}

func TestDatabase_OpenWorksForFreshDirectory(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_OpenFailsForInvalidDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "some_file.dat")
	if err := os.WriteFile(path, []byte("hello"), 0600); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	_, err := OpenDatabase(path, testConfig, testProperties)
	if err == nil {
		t.Fatalf("expected an error, got nothing")
	}
}

func TestDatabase_CloseTwice_SecondCallFails(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
	if err := db.Close(); err == nil {
		t.Fatalf("closing already closed database should fail")
	}
}

func TestDatabase_CloseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	stateDB.EXPECT().Close().AnyTimes().Return(injectedErr)
	stateDB.EXPECT().Flush().AnyTimes().Return(nil)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if err := db.Close(); !errors.Is(err, injectedErr) {
		t.Errorf("unexpected error: %v != %v", err, injectedErr)
	}
}

func TestDatabase_FlushFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	stateDB.EXPECT().Flush().AnyTimes().Return(injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if err := db.Flush(); !errors.Is(err, injectedErr) {
		t.Errorf("unexpected error: %s != %v", err, injectedErr)
	}

	if err := db.Close(); !errors.Is(err, injectedErr) {
		t.Errorf("unexpected error: %s != %v", err, injectedErr)
	}
}

func TestDatabase_QueryBlock_UnderlyingDB_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	state.EXPECT().GetArchiveState(gomock.Any()).Return(nil, injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if err := db.QueryBlock(0, func(context HistoricBlockContext) error {
		return nil
	}); !errors.Is(err, injectedErr) {
		t.Errorf("archive query should fail")
	}
}

func TestDatabase_QueryHeadState_UnderlyingDBQuery_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	state.EXPECT().GetHash().Return(common.Hash{}, injectedErr)
	state.EXPECT().Check()

	db := &database{
		db:    state,
		state: stateDB,
	}

	err := db.QueryHeadState(func(context QueryContext) {
		context.GetStateHash()
	})
	if !errors.Is(err, injectedErr) {
		t.Errorf("head state query should have failed, got %v", err)
	}
}

func TestDatabase_QueryHeadState_UnderlyingDB_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	state.EXPECT().GetHash().Return(common.Hash{}, nil)
	state.EXPECT().Check().Return(injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	err := db.QueryHeadState(func(context QueryContext) {
		context.GetStateHash()
	})
	if !errors.Is(err, injectedErr) {
		t.Errorf("head state query should have failed, got %v", err)
	}
}

func TestDatabase_GetBlockHeight_UnderlyingDB_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	stateDB.EXPECT().GetArchiveBlockHeight().Return(uint64(0), true, injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if _, err := db.GetArchiveBlockHeight(); !errors.Is(err, injectedErr) {
		t.Errorf("archive query should fail")
	}
}

func TestDatabase_GetBlockHeight_EmptyArchive(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	stateDB.EXPECT().GetArchiveBlockHeight().Return(uint64(0), true, nil)

	db := &database{
		db:    state,
		state: stateDB,
	}

	block, err := db.GetArchiveBlockHeight()
	if err != nil {
		t.Errorf("cannot get block height: %v", err)
	}

	if block >= 0 {
		t.Errorf("non archive database should return negative block number, was: %d", block)
	}
}

func TestDatabase_GetHistoricStateHash_UnderlyingDB_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	state.EXPECT().GetArchiveState(gomock.Any()).Return(nil, injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if _, err := db.GetHistoricStateHash(0); !errors.Is(err, injectedErr) {
		t.Errorf("archive query should fail")
	}
}

func TestDatabase_GetHistoricStateHash_UnderlyingDB_FailsGettingHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	st := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	subSt := state.NewMockState(ctrl)
	subSt.EXPECT().GetHash().Return(common.Hash{}, injectedErr)
	subSt.EXPECT().Check().Times(2).Return(nil)

	st.EXPECT().GetArchiveState(gomock.Any()).Return(subSt, nil)

	db := &database{
		db:    st,
		state: stateDB,
	}

	if _, err := db.GetHistoricStateHash(0); !errors.Is(err, injectedErr) {
		t.Errorf("archive query should fail")
	}
}

func TestDatabase_GetHistoricContext_UnderlyingDB_Fails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)

	injectedErr := fmt.Errorf("injectedErr")
	state.EXPECT().GetArchiveState(gomock.Any()).Return(nil, injectedErr)

	db := &database{
		db:    state,
		state: stateDB,
	}

	if _, err := db.GetHistoricContext(0); !errors.Is(err, injectedErr) {
		t.Errorf("archive query should fail")
	}
}

func TestDatabase_OpeningArchiveFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockStateDB(ctrl)
	state := state.NewMockState(ctrl)
	state.EXPECT().Close()

	injectedErr := fmt.Errorf("injectedErr")
	stateDB.EXPECT().GetArchiveBlockHeight().Return(uint64(0), false, injectedErr)
	stateDB.EXPECT().Close()

	if _, err := openStateDb(state, stateDB); !errors.Is(err, injectedErr) {
		t.Errorf("opening archive should fail")
	}
}

func TestDatabase_OpenFailsForInvalidProperty(t *testing.T) {
	tests := map[string]struct {
		property Property
		value    string
	}{
		"liveCache-not-an-int": {
			property: LiveDBCache,
			value:    "hello",
		},
		"archiveCache-not-an-int": {
			property: ArchiveCache,
			value:    "hello",
		},
		"StorageCache-not-an-int": {
			property: StorageCache,
			value:    "hello",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			properties := Properties{}
			properties[test.property] = test.value
			_, err := OpenDatabase(t.TempDir(), testConfig, properties)
			if err == nil {
				t.Errorf("expected an error, got nothing")
			}
		})
	}
}

func TestHeadBlockContext_CanCreateSequenceOfBlocks(t *testing.T) {
	for _, config := range []Configuration{testConfig, testNonArchiveConfig} {
		t.Run(fmt.Sprintf("%v", config), func(t *testing.T) {
			db, err := OpenDatabase(t.TempDir(), config, testProperties)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}

			for i := 0; i < 10; i++ {
				block, err := db.BeginBlock(uint64(i))
				if err != nil {
					t.Fatalf("failed to create block %d: %v", i, err)
				}
				if err := block.Abort(); err != nil {
					t.Fatalf("failed to abort block %d: %v", i, err)
				}
			}

			if err := db.Close(); err != nil {
				t.Fatalf("failed to close database: %v", err)
			}
		})
	}
}

func TestDatabase_CannotStartMultipleBlocksAtOnce(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	block, err := db.BeginBlock(12)
	if err != nil {
		t.Fatalf("failed to start block: %v", err)
	}

	_, err = db.BeginBlock(14)
	if err == nil {
		t.Fatalf("opening two head blocks at the same time should fail")
	}

	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort head block: %v", err)
	}

	block, err = db.BeginBlock(12)
	if err != nil {
		t.Fatalf("failed to start block: %v", err)
	}

	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort head block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BulkLoadProducesBlocks(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	load, err := db.StartBulkLoad(12)
	if err != nil {
		t.Fatalf("failed to start bulk-load: %v", err)
	}

	load.CreateAccount(Address{1})
	load.SetNonce(Address{1}, 12)
	load.CreateAccount(Address{2})
	load.SetNonce(Address{2}, 14)

	if err := load.Finalize(); err != nil {
		t.Fatalf("failed to finalize bulk load: %v", err)
	}

	err = errors.Join(
		db.QueryBlock(11, func(bc HistoricBlockContext) error {
			return errors.Join(
				bc.RunTransaction(func(tc TransactionContext) error {
					if tc.Exist(Address{1}) {
						t.Errorf("account 1 should not exist")
					}
					if tc.Exist(Address{2}) {
						t.Errorf("account 2 should not exist")
					}
					return nil
				}),
			)
		}),
		db.QueryBlock(12, func(bc HistoricBlockContext) error {
			return errors.Join(
				bc.RunTransaction(func(tc TransactionContext) error {
					if !tc.Exist(Address{1}) {
						t.Errorf("account 1 should exist")
					}
					if want, got := uint64(12), tc.GetNonce(Address{1}); want != got {
						t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
					}
					if !tc.Exist(Address{2}) {
						t.Errorf("account 2 should exist")
					}
					if want, got := uint64(14), tc.GetNonce(Address{2}); want != got {
						t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
					}
					return nil
				}),
			)
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error during query evaluation: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BeginBlock_InvalidBlock(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	bctx, err := db.BeginBlock(5)
	if err != nil {
		t.Fatalf("cannot begin block: %v", err)
	}
	if err := bctx.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	// cannot start the same block
	_, err = db.BeginBlock(5)
	if err == nil {
		t.Errorf("beginning duplicated block should fail")
	}

	// cannot start older block
	_, err = db.BeginBlock(3)
	if err == nil {
		t.Errorf("beginning older block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BeginBlock_InvalidBlock_ReopenDB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	bctx, err := db.BeginBlock(5)
	if err != nil {
		t.Fatalf("cannot begin block: %v", err)
	}
	if err := bctx.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// cannot start the same block
	_, err = db.BeginBlock(5)
	if err == nil {
		t.Errorf("beginning duplicated block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// cannot start older block
	_, err = db.BeginBlock(3)
	if err == nil {
		t.Errorf("beginning older block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BeginBlock_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// cannot start the block
	_, err = db.BeginBlock(5)
	if err == nil {
		t.Errorf("beginning block should fail")
	}
}

func TestDatabase_BeginBlock_CanStartAbortedBlock(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	bctx, err := db.BeginBlock(5)
	if err != nil {
		t.Fatalf("cannot begin block: %v", err)
	}
	if err := bctx.Abort(); err != nil {
		t.Fatalf("cannot abort block: %v", err)
	}

	// can start the same block
	bctx, err = db.BeginBlock(5)
	if err != nil {
		t.Errorf("cannot begin block: %v", err)
	}
	if err := bctx.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BeginBlock_CanStartAbortedBlock_ReopenDB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	bctx, err := db.BeginBlock(5)
	if err != nil {
		t.Fatalf("cannot begin block: %v", err)
	}
	if err := bctx.Abort(); err != nil {
		t.Fatalf("cannot abort block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// can start the same block
	bctx, err = db.BeginBlock(5)
	if err != nil {
		t.Errorf("cannot begin block: %v", err)
	}
	if err := bctx.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// can start the next block
	bctx, err = db.BeginBlock(6)
	if err != nil {
		t.Errorf("cannot begin block: %v", err)
	}
	if err := bctx.Commit(); err != nil {
		t.Fatalf("cannot commit block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_InvalidBlock(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err != nil {
		t.Fatalf("cannot add block: %v", err)
	}

	// cannot start the same block
	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err == nil {
		t.Errorf("adding duplicated block should fail")
	}

	// cannot start older block
	if err := db.AddBlock(3, func(context HeadBlockContext) error {
		return nil
	}); err == nil {
		t.Errorf("adding older block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_ReopenDB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err != nil {
		t.Fatalf("cannot add block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// cannot start the same block
	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err == nil {
		t.Errorf("adding duplicated block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// cannot start older block
	if err := db.AddBlock(3, func(context HeadBlockContext) error {
		return nil
	}); err == nil {
		t.Errorf("adding older block should fail")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_CanStartAbortedBlock(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return fmt.Errorf("injectedError")
	}); err == nil {
		t.Fatalf("block should be aborted")
	}

	// can start the same block
	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err != nil {
		t.Errorf("cannot add block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_CanStartAbortedBlock_ReopenDB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return fmt.Errorf("injectedError")
	}); err == nil {
		t.Fatalf("block should be aborted")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = OpenDatabase(dir, testConfig, testProperties)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// can start the same block
	if err := db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	}); err != nil {
		t.Errorf("cannot add block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// cannot start the block
	err = db.AddBlock(5, func(context HeadBlockContext) error {
		return nil
	})
	if err == nil {
		t.Errorf("adding block should fail")
	}
}

func TestDatabase_CloseDB_Uncommitted_Block(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot abort ctx: %v", err)
		}
	}()

	ctx, err := db.BeginBlock(5)
	if err != nil {
		t.Errorf("cannot begin block: %v", err)
	}

	defer func() {
		if err := ctx.Abort(); err != nil {
			t.Fatalf("cannot abort ctx: %v", err)
		}
	}()

	if err := db.Close(); !errors.Is(err, errBlockContextRunning) {
		t.Fatalf("closing database should fail while block is not committed")
	}

}

func TestDatabase_CloseDB_Unfinished_Queries(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.AddBlock(0, func(context HeadBlockContext) error {
		return nil
	}); err != nil {
		t.Fatalf("cannot add block: %v", err)
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush db: %v", err)
	}

	const loops = 10
	ctxs := make([]HistoricBlockContext, 0, loops)
	for i := 0; i < loops; i++ {
		ctx, err := db.GetHistoricContext(0)
		if err != nil {
			t.Errorf("cannot get history: %v", err)
		}
		ctxs = append(ctxs, ctx)
	}

	// each close should fail as there are running queries
	for i := 0; i < loops; i++ {
		if err := db.Close(); !errors.Is(err, errBlockContextRunning) {
			t.Fatalf("closing database should fail while block is not committed")
		}
		if err := ctxs[i].Close(); err != nil {
			t.Fatalf("cannot close query: %v", err)
		}
	}

	// all history queries closed, db can be closed
	if err := db.Close(); err != nil {
		t.Errorf("db cannot be closed: %v", err)
	}
}

func TestDatabase_BeginBlock_Parallel(t *testing.T) {
	const loops = 100

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	success := &atomic.Int32{}
	wg := &sync.WaitGroup{}
	wg.Add(loops)
	for i := 0; i < loops; i++ {
		go func(i int) {
			defer wg.Done()
			bctx, err := db.BeginBlock(uint64(i))
			// once the block could be created, it must be able to commit
			if err == nil {
				if err := bctx.Commit(); err != nil {
					t.Errorf("cannot commit block: %v", err)
				} else {
					success.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	if success.Load() == 0 {
		t.Errorf("no block was added")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_Parallel(t *testing.T) {
	const loops = 100

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	success := &atomic.Int32{}
	wg := &sync.WaitGroup{}
	wg.Add(loops)
	for i := 0; i < loops; i++ {
		go func(i int) {
			defer wg.Done()
			err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
				return nil
			})
			if err == nil {
				success.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if success.Load() == 0 {
		t.Errorf("no block was added")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_GetHistoricBlock_Parallel(t *testing.T) {
	const loops = 100

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// init a few blocks
	for i := 0; i < loops; i++ {
		if err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush db: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(loops)
	for i := 0; i < loops; i++ {
		go func(i int) {
			defer wg.Done()
			bctx, err := db.GetHistoricContext(uint64(i))
			if err != nil {
				t.Errorf("cannot get historic block: %v", err)
			}
			if err := bctx.Close(); err != nil {
				t.Errorf("cannot commit block: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_QueryBlock_Parallel(t *testing.T) {
	const loops = 100

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// init a few blocks
	for i := 0; i < loops; i++ {
		if err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush db: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(loops)
	for i := 0; i < loops; i++ {
		go func(i int) {
			defer wg.Done()
			if err := db.QueryBlock(uint64(i), func(context HistoricBlockContext) error {
				return nil
			}); err != nil {
				t.Errorf("cannot query block: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_AddBlock_QueryBlock_Parallel(t *testing.T) {
	const loops = 100

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// init a few blocks
	for i := 0; i < loops; i++ {
		if err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush db: %v", err)
	}

	// keep adding more blocks while querying already created once
	wg := &sync.WaitGroup{}
	wg.Add(loops)
	for i := 0; i < loops; i++ {
		go func(i int) {
			defer wg.Done()
			if err := db.QueryBlock(uint64(i), func(context HistoricBlockContext) error {
				return nil
			}); err != nil {
				t.Errorf("cannot query block: %v", err)
			}
		}(i)
	}

	wg.Add(loops)
	added := &atomic.Int32{}
	for i := 0; i < loops; i++ {
		go func(block int) {
			defer wg.Done()
			if err := db.AddBlock(uint64(block), func(context HeadBlockContext) error {
				return nil
			}); err == nil {
				added.Add(1)
			}
		}(i + loops)
	}

	wg.Wait()

	if added.Load() == 0 {
		t.Errorf("no block was added")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_QueryBlock_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if err := db.QueryBlock(0, func(context HistoricBlockContext) error {
		return nil
	}); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_QueryHeadState_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if err := db.QueryHeadState(nil); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_GetBlockHeight_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if _, err := db.GetArchiveBlockHeight(); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_GetHistoricStateHash_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if _, err := db.GetHistoricStateHash(0); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_GetHistoricContext_NonExistingBlock(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close db: %v", err)
		}
	}()

	if _, err := db.GetHistoricContext(100); err == nil {
		t.Errorf("should not be able to query non-existing block")
	}
}

func TestDatabase_GetHistoricContext_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if _, err := db.GetHistoricContext(0); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_Historic_Block_Available(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	addr := Address{1}
	if err := db.AddBlock(0, func(context HeadBlockContext) error {
		if err := context.RunTransaction(func(context TransactionContext) error {
			context.CreateAccount(addr)
			context.AddBalance(addr, NewAmount(1000))
			return nil
		}); err != nil {
			t.Fatalf("cannot commit transaction: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("cannot add block: %v", err)
	}

	const loops = 10
	for i := 1; i < loops; i++ {
		// cannot start the same block
		if err := db.AddBlock(uint64(i), func(context HeadBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				context.AddBalance(addr, NewAmount(100))
				return nil
			}); err != nil {
				t.Fatalf("cannot commit transaction: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush db: %v", err)
	}

	var transactions int
	// query historic blocks
	for i := 0; i < loops; i++ {
		err := db.QueryBlock(uint64(i), func(context HistoricBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				if got, want := context.GetBalance(addr), NewAmount(uint64(i*100)+1000); got != want {
					t.Errorf("balance does not match for block: %d, got: %d != wanted: %d", i, got, want)
				}
				transactions++
				return nil
			}); err != nil {
				t.Errorf("cannot run transaction: %v", err)
			}
			return nil
		})
		if err != nil {
			t.Errorf("failed to query block %d: %v", i, err)
		}
	}

	if transactions != loops {
		t.Errorf("not all historic blocks were visited: %d", transactions)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_StartBulkLoad_Can_Run_Consecutive(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	for i := 0; i < 20; i++ {
		ctx, err := db.StartBulkLoad(uint64(i))
		if err != nil {
			t.Errorf("cannot start bulk load: %v", err)
		}
		if err := ctx.Finalize(); err != nil {
			t.Errorf("cannot finish bulk load: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_StartBulkLoad_ClosedDB(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if _, err := db.StartBulkLoad(0); !errors.Is(err, errDbClosed) {
		t.Errorf("should not be able to query closed database")
	}
}

func TestDatabase_StartBulkLoad_Cannot_Start_Twice(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	ctx, err := db.StartBulkLoad(0)
	if err != nil {
		t.Fatalf("cannot start bulk load: %v", err)
	}

	if _, err := db.StartBulkLoad(0); !errors.Is(err, errBlockContextRunning) {
		t.Errorf("should not be able to run bulk load")
	}

	if err := ctx.Finalize(); err != nil {
		t.Fatalf("cannot finish bulk load: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_StartBulkLoad_Cannot_Start_Wrong_Block(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	ctx, err := db.StartBulkLoad(10)
	if err != nil {
		t.Errorf("cannot start bulk load: %v", err)
	}
	if err := ctx.Finalize(); err != nil {
		t.Errorf("cannot finish bulk load: %v", err)
	}

	if _, err := db.StartBulkLoad(3); err == nil {
		t.Errorf("block should be out of range")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_StartBulkLoad_Cannot_Finalize_Twice(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close db: %v", err)
		}
	}()

	ctx, err := db.StartBulkLoad(0)
	if err != nil {
		t.Fatalf("cannot start bulk load: %v", err)
	}

	if err := ctx.Finalize(); err != nil {
		t.Fatalf("cannot finish bulk load: %v", err)
	}

	if err := ctx.Finalize(); err == nil {
		t.Errorf("second call to finalize should fail")
	}
}

func TestDatabase_Async_AddBlock_QueryHistory_Close_ShouldNotThrowUnexpectedError(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	addBlock := func(block uint64) error {
		return db.AddBlock(block, func(context HeadBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				addr := Address{byte(block)}
				context.CreateAccount(addr)
				context.AddBalance(addr, NewAmount(100))
				return nil
			}); err != nil {
				t.Fatalf("cannot commit transaction: %v", err)
			}
			return nil
		})
	}

	// init a few blocks
	const loops = 10
	for i := 0; i < loops; i++ {
		if err := addBlock(uint64(i)); err != nil {
			t.Fatalf("cannot add block: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("cannot flush: %v", err)
	}

	minHeadUpdates := &atomic.Int32{}
	minHeadUpdates.Add(100)

	minHistoricQueries := &atomic.Int32{}
	minHistoricQueries.Add(100)

	// run parallel update to the head state
	go func() {
		block := loops
		for {
			if err := addBlock(uint64(block)); err != nil {
				if errors.Is(err, errDbClosed) {
					break // this is ok, db was closed in parallel
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
			block++
			minHeadUpdates.Add(-1)
		}
	}()

	// parallel queries to existing blocks
	go func() {
		for {
			if _, err := db.GetArchiveBlockHeight(); err != nil {
				if errors.Is(err, errDbClosed) {
					break // this is ok, db was closed in parallel
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
			minHistoricQueries.Add(-1)
		}
	}()
	go func() {
		var block uint64
		for {
			ctx, err := db.GetHistoricContext(block % loops)
			if err != nil {
				if errors.Is(err, errDbClosed) {
					break // this is ok, db was closed in parallel
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
			if err := ctx.Close(); err != nil {
				t.Errorf("cannot close context: %v", err)
			}
			block++
			minHistoricQueries.Add(-1)
		}
	}()
	go func() {
		var block uint64
		for {
			if _, err := db.GetHistoricStateHash(block % loops); err != nil {
				if errors.Is(err, errDbClosed) {
					break // this is ok, db was closed in parallel
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
			block++
			minHistoricQueries.Add(-1)
		}
	}()
	go func() {
		var block uint64
		for {
			if err := db.QueryBlock(block%loops, func(context HistoricBlockContext) error {
				return nil
			}); err != nil {
				if errors.Is(err, errDbClosed) {
					break // this is ok, db was closed in parallel
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
			block++
			minHistoricQueries.Add(-1)
		}
	}()

	// make sure some queries happen before an attempt to close
	for minHeadUpdates.Load() > 0 || minHistoricQueries.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// parallel close of the database
	// should not cause other than expected error
	for {
		if err := db.Close(); err == nil {
			break // db was closed, we are done
		} else {
			// concurrent access is ok, just repeat attempt to close again
			if !errors.Is(err, errBlockContextRunning) {
				t.Errorf("failed to close database: %v", err)
			}
		}
	}
}

func TestDatabase_Async_QueryHead_Accesses_ConsistentState(t *testing.T) {
	// This test case checks that query operations see a consistent state
	// when running concurrent updates.
	const (
		numReaders = 10
		numBlocks  = 100
	)
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close db: %v", err)
		}
	}()

	addr1 := Address{1}
	addr2 := Address{2}

	// Have a few goroutines testing that nonces are in sync.
	var group sync.WaitGroup
	group.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer group.Done()
			nonce := uint64(0)
			for nonce < numBlocks {
				err := db.QueryHeadState(func(ctxt QueryContext) {
					// Readers should always see the same nonces.
					n1 := ctxt.GetNonce(addr1)
					n2 := ctxt.GetNonce(addr2)
					if n1 != n2 {
						t.Errorf("nonces out of sync: %d vs %d", n1, n2)
					}
					nonce = n1
				})
				if err != nil {
					t.Errorf("failed to query head: %v", err)
				}
			}
		}()
	}

	// Add blocks updating nonces in sync.
	for i := 1; i <= numBlocks; i++ {
		block := uint64(i)
		err := db.AddBlock(block, func(context HeadBlockContext) error {
			if err := context.RunTransaction(func(context TransactionContext) error {
				// In all blocks the nonces of both accounts are identical.
				context.SetNonce(addr1, block)
				context.SetNonce(addr2, block)
				return nil
			}); err != nil {
				t.Fatalf("cannot commit transaction: %v", err)
			}
			return nil
		})
		if err != nil {
			t.Errorf("failed to add block %d: %v", i, err)
		}
	}

	group.Wait()
}

func TestDatabase_ActiveHeadQueryBlockDataBaseClose(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	queryStarted := make(chan bool)
	done := &atomic.Bool{}
	go db.QueryHeadState(func(QueryContext) {
		defer wg.Done()
		queryStarted <- true
		// keep this alive to block the closing of the database
		time.Sleep(time.Second)
		done.Store(true)
	})

	go func() {
		defer wg.Done()
		<-queryStarted
		// This should block until all queries are done
		if err := db.Close(); err != nil {
			t.Errorf("cannot close db: %v", err)
		}
		if !done.Load() {
			t.Errorf("finished closing before queries are complete")
		}
	}()

	wg.Wait()
}

func TestDatabase_QueryCannotBeStartedOnClosedDatabase(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	err = db.QueryHeadState(func(QueryContext) {})
	if !errors.Is(err, errDbClosed) {
		t.Errorf("Starting a query on a closed database should have failed, got %v", err)
	}
}

func TestDatabase_ArchiveCanBeAccessedAsync(t *testing.T) {
	const numBlocks = 1000
	addr1 := Address{1}

	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("cannot open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close db: %v", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// query archive
	go func() {
		defer wg.Done()
		for {
			height, err := db.GetArchiveBlockHeight()
			if err != nil {
				t.Errorf("cannot get archive height: %v", err)
				return
			}
			if height >= 0 {
				ctx, err := db.GetHistoricContext(uint64(height))
				if err != nil {
					t.Errorf("cannot get historic context: height: %d,  %v", height, err)
					return
				}
				if err := ctx.Close(); err != nil {
					t.Errorf("cannot close ctx: %v", err)
				}
			}
			if height >= numBlocks {
				return // we are done, all blocks ready in the archive -> no error so far
			}
		}
	}()

	// add blocks
	go func() {
		defer wg.Done()
		for i := 1; i <= numBlocks; i++ {
			block := uint64(i)
			err := db.AddBlock(block, func(context HeadBlockContext) error {
				if err := context.RunTransaction(func(context TransactionContext) error {
					// In all blocks, the nonces of both accounts are identical.
					context.SetNonce(addr1, block)
					return nil
				}); err != nil {
					t.Errorf("cannot commit transaction: %v", err)
				}
				return nil
			})
			if err != nil {
				t.Errorf("failed to add block %d: %v", i, err)
			}
		}
	}()

	wg.Wait()
}

func TestDatabase_GetMemoryFootprint(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := state.NewMockState(ctrl)

	state.EXPECT().GetMemoryFootprint()

	db := &database{
		db: state,
	}

	db.GetMemoryFootprint()
}
