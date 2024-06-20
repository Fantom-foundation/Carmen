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
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

func TestTransaction_Cannot_Commit_Twice(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockVmStateDB(ctrl)
	stateDB.EXPECT().EndTransaction()
	stateDB.EXPECT().Check()

	tx := &transactionContext{
		blockContext: &commonContext{
			transactionActive: true,
		},
		state: stateDB,
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("cannot commit transaction: %v", err)
	}

	if err := tx.Commit(); err == nil {
		t.Errorf("commit should fail")
	}
}

func TestTransaction_Cannot_Commit_After_Abort(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockVmStateDB(ctrl)
	stateDB.EXPECT().AbortTransaction().AnyTimes()
	stateDB.EXPECT().Check()

	tx := &transactionContext{
		blockContext: &commonContext{
			transactionActive: true,
		},
		state: stateDB,
	}

	if err := tx.Abort(); err != nil {
		t.Errorf("cannot abort transaction: %v", err)
	}

	if err := tx.Commit(); err == nil {
		t.Errorf("commit should fail")
	}
}

func TestTransaction_Second_Abort_Noop(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockVmStateDB(ctrl)
	stateDB.EXPECT().AbortTransaction().AnyTimes()
	stateDB.EXPECT().Check()

	tx := &transactionContext{
		blockContext: &commonContext{
			transactionActive: true,
		},
		state: stateDB,
	}

	if err := tx.Abort(); err != nil {
		t.Errorf("unexpected error during abort: %v", err)
	}

	if err := tx.Abort(); err != nil {
		t.Errorf("cannot abort block: %v", err)
	}

}

func TestDatabase_AbortedTransactionsHaveNoEffect(t *testing.T) {
	db, err := openTestDatabase(t)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("cannot close db: %v", err)
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
	if err := tx.Abort(); err != nil {
		t.Fatalf("unexpected error during abort: %v", err)
	}

	tx, err = block.BeginTransaction()
	if err != nil {
		t.Fatalf("failed to start second transaction: %v", err)
	}
	if want, got := uint64(0), tx.GetNonce(Address{}); want != got {
		t.Errorf("unexpected result after abort, want %d, got %d", want, got)
	}

	if err := tx.Abort(); err != nil {
		t.Fatalf("unexpected error during abort: %v", err)
	}
	if err := block.Abort(); err != nil {
		t.Fatalf("cannot abort block: %v", err)
	}
}

func TestTransaction_Operations_Passthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockVmStateDB(ctrl)
	stateDB.EXPECT().CreateAccount(gomock.Any())
	stateDB.EXPECT().Exist(gomock.Any())
	stateDB.EXPECT().Empty(gomock.Any())
	stateDB.EXPECT().Suicide(gomock.Any())
	stateDB.EXPECT().HasSuicided(gomock.Any())
	stateDB.EXPECT().GetBalance(gomock.Any())
	stateDB.EXPECT().AddBalance(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SubBalance(gomock.Any(), gomock.Any())
	stateDB.EXPECT().GetNonce(gomock.Any())
	stateDB.EXPECT().SetNonce(gomock.Any(), gomock.Any())
	stateDB.EXPECT().GetCommittedState(gomock.Any(), gomock.Any())
	stateDB.EXPECT().GetState(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SetState(gomock.Any(), gomock.Any(), gomock.Any())
	stateDB.EXPECT().GetTransientState(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SetTransientState(gomock.Any(), gomock.Any(), gomock.Any())
	stateDB.EXPECT().HasEmptyStorage(gomock.Any())
	stateDB.EXPECT().GetCode(gomock.Any())
	stateDB.EXPECT().SetCode(gomock.Any(), gomock.Any())
	stateDB.EXPECT().GetCodeHash(gomock.Any())
	stateDB.EXPECT().GetCodeSize(gomock.Any())
	stateDB.EXPECT().AddRefund(gomock.Any())
	stateDB.EXPECT().SubRefund(gomock.Any())
	stateDB.EXPECT().GetRefund()
	stateDB.EXPECT().AddLog(gomock.Any())
	stateDB.EXPECT().GetLogs().Return([]*common.Log{{}})
	stateDB.EXPECT().ClearAccessList()
	stateDB.EXPECT().AddAddressToAccessList(gomock.Any())
	stateDB.EXPECT().AddSlotToAccessList(gomock.Any(), gomock.Any())
	stateDB.EXPECT().IsAddressInAccessList(gomock.Any())
	stateDB.EXPECT().IsSlotInAccessList(gomock.Any(), gomock.Any())
	stateDB.EXPECT().Snapshot()
	stateDB.EXPECT().RevertToSnapshot(10)

	stateDB.EXPECT().EndTransaction()
	stateDB.EXPECT().Check()

	tx := &transactionContext{
		blockContext: &commonContext{
			transactionActive: true,
		},
		state: stateDB,
	}

	var address Address
	var key Key
	var value Value
	tx.CreateAccount(address)
	tx.Exist(address)
	tx.Empty(address)
	tx.SelfDestruct(address)
	tx.HasSelfDestructed(address)
	tx.GetBalance(address)
	tx.AddBalance(address, NewAmount(100))
	tx.SubBalance(address, NewAmount(100))
	tx.GetNonce(address)
	tx.SetNonce(address, 100)
	tx.GetCommittedState(address, key)
	tx.GetState(address, key)
	tx.SetState(address, key, value)
	tx.GetTransientState(address, key)
	tx.SetTransientState(address, key, value)
	tx.HasEmptyState(address)
	tx.GetCode(address)
	tx.SetCode(address, []byte{})
	tx.GetCodeHash(address)
	tx.GetCodeSize(address)
	tx.AddRefund(100)
	tx.SubRefund(100)
	tx.GetRefund()
	tx.AddLog(nil)
	tx.AddLog(&Log{})
	tx.GetLogs()
	tx.ClearAccessList()
	tx.AddAddressToAccessList(address)
	tx.AddSlotToAccessList(address, key)
	tx.IsAddressInAccessList(address)
	tx.IsSlotInAccessList(address, key)
	tx.Snapshot()
	tx.RevertToSnapshot(10)

	if err := tx.Commit(); err != nil {
		t.Errorf("cannot commit transaction: %v", err)
	}

}

func TestTransaction_AfterCommitAllOperationsAreNoops(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockVmStateDB(ctrl)
	stateDB.EXPECT().EndTransaction()
	stateDB.EXPECT().Check()

	tx := &transactionContext{
		blockContext: &commonContext{
			transactionActive: true,
		},
		state: stateDB,
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("cannot commit transaction: %v", err)
	}

	var address Address
	var key Key
	var value Value
	tx.CreateAccount(address)
	tx.Exist(address)
	tx.Empty(address)
	tx.SelfDestruct(address)
	tx.HasSelfDestructed(address)
	tx.GetBalance(address)
	tx.AddBalance(address, NewAmount(100))
	tx.SubBalance(address, NewAmount(100))
	tx.GetNonce(address)
	tx.SetNonce(address, 100)
	tx.GetCommittedState(address, key)
	tx.GetState(address, key)
	tx.SetState(address, key, value)
	tx.GetTransientState(address, key)
	tx.SetTransientState(address, key, value)
	tx.HasEmptyState(address)
	tx.GetCode(address)
	tx.SetCode(address, []byte{})
	tx.GetCodeHash(address)
	tx.GetCodeSize(address)
	tx.AddRefund(100)
	tx.SubRefund(100)
	tx.GetRefund()
	tx.AddLog(nil)
	tx.GetLogs()
	tx.ClearAccessList()
	tx.AddAddressToAccessList(address)
	tx.AddSlotToAccessList(address, key)
	tx.IsAddressInAccessList(address)
	tx.IsSlotInAccessList(address, key)
	tx.Snapshot()
	tx.RevertToSnapshot(10)
}
