package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/golang/mock/gomock"
)

func TestCaremenStateImplementsStateDbInterface(t *testing.T) {
	var db stateDB
	var _ StateDB = &db
}

func TestCarmenStateAccountsCanBeCreatedAndDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the account creation needs to check whether the account exists
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.CreateAccount(address1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after it was created")
	}
	if db.HasSuicided(address1) {
		t.Errorf("New account is considered deleted")
	}
	db.Suicide(address1)
	if !db.HasSuicided(address1) {
		t.Errorf("Destroyed account is still considered alive")
	}
	// The account should stop existing at the end of the transaction
	db.EndTransaction()
	db.BeginTransaction()
	if db.Exist(address1) {
		t.Errorf("Account still exists after suicide")
	}
}

func TestCarmenStateCreateAccountSetsNonceCodeAndBalanceToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.CreateAccount(address1)

	if got := db.GetNonce(address1); got != 0 {
		t.Errorf("nonce not initialized with zero")
	}

	if got := db.GetBalance(address1); got.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("balance not initialized with zero")
	}

	if got := db.GetCode(address1); len(got) != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}
}

func TestCarmenStateRecreatingAccountSetsNonceCodeAndBalanceToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a previously deleted account.
	mock.EXPECT().GetAccountState(address1).Return(common.Deleted, nil)

	db.CreateAccount(address1)

	if got := db.GetNonce(address1); got != 0 {
		t.Errorf("nonce not initialized with zero")
	}

	if got := db.GetBalance(address1); got.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("balance not initialized with zero")
	}

	if got := db.GetCode(address1); len(got) != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}
}

func TestCarmenStateRecreatingAccountResetsStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account is non-exisiting, it gets destroyed, and recreated.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)

	// First transaction creates an account and sets some storage values.
	db.BeginTransaction()
	db.CreateAccount(address1)
	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.EndTransaction()

	// In the second transaction we delete and restore the account.
	db.BeginTransaction()

	// Initially the old values should be present.
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("Wrong state value, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("Wrong state value, wanted %v, got %v", val2, got)
	}

	db.Suicide(address1)
	// Note: after suicide processing of the contract ends. However, the enclosing call
	// may immediately re-create the account in the same transaction.

	db.CreateAccount(address1)

	// The values should still be gone.
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("Wrong state value, wanted %v, got %v", zero, got)
	}
	if got := db.GetState(address1, key2); got != zero {
		t.Errorf("Wrong state value, wanted %v, got %v", zero, got)
	}
	if got := db.GetState(address1, key3); got != zero {
		t.Errorf("Wrong state value, wanted %v, got %v", zero, got)
	}

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateRecreatingAccountResetsStorageButRetainsNewState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists with some state.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	// At the end the account is deleted and recreated with the new state.
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetStorage(address1, key1, val2).Return(nil)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("Wrong initial state, wanted %v, got %v", val1, got)
	}

	// When deleting the account, the state is still present since the suicide only becomes
	// effective at the end of the transaction.
	db.Suicide(address1)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("Wrong post-suicide state, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("Wrong post-suicide state, wanted %v, got %v", val2, got)
	}

	// However, if the account is re-created in the same transaction, the storage is deleted.
	db.CreateAccount(address1)
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("Wrong post-recreate state, wanted %v, got %v", zero, got)
	}
	if got := db.GetState(address1, key2); got != zero {
		t.Errorf("Wrong post-recreate state, wanted %v, got %v", zero, got)
	}

	// Values written now are preserved.
	db.SetState(address1, key1, val2)
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("Wrong state, wanted %v, got %v", val2, got)
	}
	db.EndTransaction()

	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("Wrong post-end-of-transaction state, wanted %v, got %v", val2, got)
	}

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateStorageOfDestroyedAccountIsStillAccessibleTillEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account existis with some values inside.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	db.BeginTransaction()

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("initial value not present, wanted %v, got %v", val1, got)
	}

	db.Suicide(address1)

	// This one is read from the intra-transaction cache in the StateDB (tests that the cache is not touched)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("expected no change of value due to suicide till end of transaction, wanted %v, got %v", val1, got)
	}

	// This one needs to be fetched from the underlying DB
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("expected no change of value due to suicide till end of transaction, wanted %v, got %v", val2, got)
	}

	db.EndTransaction()

	db.BeginTransaction()

	// This one is read from the intra-transaction cache in the StateDB (tests that the cache is reset)
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("expected storage to be reset at end of transaction; wanted %v, got %v", zero, got)
	}

	// This one is read from the intra-transaction cache in the StateDB
	if got := db.GetState(address1, key2); got != zero {
		t.Errorf("expected storage to be reset at end of transaction; wanted %v, got %v", zero, got)
	}

	// This one would have to be fetched from the underlying DB, but since the deletion of the account
	// is now committed, this should not perform an actual fetch.
	if got := db.GetState(address1, key3); got != zero {
		t.Errorf("expected storage to be reset at end of transaction; wanted %v, got %v", zero, got)
	}

	db.EndTransaction()
}

func TestCarmenStateStoreDataCacheIsResetAfterSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists and has a slot value set.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	// During the processing the account is deleted.
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetStorage(address1, key2, val2).Return(nil)

	// In the first transaction key1 is fetched, ending up in the store data cache.
	db.BeginTransaction()
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("unexpected value, wanted %v, got %v", val1, got)
	}
	db.EndTransaction()
	db.EndBlock() // < stored value remains in store data cache

	// In the next block the account is destroyed
	db.BeginTransaction()
	db.Suicide(address1)
	db.EndTransaction()
	db.BeginTransaction()
	// This value is zero because within this block the address1 was cleared.
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("unexpected value, wanted %v, got %v", zero, got)
	}
	db.SetState(address1, key2, val2)
	db.EndTransaction()
	db.EndBlock() // < here the stored data cache is reset to forget the old state; also, key2/val2 is stored in DB

	// In this block we try to read the value again. This time it is not cached
	// in the snapshot state nor is the account marked as being cleared. The value
	// is retrieved from the store data cache.
	db.BeginTransaction()
	// This value is now fetched from the value store, which is supposed to be cleared
	// at the end of the block deleting the account.
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("unexpected value, wanted %v, got %v", zero, got)
	}
	// The value for key2 is also retrieved from the value store (no expected GetStorage call for this).
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("unexpected value, wanted %v, got %v", val2, got)
	}
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateRollingBackSuicideRestoresValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	// In the transaction we delete and restore the account.
	db.BeginTransaction()

	// Initially the old values should be present.
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("Wrong state value, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("Wrong state value, wanted %v, got %v", val2, got)
	}

	snapshot := db.Snapshot()
	db.Suicide(address1)
	// Note: after a suicide of a contract execution ends but enclosing call may continue and roll back.

	db.RevertToSnapshot(snapshot)

	// The values should still be restored.
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("Wrong state value, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("Wrong state value, wanted %v, got %v", val2, got)
	}

	db.EndTransaction()
	db.EndBlock() // < no change is send to the DB
}

func TestCarmenStateDestroyingAndRecreatingAnAccountInTheSameTransactionCallsDeleteAndCreateAccountOnStateDb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	// The account is to be deleted and re-created.
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)

	// In a transaction we destroy the account and recreate it. This should cause
	// the account to be deleted and re-initialized in the StateDB at the end of
	// the block.
	db.BeginTransaction()

	if !db.Exist(address1) {
		t.Errorf("Account does not initially exist!")
	}

	db.Suicide(address1)
	db.CreateAccount(address1)

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateDoubleDestroyedAccountThatIsOnceRolledBackIsStillCleared(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	// The account is to be deleted and re-created.
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)

	// In a transaction we destroy the account, re-create it, destroy it and roll back
	// the second destroy; After this, the account still needs to be cleared at the
	// end of the block since the first destroy is to be committed.
	db.BeginTransaction()

	if !db.Exist(address1) {
		t.Errorf("Account does not initially exist!")
	}

	db.Suicide(address1)
	db.CreateAccount(address1)

	snapshot := db.Snapshot()
	db.Suicide(address1)
	db.RevertToSnapshot(snapshot)

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateRecreatingExistingAccountSetsNonceAndCodeToZeroAndPreservesBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a previously deleted account.
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("failed to set up test case: %v", err)
	}
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetBalance(address1).Return(b12, nil)
	db.SetNonce(address1, 14)
	db.SetCode(address1, []byte{1, 2, 3})

	db.CreateAccount(address1)

	if got := db.GetNonce(address1); got != 0 {
		t.Errorf("nonce not initialized with zero")
	}

	if got := db.GetBalance(address1); got.Cmp(big.NewInt(12)) != 0 {
		t.Errorf("balance not preserved")
	}

	if got := db.GetCode(address1); len(got) != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-lenght code")
	}
}

func TestCarmenStateCreateAccountCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	snapshot := db.Snapshot()

	db.CreateAccount(address1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after it was created")
	}

	db.RevertToSnapshot(snapshot)
	if db.Exist(address1) {
		t.Errorf("Account still exists after rollback")
	}
}

func TestCarmenStateSuicideIndicatesExistingAccountAsBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	// An existing account is indicated as being deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that existing account did not exist before delete")
	}

	// The account should stop existing at the end of the transaction
	db.EndTransaction()
	db.BeginTransaction()

	// Deleting it a second time indicates the account as already deleted.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates deleted account still existed")
	}
}

func TestCarmenStateSuicideIndicatesUnknownAccountAsNotBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an unknown account.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	// An unknown account is indicated as not being deleted.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates that unknown account existed before delete")
	}

	// Deleting it a second time does not change this.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates deleted account still existed")
	}
}

func TestCarmenStateSuicideIndicatesDeletedAccountAsNotBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a deleted account.
	mock.EXPECT().GetAccountState(address1).Return(common.Deleted, nil)

	// An already deleted account is indicated as not being deleted during the suicide.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates that deleted account existed before delete")
	}

	// Deleting it a second time does not change this.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates deleted account still existed")
	}
}

func TestCarmenStateSuicideRemovesBalanceFromAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("error preparing test: %v", err)
	}
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetBalance(address1).Return(b12, nil)

	want := big.NewInt(12)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("invalid initial balance, wanted %v, got %v", want, got)
	}

	db.Suicide(address1)

	want = big.NewInt(0)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("invalid balance after account destruction, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateSuicideCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("error preparing test: %v", err)
	}
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetBalance(address1).Return(b12, nil)

	if !db.Exist(address1) {
		t.Errorf("Account state is not loaded from underlying state")
	}

	snapshot := db.Snapshot()

	db.Suicide(address1)

	if !db.HasSuicided(address1) {
		t.Errorf("Account is not marked as suicided after suicide")
	}

	if got := db.GetBalance(address1); got.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Balance of account not cleared")
	}

	db.RevertToSnapshot(snapshot)
	if !db.Exist(address1) {
		t.Errorf("Account remains deleted after rollback")
	}
	if db.HasSuicided(address1) {
		t.Errorf("Account is still marked as suicided after rollback")
	}
	if got := db.GetBalance(address1); got.Cmp(big.NewInt(12)) != 0 {
		t.Errorf("Balance of account not restored")
	}
}

func TestCarmenStateSuicideIsExecutedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	db.Suicide(address1)

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateSuicideCanBeCanceledThroughRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(5)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{1, 2, 3}).Return(nil)

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	snapshot := db.Snapshot()
	db.Suicide(address1)
	db.RevertToSnapshot(snapshot)

	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCreatedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is created at the end of the transaction.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	db.CreateAccount(address1)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCreatedAccountsAreForgottenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The created account is only created once, and nonces and code are initialized.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	db.CreateAccount(address1)
	db.EndTransaction()
	db.EndBlock()
	db.EndBlock()
}

func TestCarmenStateCreatedAccountsAreDiscardedOnEndOfAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Needs to check whether the account already existed before the creation.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.CreateAccount(address1)
	db.AbortTransaction()
	db.EndBlock()
	db.EndBlock()
}

func TestCarmenStateDeletedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateDeletedAccountsRetainCodeUntilEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	code := []byte{1, 2, 3}
	db.CreateAccount(address1)
	db.SetCode(address1, code)
	db.EndTransaction()

	if got := db.GetCode(address1); bytes.Compare(got, code) != 0 {
		t.Errorf("retrieved wrong code, got %v, wanted %v", got, code)
	}

	db.Suicide(address1)

	// Now the code should still exist
	if got := db.GetCode(address1); bytes.Compare(got, code) != 0 {
		t.Errorf("retrieved wrong code, got %v, wanted %v", got, code)
	}

	db.EndTransaction()

	// Now the code should be gone.
	if got := db.GetCode(address1); len(got) != 0 {
		t.Errorf("retrieved wrong code, got %v, wanted empty code", got)
	}

	db.EndBlock()
}

func TestCarmenStateDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock()
}

func TestCarmenStateCreatedAndDeletedAccountsAreDeletedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().DeleteAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCreatedAndDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock()
}

func TestCarmenStateEmptyAccountsAreRecognized(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its balance and nonce set to zero.
	mock.EXPECT().GetBalance(address1).Return(common.Balance{}, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
}

func TestCarmenStateSettingTheBalanceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its balance and nonce set to zero.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetBalance(address1).Return(common.Balance{}, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.AddBalance(address1, big.NewInt(1))
	if db.Empty(address1) {
		t.Errorf("Account with balance != 0 is still considered empty")
	}
}

func TestCarmenStateSettingTheBalanceCreatesAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	addedBalance := big.NewInt(5)
	balance, _ := common.ToBalance(addedBalance)

	// The account have not existed - must be created by AddBalance call.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, balance).Return(nil)

	db.AddBalance(address1, addedBalance)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after adding balance")
	}
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateSettingTheNonceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its nonce and code set to zero.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(1)).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)

	db.CreateAccount(address1)
	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.SetNonce(address1, 1)
	if db.Empty(address1) {
		t.Errorf("Account with nonce != 0 is still considered empty")
	}
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCreatesAccountOnNonceSetting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account does not exist, is expected to be created automatically.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(1)).Return(nil)

	db.SetNonce(address1, 1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after setting the nonce")
	}
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateGetBalanceReturnsFreshCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	want := big.NewInt(12)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)

	val1 := db.GetBalance(address1)
	val2 := db.GetBalance(address1)

	if val1 == val2 {
		t.Errorf("Did not obtain fresh copy of internal big.Int value")
	}

	if val1.Cmp(val2) != 0 {
		t.Errorf("Values of multiple reads do not match: %v vs %v", val1, val2)
	}

	val1.Add(val1, big.NewInt(1))
	if val1.Cmp(val2) == 0 {
		t.Errorf("Failed to modify value 1")
	}

	val3 := db.GetBalance(address1)
	if val2.Cmp(val3) != 0 {
		t.Errorf("Modifying retrieved value changed stored value: %v vs %v", val1, val2)
	}
}

func TestCarmenStateBalancesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	want := big.NewInt(12)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)

	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateBalancesAreOnlyReadOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	want := big.NewInt(12)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)

	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}
	db.GetBalance(address1)
	db.GetBalance(address1)
}

func TestCarmenStateBalancesCanBeSnapshottedAndReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is initially 10. This should only be fetched once.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)

	snapshot0 := db.Snapshot()

	want.SetInt64(10)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}

	snapshot1 := db.Snapshot()
	diff := big.NewInt(2)
	want.Add(want, diff)
	db.AddBalance(address1, diff)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}

	snapshot2 := db.Snapshot()
	diff = big.NewInt(3)
	want.Sub(want, diff)
	db.SubBalance(address1, diff)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot2)
	want.SetInt64(12)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot1)
	want.SetInt64(10)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot0)
	if got := db.GetBalance(address1); got.Cmp(want) != 0 {
		t.Errorf("error retrieving balance, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateBalanceIsWrittenToStateIfChangedAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(12))
	mock.EXPECT().SetBalance(address1, balance).Return(nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock()

	// The second end-of-block should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateBalanceOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(14))
	mock.EXPECT().SetBalance(address1, balance).Return(nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.AddBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(3))
	db.EndTransaction()
	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateBalanceUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.SubBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(5))
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateBalanceIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.AbortTransaction()
	db.EndBlock()
}

func TestCarmenStateNoncesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	var want uint64 = 12
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(want), nil)

	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNoncesAreOnlyReadOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	var want uint64 = 12
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(want), nil)

	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
	db.GetNonce(address1)
	db.GetNonce(address1)
}

func TestCarmenStateNoncesCanBeWrittenAndReadWithoutStateAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetNonce creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	var want uint64 = 12
	db.SetNonce(address1, want)
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
	want = 14
	db.SetNonce(address1, want)
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNoncesOfANonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The nonce is fetched, and its default is zero.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(0), nil)

	var want uint64 = 0
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNonceOfADeletedAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The side-effects of the creation of the account in the first transactions are expected.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(12)).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().SetCode(address1, []byte{}).Return(nil)

	// Also the fetch of the Nonce value in the second transaction is expected.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(12), nil)

	// Create an account and set the nonce.
	db.CreateAccount(address1)
	db.SetNonce(address1, 12)
	db.EndTransaction()
	db.EndBlock()

	// Fetch the nonce in a new transaction.
	var want uint64 = 12
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.Suicide(address1)

	// The suicide is delayed until the end of the transaction.
	want = 12
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.EndTransaction()

	// The suicide was completed and the nonce is zero.
	want = 0
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNonceOfADeletedAccountGetsResetAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)

	var want uint64 = 0
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.SetNonce(address1, 12)
	want = 12
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.Suicide(address1)

	// The destruction is delayed until the end of the transaction.
	want = 12
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.EndTransaction()

	// After the end of the transaction, the nonce is zero.
	want = 0
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNoncesCanBeSnapshottedAndReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is initially 10. This should only be fetched once.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	snapshot0 := db.Snapshot()

	var want uint64 = 10
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	snapshot1 := db.Snapshot()
	want = 11
	db.SetNonce(address1, want)
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	snapshot2 := db.Snapshot()
	want = 12
	db.SetNonce(address1, want)
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot2)
	want = 11
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot1)
	want = 10
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot0)
	want = 10
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNoncesOnlySetCanBeReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is initially 10. This should only be fetched once.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	snapshot0 := db.Snapshot()

	var want uint64 = 11
	db.SetNonce(address1, want)
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot0)
	want = 10
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateNoncesIsWrittenToStateIfChangedAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The updated value is expected to be written to the state.
	mock.EXPECT().SetNonce(address1, common.ToNonce(10)).Return(nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.SetNonce(address1, 10)
	db.EndTransaction()
	db.EndBlock()

	// The second end-of-transaction should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateNoncesOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	mock.EXPECT().SetNonce(address1, common.ToNonce(12)).Return(nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.SetNonce(address1, 10)
	db.SetNonce(address1, 11)
	db.EndTransaction()
	db.SetNonce(address1, 12)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateNoncesUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is only read, never written.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	value := db.GetNonce(address1)
	db.SetNonce(address1, value)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateNoncesIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetNonce create the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	// No other mock call is expected.

	db.SetNonce(address1, 10)
	db.AbortTransaction()
	db.EndBlock()
}

func TestCarmenStateValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateCommittedValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateCommittedValuesAreOnlyFetchedOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	db.GetCommittedState(address1, key1)
	db.GetCommittedState(address1, key1)
}

func TestCarmenStateCommittedValuesCanBeFetchedAfterValueBeingWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	db.SetState(address1, key1, val2)
	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}
}

func TestCarmenStateFetchedCommittedValueIsNotResetInRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The commited state is only read ones
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	snapshot := db.Snapshot()
	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
	db.RevertToSnapshot(snapshot)
	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}
}

func TestCarmenStateWrittenValuesCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateWrittenValuesCanBeUpdated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}

	db.SetState(address1, key1, val2)
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}
}

func TestCarmenStateWrittenValuesCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)

	snapshot0 := db.Snapshot()

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}

	snapshot1 := db.Snapshot()

	db.SetState(address1, key1, val2)
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}

	snapshot2 := db.Snapshot()

	db.SetState(address1, key1, val3)
	if got := db.GetState(address1, key1); got != val3 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val3, got)
	}

	db.RevertToSnapshot(snapshot2)
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}

	db.RevertToSnapshot(snapshot1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}

	db.RevertToSnapshot(snapshot0)
	db.GetState(address1, key1)
	if got := db.GetState(address1, key1); got != val0 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateUpdatedValuesAreCommitedToStateAtEndBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(address1, key1, val1)
	mock.EXPECT().SetStorage(address1, key2, val2)

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateRollbackedValuesAreNotCommited(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(address1, key1, val1)

	db.SetState(address1, key1, val1)
	snapshot := db.Snapshot()
	db.SetState(address1, key2, val2)
	db.RevertToSnapshot(snapshot)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateNothingIsCommitedOnTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// No expectations in mock, will fail if anything is called.

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.AbortTransaction()
	db.EndBlock()
}

func TestCarmenStateOnlyFinalValueIsStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(address1, key1, val3)

	db.SetState(address1, key1, val1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val3)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateUndoneValueUpdateIsNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only expect a read but no update.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	val := db.GetState(address1, key1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateValueIsCommittedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only expect a read but no update.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("expected initial value to be %v, got %v", val1, got)
	}
	db.SetState(address1, key1, val2)
	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("expected committed value to be %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("expected current value to be %v, got %v", val2, got)
	}

	db.EndTransaction()

	if got := db.GetCommittedState(address1, key1); got != val2 {
		t.Errorf("expected committed value to be %v, got %v", val2, got)
	}
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("expected current value to be %v, got %v", val2, got)
	}
	db.SetState(address1, key1, val3)
	if got := db.GetCommittedState(address1, key1); got != val2 {
		t.Errorf("expected committed value to be %v, got %v", val2, got)
	}
	if got := db.GetState(address1, key1); got != val3 {
		t.Errorf("expected current value to be %v, got %v", val3, got)
	}
}

func TestCarmenStateCanBeUsedForMultipleBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(address1, key1, val1)
	mock.EXPECT().SetStorage(address1, key1, val2)
	mock.EXPECT().SetStorage(address1, key1, val3)

	db.SetState(address1, key1, val1)
	db.EndTransaction()
	db.EndBlock()
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.EndBlock()
	db.SetState(address1, key1, val3)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCodesCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodesCanBeSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeUpdatesCoveredByRollbacks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	want1 := []byte{0xAC, 0xDC}
	want2 := []byte{0xFE, 0xEF}

	db.SetCode(address1, want1)
	snapshot := db.Snapshot()

	if got := db.GetCode(address1); !bytes.Equal(got, want1) {
		t.Errorf("error retrieving code, wanted %v, got %v", want1, got)
	}

	db.SetCode(address1, want2)

	if got := db.GetCode(address1); !bytes.Equal(got, want2) {
		t.Errorf("error retrieving code after update, wanted %v, got %v", want2, got)
	}

	db.RevertToSnapshot(snapshot)

	if got := db.GetCode(address1); !bytes.Equal(got, want1) {
		t.Errorf("error retrieving code after rollback, wanted %v, got %v", want1, got)
	}
}

func TestCarmenStateReadCodesAreNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)

	db.GetCode(address1)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateUpdatedCodesAreStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().SetCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateUpdatedCodesAreStoredOnlyOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().SetCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock()

	// No store on second time
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateSettingCodesCreatesAccountsImplicitly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)
	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, common.Balance{}).Return(nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().SetCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock()
}

func TestCarmenStateCodeSizeCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 2
	mock.EXPECT().GetCodeSize(address1).Return(want, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeSizeCanBeReadAfterModification(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCodeSize(address1); got != len(want) {
		t.Errorf("error retrieving code size, wanted %v, got %v", len(want), got)
	}
}

func TestCarmenStateCodeSizeOfANonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 0
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeSizeOfADeletedAccountIsZeroAfterEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing acocunt.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	db.SetCode(address1, []byte{1, 2, 3})
	db.Suicide(address1)

	// The destruction is delayed until the end of the transaction.
	want := 3
	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}

	db.EndTransaction()

	// Now the code should be gone.
	want = 0
	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashOfNonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The state DB is asked for the accounts existence, but not for the hash.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	want := common.Hash{}
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashOfAnExistingAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account with empty code.
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)
	mock.EXPECT().GetCodeHash(address1).Return(common.GetKeccak256Hash([]byte{}), nil)

	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashOfNewlyCreatedAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// At the start the account does not exist.
	mock.EXPECT().GetAccountState(address1).Return(common.Unknown, nil)

	db.CreateAccount(address1)
	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := common.Hash{0xAC, 0xDC}
	mock.EXPECT().GetCodeHash(address1).Return(want, nil)
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateSetCodeSizeCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	want := []byte{0xAB, 0xCD}
	db.SetCode(address1, want)

	snapshot1 := db.Snapshot()
	db.SetCode(address1, []byte{0x12, 0x34, 0x56})

	db.RevertToSnapshot(snapshot1)
	if got := db.GetCodeSize(address1); got != len(want) {
		t.Errorf("failed to roll back set code, wanted %v, got %v", want, got)
	}
	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("failed to roll back set code, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashCanBeReadAfterModification(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetAccountState(address1).Return(common.Exists, nil)

	code := []byte{0xAC, 0xDC}
	db.SetCode(address1, code)

	want := common.GetKeccak256Hash(code)
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateInitialRefundIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	if got := db.GetRefund(); got != 0 {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
}

func TestCarmenStateRefundCanBeModified(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	var want uint64 = 0
	if got := db.GetRefund(); got != want {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
	db.AddRefund(12)
	want += 12
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}
	db.SubRefund(10)
	want -= 10
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}
	db.AddRefund(14)
	want += 14
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateAddedRefundCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	var want uint64 = 0
	if got := db.GetRefund(); got != want {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
	db.AddRefund(12)
	want += 12
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	snapshot1 := db.Snapshot()
	db.AddRefund(14)
	want += 14
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	snapshot2 := db.Snapshot()
	db.AddRefund(16)
	want += 16
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot2)
	want -= 16
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to roll back refund, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot1)
	want -= 14
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to roll back refund, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateRemovedRefundCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	var want uint64 = 0
	if got := db.GetRefund(); got != want {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
	db.AddRefund(100)
	want += 100
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	snapshot1 := db.Snapshot()
	db.SubRefund(14)
	want -= 14
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	snapshot2 := db.Snapshot()
	db.SubRefund(16)
	want -= 16
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot2)
	want += 16
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to roll back refund, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot1)
	want += 14
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to roll back refund, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateRefundIsResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	var want uint64 = 12
	db.AddRefund(12)
	if got := db.GetRefund(); got != 12 {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}
	db.EndTransaction()
	if got := db.GetRefund(); got != 0 {
		t.Errorf("refund after end of transaction is not 0, got: %v", got)
	}
}

func TestCarmenStateRefundIsResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	var want uint64 = 12
	db.AddRefund(12)
	if got := db.GetRefund(); got != 12 {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}
	db.AbortTransaction()
	if got := db.GetRefund(); got != 0 {
		t.Errorf("refund after abort of transaction is not 0, got: %v", got)
	}
}

func TestCarmenStateAccessedAddressesCanBeAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed address list initially not empty")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Accessed address list initially not empty")
	}

	db.AddAddressToAccessList(address1)
	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Added address not in access list")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Non-added address is in access list")
	}

	db.AddAddressToAccessList(address2)
	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Added address not in access list")
	}
	if !db.IsAddressInAccessList(address2) {
		t.Errorf("Added address not in access list")
	}
}

func TestCarmenStateAccessedAddressesCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	snapshot1 := db.Snapshot()

	db.AddAddressToAccessList(address1)
	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Added address not in access list")
	}

	snapshot2 := db.Snapshot()

	db.AddAddressToAccessList(address2)
	if !db.IsAddressInAccessList(address2) {
		t.Errorf("Added address not in access list")
	}

	db.RevertToSnapshot(snapshot2)
	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Address 1 should still be in access list")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Address 2 should be removed by rollback")
	}

	db.RevertToSnapshot(snapshot1)
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Address 1 should be removed by rollback")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Address 2 should be removed by rollback")
	}
}

func TestCarmenStateAccessedAddressesAreResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.EndTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at end of transaction")
	}
}

func TestCarmenStateAccessedAddressesAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.AbortTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at abort of transaction")
	}
}

func TestCarmenStateAccessedSlotsCanBeAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	if _, b := db.IsSlotInAccessList(address1, key1); b {
		t.Errorf("Accessed slot list initially not empty")
	}
	if _, b := db.IsSlotInAccessList(address1, key2); b {
		t.Errorf("Accessed slot list initially not empty")
	}
	if _, b := db.IsSlotInAccessList(address2, key2); b {
		t.Errorf("Accessed slot list initially not empty")
	}

	db.AddSlotToAccessList(address1, key1)
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address1, key2); b {
		t.Errorf("Non-added slot in access list")
	}
	if _, b := db.IsSlotInAccessList(address2, key2); b {
		t.Errorf("Non-added slot in access list")
	}

	db.AddSlotToAccessList(address2, key2)
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address1, key2); b {
		t.Errorf("Non-added slot in access list")
	}
	if _, b := db.IsSlotInAccessList(address2, key2); !b {
		t.Errorf("Added slot not in access list")
	}

	db.AddSlotToAccessList(address1, key2)
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address1, key2); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address2, key2); !b {
		t.Errorf("Added slot not in access list")
	}
}

func TestCarmenStateAddingSlotToAccessListAddsAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Address of accessed slot not in address list")
	}
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
}

func TestCarmenStateAccessedSlotsCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	snapshot1 := db.Snapshot()
	db.AddSlotToAccessList(address1, key1)
	snapshot2 := db.Snapshot()
	db.AddSlotToAccessList(address2, key1)

	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Address of added slot not in access list")
	}
	if !db.IsAddressInAccessList(address2) {
		t.Errorf("Address of added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address2, key1); !b {
		t.Errorf("Added slot not in access list")
	}

	db.RevertToSnapshot(snapshot2)

	if !db.IsAddressInAccessList(address1) {
		t.Errorf("Rollback removed address 1 although still needed")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Address 2 still present after rollback")
	}
	if _, b := db.IsSlotInAccessList(address1, key1); !b {
		t.Errorf("Added slot not in access list")
	}
	if _, b := db.IsSlotInAccessList(address2, key1); b {
		t.Errorf("Added slot not removed by rollback")
	}

	db.RevertToSnapshot(snapshot1)

	if db.IsAddressInAccessList(address1) {
		t.Errorf("Address 1 still present after rollback")
	}
	if db.IsAddressInAccessList(address2) {
		t.Errorf("Address 2 still present after rollback")
	}
	if _, b := db.IsSlotInAccessList(address1, key1); b {
		t.Errorf("Added slot not removed by rollback")
	}
	if _, b := db.IsSlotInAccessList(address2, key1); b {
		t.Errorf("Added slot not removed by rollback")
	}
}

func TestCarmenStateAccessedSlotsAreResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.EndTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at end of transaction")
	}
}

func TestCarmenStateAccessedAddressedAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.AbortTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at abort of transaction")
	}
}

func TestCarmenStateBulkLoadReachesState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	balance, _ := common.ToBalance(big.NewInt(12))
	code := []byte{1, 2, 3}

	mock.EXPECT().CreateAccount(address1).Return(nil)
	mock.EXPECT().SetBalance(address1, balance).Return(nil)
	mock.EXPECT().SetNonce(address1, common.ToNonce(14)).Return(nil)
	mock.EXPECT().SetStorage(address1, key1, val1).Return(nil)
	mock.EXPECT().SetCode(address1, code).Return(nil)
	mock.EXPECT().Flush().Return(nil)
	mock.EXPECT().GetHash().Return(common.Hash{}, nil)

	load := db.StartBulkLoad()
	load.CreateAccount(address1)
	load.SetBalance(address1, big.NewInt(12))
	load.SetNonce(address1, 14)
	load.SetState(address1, key1, val1)
	load.SetCode(address1, code)

	load.Close()
}

func TestCarmenStateGetMemoryFootprintIsReturnedAndNotZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetMemoryFootprint().Return(common.NewMemoryFootprint(0))

	fp := db.GetMemoryFootprint()
	if fp == nil || fp.Total() == 0 {
		t.Errorf("invalid memory footpring: %v", fp)
	}
}

func testCarmenStateDbHashAfterModification(t *testing.T, mod func(s StateDB)) {
	ref_state, err := NewGoMemoryState()
	if err != nil {
		t.Fatalf("failed to create reference state: %v", err)
	}
	ref := CreateStateDBUsing(ref_state)
	defer ref.Close()
	mod(ref)
	ref.EndTransaction()
	ref.EndBlock()
	want := ref.GetHash()
	for i := 0; i < 3; i++ {
		for _, config := range initStates() {
			t.Run(fmt.Sprintf("%v/run=%d", config.name, i), func(t *testing.T) {
				state, err := config.createState(t.TempDir())
				if err != nil {
					t.Fatalf("failed to initialize state %s", config.name)
				}
				stateDb := CreateStateDBUsing(state)
				defer stateDb.Close()

				mod(stateDb)
				stateDb.EndTransaction()
				stateDb.EndBlock()
				if got := stateDb.GetHash(); want != got {
					t.Errorf("Invalid hash, wanted %v, got %v", want, got)
				}
			})
		}
	}
}

func TestCarmenStateHashIsDeterministicForEmptyState(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		// nothing
	})
}

func TestCarmenStateHashIsDeterministicForSingleUpdate(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.SetState(address1, key1, val1)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleUpdate(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.SetState(address1, key1, val1)
		s.SetState(address2, key2, val2)
		s.SetState(address3, key3, val3)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleAccountCreations(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleAccountModifications(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.Suicide(address2)
		s.Suicide(address1)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleBalanceUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.AddBalance(address1, big.NewInt(12))
		s.AddBalance(address2, big.NewInt(14))
		s.AddBalance(address3, big.NewInt(16))
		s.SubBalance(address3, big.NewInt(8))
	})
}

func TestCarmenStateHashIsDeterministicForMultipleNonceUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.SetNonce(address1, 12)
		s.SetNonce(address2, 14)
		s.SetNonce(address3, 18)
	})
}

func TestCarmenStateHashIsDeterministicForMultipleCodeUpdates(t *testing.T) {
	testCarmenStateDbHashAfterModification(t, func(s StateDB) {
		s.SetCode(address1, []byte{0xAC})
		s.SetCode(address2, []byte{0xDC})
		s.SetCode(address3, []byte{0x20})
	})
}

func TestSlotIdOrder(t *testing.T) {
	inputs := []struct {
		a, b slotId
		want int
	}{
		// Some cases with equal values.
		{slotId{address1, key1}, slotId{address1, key1}, 0},
		{slotId{address1, key2}, slotId{address1, key2}, 0},
		{slotId{address2, key1}, slotId{address2, key1}, 0},
		// Some cases where the address makes the difference.
		{slotId{address1, key1}, slotId{address2, key1}, -1},
		{slotId{address3, key1}, slotId{address2, key1}, 1},
		// Some cases where the key makes the difference.
		{slotId{address2, key1}, slotId{address2, key2}, -1},
		{slotId{address2, key3}, slotId{address2, key2}, 1},
	}
	for _, input := range inputs {
		if got := input.a.Compare(&input.b); got != input.want {
			t.Errorf("Comparison of %v and %v failed, wanted %d, got %d", input.a, input.b, input.want, got)
		}
	}
}

const numSlots = 1000

// TestPersistentStateDB modifies stateDB first, then it is closed and is re-opened in another process,
// and it is tested that data are available, i.e. all was successfully persisted
func TestPersistentStateDB(t *testing.T) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {

			// skip in-memory
			if config.name == "cpp-InMemory" || config.name == "go-Memory" {
				return
			}

			dir := t.TempDir()
			s, err := config.createState(dir)
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}

			stateDb := CreateStateDBUsing(s)

			stateDb.BeginEpoch()
			stateDb.BeginBlock()
			stateDb.BeginTransaction()

			// init state DB data
			stateDb.CreateAccount(address1)
			stateDb.AddBalance(address1, big.NewInt(153))
			stateDb.SetNonce(address1, 58)
			stateDb.SetCode(address1, []byte{1, 2, 3})

			// insert number of slots to address 1
			for i := 0; i < numSlots; i++ {
				val := toVal(uint64(i))
				stateDb.SetState(address1, toKey(uint64(i)), val)
			}

			stateDb.EndTransaction()
			stateDb.EndBlock()
			stateDb.BeginBlock()
			stateDb.BeginTransaction()

			stateDb.CreateAccount(address2)
			stateDb.AddBalance(address2, big.NewInt(6789))
			stateDb.SetNonce(address2, 91)
			stateDb.SetCode(address2, []byte{3, 2, 1})

			// insert number of slots to address 2
			for i := 0; i < numSlots; i++ {
				val := toVal(uint64(i + numSlots))
				stateDb.SetState(address2, toKey(uint64(i)), val)
			}

			stateDb.EndTransaction()
			stateDb.EndBlock()
			stateDb.EndEpoch(1)

			if err := stateDb.Close(); err != nil {
				t.Errorf("Cannot close state: %e", err)
			}

			execSubProcessTest(t, dir, config.name, "TestStateDBRead")
		})
	}
}

// TestStateDBRead verifies data are available in a stateDB.
// The given state reads the data from the given directory and verifies the data are present.
// Name of the index and directory is provided as command line arguments
func TestStateDBRead(t *testing.T) {
	// do not runt this test stand-alone
	if *stateDir == "DEFAULT" {
		return
	}

	s := createState(t, *stateImpl, *stateDir)
	defer func() {
		_ = s.Close()
	}()

	stateDb := CreateStateDBUsing(s)

	if state := stateDb.getAccountState(address1); state != common.Exists {
		t.Errorf("Unexpected value, val: %v != %v", state, common.Exists)
	}
	if state := stateDb.getAccountState(address2); state != common.Exists {
		t.Errorf("Unexpected value, val: %v != %v", state, common.Exists)
	}

	if balance := stateDb.GetBalance(address1); balance.Cmp(big.NewInt(153)) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := stateDb.GetBalance(address2); balance.Cmp(big.NewInt(6789)) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", balance, 6789)
	}

	if nonce := stateDb.GetNonce(address1); nonce != 58 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 58)
	}
	if nonce := stateDb.GetNonce(address2); nonce != 91 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 91)
	}

	if code := stateDb.GetCode(address1); bytes.Compare(code, []byte{1, 2, 3}) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{1, 2, 3})
	}
	if code := stateDb.GetCode(address2); bytes.Compare(code, []byte{3, 2, 1}) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{3, 2, 1})
	}

	// slots in address 1
	for i := 0; i < numSlots; i++ {
		val := toVal(uint64(i))
		key := toKey(uint64(i))
		if storage := stateDb.GetState(address1, key); storage != val {
			t.Errorf("Unexpected value, val: %v != %v", storage, val)
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
