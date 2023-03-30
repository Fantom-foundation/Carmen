package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/golang/mock/gomock"
)

func TestCarmenStateImplementsStateDbInterface(t *testing.T) {
	var db stateDB
	var _ StateDB = &db
}

func prepareMockState(ctrl *gomock.Controller) *MockdirectUpdateState {
	mock := NewMockdirectUpdateState(ctrl)
	// Implement the Apply() function by distributing the calls among specialized functions.
	mock.
		EXPECT().
		Apply(gomock.Any(), gomock.Any()).
		DoAndReturn(func(block uint64, update common.Update) error {
			// Check that the produced update is valid.
			if err := update.Check(); err != nil {
				ctrl.T.Errorf("Update invalid: %v", err)
			}
			// Distribute the update among the individual setters.
			return applyUpdate(mock, update)
		}).
		AnyTimes()
	return mock
}

func TestCarmenStateAccountsCanBeCreatedAndDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the account creation needs to check whether the account exists
	mock.EXPECT().Exists(address1).Return(false, nil)
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

	if !db.Exist(address1) {
		t.Errorf("Destroyed account should still be considered to exist until the end of the transaction")
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)

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

func TestCarmenStateCreateAccountSetsStorageToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)

	if got := db.GetState(address1, key1); got != (common.Value{}) {
		t.Errorf("state not initialized with zero")
	}
}

func TestCarmenStateRecreateingAnAccountSetsStorageToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	db.CreateAccount(address1)
	db.SetState(address1, key1, val1)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("state not set to specified value")
	}

	// re-creating an account in the same transaction is clearing the state
	db.CreateAccount(address1)
	if got := db.GetState(address1, key1); got != (common.Value{}) {
		t.Errorf("state not set to specified value")
	}

	db.EndBlock(1)
}

func TestCarmenStateRecreatingAccountSetsNonceCodeAndBalanceToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a previously deleted account.
	mock.EXPECT().Exists(address1).Return(false, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account is non-exisiting, it gets recreated.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{0, 0, 0, 0, 0, 0, 0, 1}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// First transaction creates an account and sets some storage values.
	db.BeginTransaction()
	db.CreateAccount(address1)
	db.SetNonce(address1, 1)
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
	db.SetNonce(address1, 1)

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
	db.EndBlock(1)
}

func TestCarmenStateRecreatingAccountResetsStorageButRetainsNewState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists with some state.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	// At the end the account is recreated with the new state.
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(12)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setStorage(address1, key1, val2).Return(nil)

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
	db.SetNonce(address1, 12) // to avoid empty-account deletion
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
	db.EndBlock(123)
}

func TestCarmenStateDestroyingRecreatedAccountIsNotResettingClearingState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account exists with some state.
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.Suicide(address1)
	if db.clearedAccounts[address1] != pendingClearing {
		t.Errorf("destroyed account is not marked for clearing")
	}

	db.CreateAccount(address1)
	if db.clearedAccounts[address1] != cleared {
		t.Errorf("recreated account was not cleared")
	}

	db.GetState(address1, key1) // should not reach the store (no expectation stated above)

	db.Suicide(address1)
	if db.clearedAccounts[address1] != cleared {
		t.Errorf("destroyed recreated account is no longer considered cleared")
	}

	db.GetState(address1, key1) // should not reach the store (no expectation stated above)
}

func TestCarmenStateStorageOfDestroyedAccountIsStillAccessibleTillEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account existis with some values inside.
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists and has a slot value set.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	// During the processing the account is deleted.
	mock.EXPECT().deleteAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// The second value fetched in the last block must also be retrieved from the store.
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	// In the first transaction key1 is fetched, ending up in the store data cache.
	db.BeginTransaction()
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("unexpected value, wanted %v, got %v", val1, got)
	}
	db.EndTransaction()
	db.EndBlock(1) // < stored value remains in store data cache

	// In the next block the account is destroyed
	db.BeginTransaction()
	db.Suicide(address1)
	db.EndTransaction()
	db.BeginTransaction()
	// This value is zero because within this block the address1 was cleared.
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("unexpected value, wanted %v, got %v", zero, got)
	}
	db.SetState(address1, key2, val2) // < implicitly re-creates an empty account, which should be removed at the end of the block
	db.EndTransaction()
	db.EndBlock(2) // < here the stored data cache is reset to forget the old state; also, key2/val2 is stored in DB

	// At this point address1 should be all empty -- in the store and the caches.

	// In this block we try to read the value again. This time it is not cached
	// in the snapshot state nor is the account marked as being cleared. The value
	// is retrieved from the store data cache.
	db.BeginTransaction()
	// This value is now fetched from the value store, which is supposed to be cleared
	// at the end of the block deleting the account.
	if got := db.GetState(address1, key1); got != zero {
		t.Errorf("unexpected value, wanted %v, got %v", zero, got)
	}
	// The value for key2 is also retrieved from the value store.
	if got := db.GetState(address1, key2); got != val2 {
		t.Errorf("unexpected value, wanted %v, got %v", val2, got)
	}
	db.EndTransaction()
	db.EndBlock(3)
}

func TestCarmenStateRollingBackSuicideRestoresValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	db.EndBlock(1) // < no change is send to the DB
}

func TestCarmenStateDestroyingAndRecreatingAnAccountInTheSameTransactionCallsDeleteAndCreateAccountOnStateDb(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)

	// The account is to be re-created.
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{0, 0, 0, 0, 0, 0, 0, 1}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// In a transaction we destroy the account and recreate it. This should cause
	// the account to be deleted and re-initialized in the StateDB at the end of
	// the block.
	db.BeginTransaction()

	if !db.Exist(address1) {
		t.Errorf("Account does not initially exist!")
	}

	db.Suicide(address1)
	db.CreateAccount(address1)
	db.SetNonce(address1, 1)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateDoubleDestroyedAccountThatIsOnceRolledBackIsStillCleared(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account is exisiting with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)

	// The account is to be re-created.
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{0, 0, 0, 0, 0, 0, 0, 1}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// In a transaction we destroy the account, re-create it, destroy it and roll back
	// the second destroy; After this, the account still needs to be cleared at the
	// end of the block since the first destroy is to be committed.
	db.BeginTransaction()

	if !db.Exist(address1) {
		t.Errorf("Account does not initially exist!")
	}

	db.Suicide(address1)
	db.CreateAccount(address1)
	db.SetNonce(address1, 1)

	snapshot := db.Snapshot()
	db.Suicide(address1)
	db.RevertToSnapshot(snapshot)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateRecreatingExistingAccountSetsNonceAndCodeToZeroAndPreservesBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a previously deleted account.
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("failed to set up test case: %v", err)
	}
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	mock.EXPECT().Exists(address1).Return(false, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)

	// An existing account is indicated as being deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that existing account did not exist before delete")
	}

	// A suicided account suicide should still return true - it is being deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that suicided account is not being deleted")
	}

	// The account should really stop existing at the end of the transaction.
	db.EndTransaction()
	db.BeginTransaction()

	// Deleting it a second time indicates the account as already deleted.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates deleted account still existed")
	}
}

func TestCarmenStateSetCodeShouldNotStopSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{1}, nil)

	// An existing account is indicated as being deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that existing account did not exist before delete")
	}

	// Setting the account code in the same tx should not stop account deleting.
	db.SetCode(address1, []byte{})

	// The account should really stop existing at the end of the transaction.
	db.EndTransaction()
	db.BeginTransaction()

	// Deleting it a second time indicates the account as already deleted.
	if exists := db.Suicide(address1); exists {
		t.Errorf("suicide indicates deleted account still existed")
	}
}

func TestCarmenStateRepeatedSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)

	// An existing account is indicated as being deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that existing account did not exist before delete")
	}

	// The account start to be considered deleted at the end of a transaction.
	db.EndTransaction()
	db.BeginTransaction()

	// Adding balance should re-create the account.
	db.AddBalance(address1, big.NewInt(123))
	db.SetState(address1, key1, val1)

	// Deleting it a second time indicates the account as already deleted.
	if exists := db.Suicide(address1); !exists {
		t.Errorf("suicide indicates that re-created account does not exist")
	}

	// The account start to be considered deleted at the end of a transaction.
	db.EndTransaction()
	db.BeginTransaction()

	// Adding balance should re-create the account again.
	db.AddBalance(address1, big.NewInt(456))
	db.SetState(address1, key2, val2)

	// The original account is expected to be deleted, the last created one is expected to be really created.
	newBalance, _ := common.ToBalance(big.NewInt(456))
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, newBalance).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setStorage(address1, key2, val2)

	// The changes are applied to the state at the end of the block.
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateSuicideIndicatesUnknownAccountAsNotBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an unknown account.
	mock.EXPECT().Exists(address1).Return(false, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a deleted account.
	mock.EXPECT().Exists(address1).Return(false, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("error preparing test: %v", err)
	}
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("error preparing test: %v", err)
	}
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().deleteAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	db.Suicide(address1)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateSuicideCanBeCanceledThroughRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(5)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{1, 2, 3}).Return(nil)

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	snapshot := db.Snapshot()
	db.Suicide(address1)
	db.RevertToSnapshot(snapshot)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCreatedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is created at the end of the transaction.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(1)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)

	db.CreateAccount(address1)
	db.SetNonce(address1, 1) // the account must not be empty
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCreatedAccountsAreForgottenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The created account is only created once, and nonces and code are initialized.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(1)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)

	db.CreateAccount(address1)
	db.SetNonce(address1, 1)
	db.EndTransaction()
	db.EndBlock(1)
	db.EndBlock(2)
}

func TestCarmenStateCreatedAccountsAreDiscardedOnEndOfAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Needs to check whether the account already existed before the creation.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)
	db.AbortTransaction()
	db.EndBlock(1)
	db.EndBlock(2)
}

func TestCarmenStateDeletedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().deleteAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(0)).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)

	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateDeletedAccountsRetainCodeUntilEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Exists(address1).Return(false, nil)

	code := []byte{1, 2, 3}
	db.CreateAccount(address1)
	db.SetCode(address1, code)
	db.EndTransaction()

	if got := db.GetCode(address1); !bytes.Equal(got, code) {
		t.Errorf("retrieved wrong code, got %v, wanted %v", got, code)
	}

	db.Suicide(address1)

	// Now the code should still exist
	if got := db.GetCode(address1); !bytes.Equal(got, code) {
		t.Errorf("retrieved wrong code, got %v, wanted %v", got, code)
	}

	db.EndTransaction()

	// Now the code should be gone.
	if got := db.GetCode(address1); len(got) != 0 {
		t.Errorf("retrieved wrong code, got %v, wanted empty code", got)
	}

	db.EndBlock(1)
}

func TestCarmenStateDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCreatedAndDeletedAccountsAreDeletedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCreatedAndDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestCarmenStateEmptyAccountsAreRecognized(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its balance and nonce set to zero.
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	addedBalance := big.NewInt(5)
	balance, _ := common.ToBalance(addedBalance)

	// The account have not existed - must be created by AddBalance call.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, balance).Return(nil)

	db.AddBalance(address1, addedBalance)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after adding balance")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateAddingZeroBalanceCreatesAccountThatIsImplicitlyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially, the account does not exist, and it is not created, since it remains empty.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	db.AddBalance(address1, big.NewInt(0))
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after adding balance")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateSubtractingZeroBalanceCreatesAccountThatIsImplicitlyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially, the account does not exist, and it is not created, since it remains empty.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	db.SubBalance(address1, big.NewInt(0))
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after subtracting balance")
	}

	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateSettingTheNonceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its nonce and code set to zero.
	mock.EXPECT().Exists(address1).Return(false, nil)

	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(1)).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	db.CreateAccount(address1)
	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.SetNonce(address1, 1)
	if db.Empty(address1) {
		t.Errorf("Account with nonce != 0 is still considered empty")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCreatesAccountOnNonceSetting(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account does not exist, is expected to be created automatically.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(1)).Return(nil)

	db.SetNonce(address1, 1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after setting the nonce")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateGetBalanceReturnsFreshCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is initially 10. This should only be fetched once.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(12))
	mock.EXPECT().setBalance(address1, balance).Return(nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock(1)

	// The second end-of-block should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock(2)
}

func TestCarmenStateBalanceOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(14))
	mock.EXPECT().setBalance(address1, balance).Return(nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.AddBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(3))
	db.EndTransaction()
	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateBalanceUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.SubBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(5))
	db.EndTransaction()
	db.EndBlock(2)
}

func TestCarmenStateBalanceIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestCarmenStateNoncesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetNonce creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The side-effects of the creation of the account in the first transactions are expected.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(12)).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// Also the fetch of the Nonce value in the second transaction is expected.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(12), nil)

	// Create an account and set the nonce.
	db.CreateAccount(address1)
	db.SetNonce(address1, 12)
	db.EndTransaction()
	db.EndBlock(1)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is initially 10. This should only be fetched once.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is initially 10. This should only be fetched once.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The updated value is expected to be written to the state.
	mock.EXPECT().setNonce(address1, common.ToNonce(10)).Return(nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetNonce(address1, 10)
	db.EndTransaction()
	db.EndBlock(1)

	// The second end-of-transaction should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock(2)
}

func TestCarmenStateNoncesOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	mock.EXPECT().setNonce(address1, common.ToNonce(12)).Return(nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetNonce(address1, 10)
	db.SetNonce(address1, 11)
	db.EndTransaction()
	db.SetNonce(address1, 12)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateNoncesUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is only read, never written.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	value := db.GetNonce(address1)
	db.SetNonce(address1, value)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateNoncesIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)
	// No other mock call is expected.

	db.SetNonce(address1, 10)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestCarmenStateValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateCommittedValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateCommittedValuesAreOnlyFetchedOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	db.GetCommittedState(address1, key1)
	db.GetCommittedState(address1, key1)
}

func TestCarmenStateCommittedValuesCanBeFetchedAfterValueBeingWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	db.SetState(address1, key1, val2)
	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}
}

func TestCarmenStateSettingValuesCreatesAccountsImplicitly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(common.Value{}, nil)

	db.SetState(address1, key1, val1)
	if !db.Exist(address1) {
		t.Errorf("no implicit account creation by SetState")
	}
}

func TestCarmenStateImplicitAccountCreatedBySetStateIsDroppedSinceEmptyIfNothingElseIsSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account is not created at the end of the transaction, nor is the value set.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(common.Value{}, nil)

	db.SetState(address1, key1, val1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenState_GethAlignment_ImplicitAccountCreatedBySetStateIsNotDroppedDueToEmptinessIfTheSetIsNotChangingTheValueOfTheSlot(t *testing.T) {
	// This behaviour was discovered in geth, which is implicitly creating an account when
	// setting a value, however, not registering it for an empty check at the end of a block
	// if the assigned value is not different than the previous value.
	// The account is implicitly created here: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/statedb.go#L437
	// The value would be set here: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/state_object.go#L296
	// And the registry for an empty yet would happen with an addition to the journal here: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/state_object.go#L291-L295
	// But the condition here prevents the registration: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/state_object.go#L285-L289

	// This problem should not occure on the chain, since any account for which a value is set is non-empty.

	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The targeted account initially does not exit.
	mock.EXPECT().Exists(address1).Return(false, nil)

	// The targeted account is created, although it is empty
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// In an earlier transaction, the account is created and dropped because it is empty.
	// As a side effect, it is remembered as being accessed, setting the stage for the test below.
	db.CreateAccount(address1)
	db.EndTransaction()
	if _, found := db.accessedAccounts[address1]; !found {
		t.Fatalf("failed to set preconditions for test")
	}

	// The account is implicitly created by setting a storage location to zero (which is the value it had before).
	value := db.GetState(address1, key1)
	db.SetState(address1, key1, value)

	if !db.Exist(address1) {
		t.Errorf("account was not implicitly created")
	}

	// At this point the account is empty ...
	if !db.Empty(address1) {
		t.Errorf("account is not considered empty")
	}

	// ... and one would expect it to be deleted by the end of the transaction ...
	db.EndTransaction()

	// ... but due to the issue in geth, this should not happen. The account must survive.
	if !db.Exist(address1) {
		t.Errorf("account did not survive")
	}

	db.EndBlock(1)
}

func TestCarmenEmptyAccountsDeletedAtEndOfTransactionsAreCleaned(t *testing.T) {
	// This issue was discovered using Aida Stochastic fuzzing. State information
	// was not properly cleaned at the end of consecutive transactions writing
	// storage values into empty accounts.
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	db.BeginTransaction()
	db.SetState(address1, key1, val1)
	if db.GetState(address1, key1) != val1 {
		t.Errorf("failed to set state to non-existing account")
	}
	db.EndTransaction() // < should delete the account and its state

	db.BeginTransaction()
	if db.GetState(address1, key1) != (common.Value{}) {
		t.Errorf("storage of empty account was not cleaned at end of the first transaction")
	}
	db.SetState(address1, key1, val1)
	db.EndTransaction() // < should be again deleted

	db.BeginTransaction()
	if db.GetState(address1, key1) != (common.Value{}) {
		t.Errorf("storage of empty account was not cleaned at end of the second transaction")
	}
	db.SetState(address1, key1, val1)
	db.EndTransaction()
}

func TestCarmenState_GethAlignment_RecreatedAccountsPreviouslySeenAreNotMarkedForEmptyChecks(t *testing.T) {
	// This behaviour was discovered in geth, and is likely a bug in Geth's state implementation.
	// When recreating an account, geth differentiates between accounts that have been
	// loaded before (as part of the same block), and accounts that have been unseen.
	// If the account has not been seen before, a `createObjectChange` event is logged, which marks
	// the created account as a potential empty account to be checked at the end of the transaction.
	// However, if the account has been seen before, a `resetObjectChange` event is registered,
	// which does not mark the new account as a potential empty account. By failing to do so, an
	// empty, re-created account survives the end of the transaction.
	//
	// The function creating account information (stateObjects) is here: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/statedb.go#L620
	// This is the code causing the issue: https://github.com/Fantom-foundation/go-ethereum-substate/blob/main/core/state/statedb.go#L631-L635

	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The targeted account initially exists, and is not empty.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(common.Balance{}, nil)

	// The account is re-created in the first transaction, and not implicitly deleted.
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// Initially, the account is not accessed.
	if _, accessed := db.accessedAccounts[address1]; accessed {
		t.Errorf("the account should not have been accessed already")
	}

	// First transaction: the account is re-created the first time.
	db.CreateAccount(address1)

	// The account exists now, and is remembered as being accessed.
	if _, accessed := db.accessedAccounts[address1]; !db.Exist(address1) || !accessed {
		t.Errorf("account in invalid state")
	}

	// At this point, the account should get removed.
	db.EndTransaction()

	if db.Exist(address1) {
		t.Errorf("account in invalid state")
	}

	// In the second transaction, the account is re-created.
	db.CreateAccount(address1)

	// At this point the account exists, and is empty.
	if !(db.Exist(address1) && db.Empty(address1)) {
		t.Errorf("the account does not exist or is not empty")
	}

	// At this point, the account is empty and was touched, so it should
	// be deleted at the end of the transaction. However, since the account
	// was only re-created in the current transaction, geth does not consider
	// it dirty, and thus does not check whether it is empty. Consequently,
	// the account survives (likely accidentialy).
	db.EndTransaction()

	// To match geth, we expect the account to exist, although it most likely
	// should be deleted since it is empty.
	if !db.Exist(address1) {
		t.Errorf("the empty account should have survived")
	}

	db.EndBlock(1)
}

func TestCarmenState_GethAlignment_ImplicitlyRecreatedAccountsPreviouslySeenAreNotMarkedForEmptyChecks(t *testing.T) {
	// This is the same as above, but for implicitly recreated accounts.
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The targeted account initially exists, and is not empty.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(common.Balance{}, nil)

	// The account is re-created in the first transaction, and not implicitly deleted.
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// Initially, the account is not accessed.
	if _, accessed := db.accessedAccounts[address1]; accessed {
		t.Errorf("the account should not have been accessed already")
	}

	// First transaction: the account is re-created the first time.
	db.CreateAccount(address1)

	// The account exists now, and is remembered as being accessed.
	if _, accessed := db.accessedAccounts[address1]; !db.Exist(address1) || !accessed {
		t.Errorf("account in invalid state")
	}

	// At this point, the account should get removed.
	db.EndTransaction()

	if db.Exist(address1) {
		t.Errorf("account in invalid state")
	}

	// In the second transaction, the account is implicitly re-created.
	db.SetState(address1, key1, val0) // val0 is used to not triger the inclusion in the emptyCandidates list due to the changed value

	// At this point the account exists, and is empty.
	if !(db.Exist(address1) && db.Empty(address1)) {
		t.Errorf("the account does not exist or is not empty")
	}

	// At this point, the account is empty and was touched, so it should
	// be deleted at the end of the transaction. However, since the account
	// was only re-created in the current transaction, geth does not consider
	// it dirty, and thus does not check whether it is empty. Consequently,
	// the account survives (likely accidentialy).
	db.EndTransaction()

	// To match geth, we expect the account to exist, although it most likely
	// should be deleted since it is empty.
	if !db.Exist(address1) {
		t.Errorf("the empty account should have survived")
	}

	db.EndBlock(1)
}

func TestCarmenStateFetchedCommittedValueIsNotResetInRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(common.Value{}, nil)

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestCarmenStateWrittenValuesCanBeUpdated(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(common.Value{}, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account exists and is non-empty.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(common.Value{}, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(common.Value{}, nil)

	mock.EXPECT().setStorage(address1, key1, val1)
	mock.EXPECT().setStorage(address1, key2, val2)

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateRollbackedValuesAreNotCommited(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val0, nil)
	mock.EXPECT().setStorage(address1, key1, val1)

	db.SetState(address1, key1, val1)
	snapshot := db.Snapshot()
	db.SetState(address1, key2, val2)
	db.RevertToSnapshot(snapshot)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateNothingIsCommitedOnTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Should test whether the account exists, nothing else.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val0, nil)

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestCarmenStateOnlyFinalValueIsStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	mock.EXPECT().setStorage(address1, key1, val3)

	db.SetState(address1, key1, val1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val3)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateUndoneValueUpdateIsNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only expect a read but no update.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	val := db.GetState(address1, key1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateValueIsCommittedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only expect a read but no update.
	mock.EXPECT().Exists(address1).Return(true, nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Times(3).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	mock.EXPECT().setStorage(address1, key1, val1)
	mock.EXPECT().setStorage(address1, key1, val2)
	mock.EXPECT().setStorage(address1, key1, val3)

	db.SetState(address1, key1, val1)
	db.EndTransaction()
	db.EndBlock(1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.EndBlock(2)
	db.SetState(address1, key1, val3)
	db.EndTransaction()
	db.EndBlock(3)
}

func TestCarmenStateCodesCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodesCanBeSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeUpdatesCoveredByRollbacks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)

	db.GetCode(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateUpdatedCodesAreStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().setCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateUpdatedCodesAreStoredOnlyOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().setCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)

	// No store on second time
	db.EndTransaction()
	db.EndBlock(2)
}

func TestCarmenStateSettingCodesCreatesAccountsImplicitly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)

	// In an earlier transaction, the account is created and dropped because it is empty.
	// As a side effect, it is remembered as being accessed, setting the stage for the test below.
	// Otherwise, the implicit creation of the account would schedule it for deletion at the end of the transaction.
	db.CreateAccount(address1)
	db.EndTransaction()
	if _, found := db.accessedAccounts[address1]; !found {
		t.Fatalf("failed to set preconditions for test")
	}

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().setCode(address1, want).Return(nil)

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestCarmenStateCodeSizeCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 2
	mock.EXPECT().GetCodeSize(address1).Return(want, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeSizeCanBeReadAfterModification(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCodeSize(address1); got != len(want) {
		t.Errorf("error retrieving code size, wanted %v, got %v", len(want), got)
	}
}

func TestCarmenStateCodeSizeOfANonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 0
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeSizeOfADeletedAccountIsZeroAfterEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The state DB is asked for the accounts existence, but not for the hash.
	mock.EXPECT().Exists(address1).Return(false, nil)

	want := common.Hash{}
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashOfAnExistingAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account with empty code.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetCodeHash(address1).Return(common.GetKeccak256Hash([]byte{}), nil)

	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashOfNewlyCreatedAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// At the start the account does not exist.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)
	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateCodeHashCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := common.Hash{0xAC, 0xDC}
	mock.EXPECT().GetCodeHash(address1).Return(want, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateSetCodeSizeCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)

	code := []byte{0xAC, 0xDC}
	db.SetCode(address1, code)

	want := common.GetKeccak256Hash(code)
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateInitialRefundIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	if got := db.GetRefund(); got != 0 {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
}

func TestCarmenStateRefundCanBeModified(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.EndTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at end of transaction")
	}
}

func TestCarmenStateAccessedAddressesAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.AbortTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at abort of transaction")
	}
}

func TestCarmenStateAccessedSlotsCanBeAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.EndTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at end of transaction")
	}
}

func TestCarmenStateAccessedAddressedAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.AbortTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at abort of transaction")
	}
}

// EIP-161: At the end of the transaction, any account touched by the execution of that transaction
// which is now empty SHALL instead become non-existent (i.e. deleted).
func TestCarmenDeletesEmptyAccountsEip161(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("failed to set up test case: %v", err)
	}

	// Initially the account exists, its balance is non-zero
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(b12, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	// Set the account balance to zero - the account becomes empty
	db.SubBalance(address1, big.NewInt(12))

	// The account should be deleted at the end of the transaction
	db.EndTransaction()
	db.BeginTransaction()
	if db.Exist(address1) {
		t.Errorf("Empty account have not beed deleted at the end of the transaction")
	}
	db.EndTransaction()

	// The account is deleted in the state at the end of the block
	mock.EXPECT().deleteAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)
	db.EndBlock(1)
}

func TestCarmenNeverCreatesEmptyAccountsEip161(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Exists(address2).Return(false, nil)
	mock.EXPECT().Exists(address3).Return(false, nil)
	mock.EXPECT().GetNonce(address2).Return(common.Nonce{}, nil)
	mock.EXPECT().GetNonce(address3).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address2).Return(0, nil)

	db.BeginBlock()
	db.BeginTransaction()
	// create account 1 explicitly, keeping it empty
	db.CreateAccount(address1)
	// create account 2 by adding balance, but setting it to zero immediately
	db.AddBalance(address2, big.NewInt(12))
	db.SubBalance(address2, big.NewInt(12))
	// create account 3 by setting code, but setting it to empty immediately
	db.SetCode(address3, []byte{0x12})
	db.SetCode(address3, []byte{})
	db.EndTransaction()

	db.EndBlock(1)
}

func TestCarmenStateSuicidedAccountNotRecreatedBySettingBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val0, nil)
	// The account will be deleted.
	mock.EXPECT().deleteAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address1, common.Nonce{}).Return(nil)
	mock.EXPECT().setCode(address1, []byte{}).Return(nil)

	// The account is suicided
	db.Suicide(address1)
	// Writes into suicided account should be lost, account should not be created
	db.AddBalance(address1, big.NewInt(12))
	db.SetNonce(address1, 4321)
	db.SetCode(address1, []byte{0x12, 0x34})
	db.SetState(address1, key1, val1)

	// The account must stay marked for removing
	if !db.HasSuicided(address1) {
		t.Errorf("address is no longer suicided")
	}
	// Until the end of transaction, account needs to behave as usual
	if big.NewInt(12).Cmp(db.GetBalance(address1)) != 0 {
		t.Errorf("changed balance lost")
	}
	if db.GetNonce(address1) != 4321 {
		t.Errorf("changed nonce lost")
	}
	if !bytes.Equal(db.GetCode(address1), []byte{0x12, 0x34}) {
		t.Errorf("changed code lost")
	}
	if db.GetState(address1, key1) != val1 {
		t.Errorf("changed storage lost")
	}

	db.EndTransaction()

	// After the end of transaction, the account should be deleted
	if db.HasSuicided(address1) {
		t.Errorf("address is suicided even after deleting")
	}
	if big.NewInt(0).Cmp(db.GetBalance(address1)) != 0 {
		t.Errorf("balance not deleted")
	}
	if db.GetNonce(address1) != 0 {
		t.Errorf("nonce not deleted")
	}
	if len(db.GetCode(address1)) != 0 {
		t.Errorf("code not deleted")
	}
	if db.GetState(address1, key1) != (common.Value{}) {
		t.Errorf("storage not deleted")
	}

	db.EndBlock(1)
}

func TestCarmenState_AccessedAccountsAreCoveredBySnapshotReverts(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Exists(address2).Return(false, nil)

	want := 0
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	snapshot1 := db.Snapshot()

	db.CreateAccount(address1)

	want++
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	snapshot2 := db.Snapshot()

	db.SetNonce(address2, 12)

	want++
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot2)

	want--
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot1)

	want--
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}
}

func TestCarmenState_AccessedAccountsAreClearedAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Exists(address2).Return(false, nil)
	mock.EXPECT().Exists(address3).Return(false, nil)

	// Only account 2 is created since the rest remains empty.
	mock.EXPECT().createAccount(address2).Return(nil)
	mock.EXPECT().setBalance(address2, common.Balance{}).Return(nil)
	mock.EXPECT().setNonce(address2, common.ToNonce(12)).Return(nil)

	want := 0
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	db.CreateAccount(address1)

	want++
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	db.SetNonce(address2, 12)

	want++
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	// The end of a transaction does not clear the accessed accounts.
	db.EndTransaction()
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	// Accessed accounts accumulate over blocks.
	db.CreateAccount(address3)

	want++
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}

	db.EndTransaction()

	// The end of a block does.
	db.EndBlock(1)

	want = 0
	if got := len(db.accessedAccounts); want != got {
		t.Errorf("unexpected number of accessed accounts, wanted %v, got %v", want, got)
	}
}

func TestCarmenStateBulkLoadReachesState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	balance, _ := common.ToBalance(big.NewInt(12))
	code := []byte{1, 2, 3}

	mock.EXPECT().createAccount(address1).Return(nil)
	mock.EXPECT().setBalance(address1, balance).Return(nil)
	mock.EXPECT().setNonce(address1, common.ToNonce(14)).Return(nil)
	mock.EXPECT().setStorage(address1, key1, val1).Return(nil)
	mock.EXPECT().setCode(address1, code).Return(nil)
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
	mock := prepareMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetMemoryFootprint().Return(common.NewMemoryFootprint(0))

	fp := db.GetMemoryFootprint()
	if fp == nil || fp.Total() == 0 {
		t.Errorf("invalid memory footpring: %v", fp)
	}
}

func testCarmenStateDbHashAfterModification(t *testing.T, mod func(s StateDB)) {
	want := map[StateSchema]common.Hash{}
	for _, s := range GetAllSchemas() {
		ref_state, err := NewCppInMemoryState(Parameters{Directory: t.TempDir(), Schema: s})
		if err != nil {
			t.Fatalf("failed to create reference state: %v", err)
		}
		ref := CreateStateDBUsing(ref_state)
		defer ref.Close()
		mod(ref)
		ref.EndTransaction()
		ref.EndBlock(1)
		want[s] = ref.GetHash()
	}
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
				stateDb.EndBlock(1)
				if got := stateDb.GetHash(); want[config.schema] != got {
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
		// skip in-memory
		if strings.HasPrefix(config.name, "cpp-memory") || strings.HasPrefix(config.name, "go-Memory") {
			continue
		}
		for _, archiveType := range []ArchiveType{LevelDbArchive, SqliteArchive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					t.Fatalf("failed to initialize state %s", t.Name())
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
				stateDb.EndBlock(1)
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
				stateDb.EndBlock(2)
				stateDb.EndEpoch(1)

				if err := stateDb.Close(); err != nil {
					t.Errorf("Cannot close state: %e", err)
				}

				execSubProcessTest(t, dir, config.name, archiveType, "TestStateDBRead")
			})
		}
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

	s := createState(t, *stateImpl, *stateDir, *archiveImpl)
	defer func() {
		_ = s.Close()
	}()

	stateDb := CreateStateDBUsing(s)

	if state := stateDb.Exist(address1); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
	}
	if state := stateDb.Exist(address2); state != true {
		t.Errorf("Unexpected value, val: %v != %v", state, true)
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
	if balance := as1.GetBalance(address1); balance.Cmp(big.NewInt(153)) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := as1.GetBalance(address2); balance.Cmp(big.NewInt(0)) != 0 {
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
	if balance := as2.GetBalance(address1); balance.Cmp(big.NewInt(153)) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", balance, 153)
	}
	if balance := as2.GetBalance(address2); balance.Cmp(big.NewInt(6789)) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", balance, 6789)
	}
	if nonce := as2.GetNonce(address1); nonce != 58 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 58)
	}
	if nonce := as2.GetNonce(address2); nonce != 91 {
		t.Errorf("Unexpected value, val: %v != %v", nonce, 91)
	}
	if code := as2.GetCode(address1); bytes.Compare(code, []byte{1, 2, 3}) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{1, 2, 3})
	}
	if code := as2.GetCode(address2); bytes.Compare(code, []byte{3, 2, 1}) != 0 {
		t.Errorf("Unexpected value, val: %v != %v", code, []byte{3, 2, 1})
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

func TestStateDBArchive(t *testing.T) {
	for _, config := range initStates() {
		for _, archiveType := range []ArchiveType{LevelDbArchive, SqliteArchive} {
			t.Run(fmt.Sprintf("%s-%s", config.name, archiveType), func(t *testing.T) {
				dir := t.TempDir()
				s, err := config.createStateWithArchive(dir, archiveType)
				if err != nil {
					t.Fatalf("failed to initialize state %s; %s", config.name, err)
				}
				defer s.Close()
				stateDb := CreateStateDBUsing(s)

				stateDb.AddBalance(address2, big.NewInt(22))

				bl := stateDb.StartBulkLoad()
				bl.CreateAccount(address1)
				bl.SetBalance(address1, big.NewInt(12))
				if err := bl.Close(); err != nil {
					t.Fatalf("failed to bulkload StateDB with archive; %s", err)
				}

				stateDb.BeginBlock()
				stateDb.AddBalance(address1, big.NewInt(22))
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
				if balance := state1.GetBalance(address1); balance.Cmp(big.NewInt(12)) != 0 {
					t.Errorf("invalid balance at block 1: %s", balance)
				}
				if balance := state2.GetBalance(address1); balance.Cmp(big.NewInt(34)) != 0 {
					t.Errorf("invalid balance at block 2: %s", balance)
				}
			})
		}
	}
}
