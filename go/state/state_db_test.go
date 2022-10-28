package state

import (
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

	// this will trigger no calls to the underlying state

	db.CreateAccount(address1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after it was created")
	}
	if db.HasSuicided(address1) {
		t.Errorf("New account is considered deleted")
	}
	db.Suicide(address1)
	if db.Exist(address1) {
		t.Errorf("Account still exists after suicide")
	}
	if !db.HasSuicided(address1) {
		t.Errorf("Destroyed account is still considered alive")
	}
}

func TestCarmenStateCreateAccountCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	mock.EXPECT().GetAccountState(gomock.Eq(address1)).Return(common.Unknown, nil)

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

func TestCarmenStateSuicideCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// this test will cause one call to the DB to check for the existence of the account
	mock.EXPECT().GetAccountState(gomock.Eq(address1)).Return(common.Exists, nil)

	if !db.Exist(address1) {
		t.Errorf("Account state is not loaded from underlying state")
	}

	snapshot := db.Snapshot()

	db.Suicide(address1)
	if db.Exist(address1) {
		t.Errorf("Account does still exist after deletion")
	}

	if !db.HasSuicided(address1) {
		t.Errorf("Account is not marked as suicided after suicide")
	}

	db.RevertToSnapshot(snapshot)
	if !db.Exist(address1) {
		t.Errorf("Account remains deleted after rollback")
	}
	if db.HasSuicided(address1) {
		t.Errorf("Account is still marked as suicided after rollback")
	}
}

func TestCarmenStateCreatedAccountsAreCommitedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is created at the end of the transaction.
	mock.EXPECT().CreateAccount(gomock.Eq(address1)).Return(nil)

	db.CreateAccount(address1)
	db.EndTransaction()
}

func TestCarmenStateCreatedAccountsAreForgottenAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The created account is only created once.
	mock.EXPECT().CreateAccount(gomock.Eq(address1)).Return(nil)

	db.CreateAccount(address1)
	db.EndTransaction()
	db.EndTransaction()
}

func TestCarmenStateCreatedAccountsAreDiscardedOnEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// -- nothing is supposed to happen on the mock --

	db.CreateAccount(address1)
	db.AbortTransaction()
}

func TestCarmenStateDeletedAccountsAreCommitedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().DeleteAccount(gomock.Eq(address1)).Return(nil)

	db.Suicide(address1)
	db.EndTransaction()
}

func TestCarmenStateDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// -- nothing is supposed to happen on the mock --

	db.Suicide(address1)
	db.AbortTransaction()
}

func TestCarmenStateCreatedAndDeletedAccountsAreDeletedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().DeleteAccount(gomock.Eq(address1)).Return(nil)

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.EndTransaction()
}

func TestCarmenStateCreatedAndDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// -- nothing is supposed to happen on the mock --

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.AbortTransaction()
}

func TestCarmenStateEmptyAccountsAreRecognized(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its balance and nonce set to zero.
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(common.Balance{}, nil)
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.Nonce{}, nil)

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
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(common.Balance{}, nil)
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.Nonce{}, nil)

	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.AddBalance(address1, big.NewInt(1))
	if db.Empty(address1) {
		t.Errorf("Account with balance != 0 is still considered empty")
	}
}

func TestCarmenStateSettingTheNonceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its balance and nonce set to zero.
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(common.Balance{}, nil)
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.Nonce{}, nil)

	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.SetNonce(address1, 1)
	if db.Empty(address1) {
		t.Errorf("Account with nonce != 0 is still considered empty")
	}
}

func TestCarmenStateGetBalanceReturnsFreshCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	want := big.NewInt(12)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

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
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

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
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

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
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

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

func TestCarmenStateBalanceIsWrittenToStateIfChangedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(12))
	mock.EXPECT().SetBalance(gomock.Eq(address1), gomock.Eq(balance)).Return(nil)

	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()

	// The second end-of-transaction should not trigger yet another update.
	db.EndTransaction()
}

func TestCarmenStateBalanceOnlyFinalValueIsWrittenAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(12))
	mock.EXPECT().SetBalance(gomock.Eq(address1), gomock.Eq(balance)).Return(nil)

	db.AddBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(3))
	db.EndTransaction()
}

func TestCarmenStateBalanceUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.SubBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(5))
	db.EndTransaction()
}

func TestCarmenStateBalanceIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(gomock.Eq(address1)).Return(balance, nil)

	db.AddBalance(address1, big.NewInt(10))
	db.AbortTransaction()
}

func TestCarmenStateNoncesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	var want uint64 = 12
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.ToNonce(want), nil)

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
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.ToNonce(want), nil)

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

	// Mock should never be consulted.

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

func TestCarmenStateNoncesCanBeSnapshottedAndReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is initially 10. This should only be fetched once.
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.ToNonce(10), nil)

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
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.ToNonce(10), nil)

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

func TestCarmenStateNoncesIsWrittenToStateIfChangedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The updated value is expected to be written to the state.
	mock.EXPECT().SetNonce(gomock.Eq(address1), gomock.Eq(common.ToNonce(10))).Return(nil)

	db.SetNonce(address1, 10)
	db.EndTransaction()

	// The second end-of-transaction should not trigger yet another update.
	db.EndTransaction()
}

func TestCarmenStateNoncesOnlyFinalValueIsWrittenAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only the last value is to be written to the state.
	mock.EXPECT().SetNonce(gomock.Eq(address1), gomock.Eq(common.ToNonce(12))).Return(nil)

	db.SetNonce(address1, 10)
	db.SetNonce(address1, 11)
	db.SetNonce(address1, 12)
	db.EndTransaction()
}

func TestCarmenStateNoncesUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Nonce is only read, never written.
	mock.EXPECT().GetNonce(gomock.Eq(address1)).Return(common.ToNonce(10), nil)

	value := db.GetNonce(address1)
	db.SetNonce(address1, value)
	db.EndTransaction()
}

func TestCarmenStateNoncesIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// No mock call is expected.

	db.SetNonce(address1, 10)
	db.AbortTransaction()
}

func TestCarmenStateValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val1, nil)

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
	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val1, nil)

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
	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val1, nil)

	db.GetCommittedState(address1, key1)
	db.GetCommittedState(address1, key1)
}

func TestCarmenStateCommittedValuesCanBeFetchedAfterValueBeingWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val1, nil)

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
	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val1, nil)

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

	mock.EXPECT().GetStorage(gomock.Eq(address1), gomock.Eq(key1)).Return(val0, nil)

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

func TestCarmenStateUpdatedValuesAreCommitedToStateAtEndTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val1))
	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key2), gomock.Eq(val2))

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.EndTransaction()
}

func TestCarmenStateRollbackedValuesAreNotCommited(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val1))

	db.SetState(address1, key1, val1)
	snapshot := db.Snapshot()
	db.SetState(address1, key2, val2)
	db.RevertToSnapshot(snapshot)
	db.EndTransaction()
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
}

func TestCarmenStateOnlyFinalValueIsCommitted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val3))

	db.SetState(address1, key1, val1)
	db.SetState(address1, key1, val2)
	db.SetState(address1, key1, val3)
	db.EndTransaction()
}

func TestCarmenStateCanBeUsedForMultipleTransactions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val1))
	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val2))
	mock.EXPECT().SetStorage(gomock.Eq(address1), gomock.Eq(key1), gomock.Eq(val3))

	db.SetState(address1, key1, val1)
	db.EndTransaction()
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val3)
	db.EndTransaction()
}

func testStateDbHashAfterModification(t *testing.T, mod func(s StateDB)) {
	ref_state, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to create reference state: %v", err)
	}
	ref := CreateStateDBUsing(ref_state)
	mod(ref)
	ref.EndTransaction()
	want := ref.GetHash()
	for i := 0; i < 10; i++ {
		for _, config := range initStates(t) {
			t.Run(fmt.Sprintf("%v/run=%d", config.name, i), func(t *testing.T) {
				state := CreateStateDBUsing(config.state)
				mod(state)
				state.EndTransaction()
				if got := state.GetHash(); want != got {
					t.Errorf("Invalid hash, wanted %v, got %v", want, got)
				}
			})
		}
	}
}

func TestStateHashIsDeterministicForEmptyState(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		// nothing
	})
}

func TestStateHashIsDeterministicForSingleUpdate(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.SetState(address1, key1, val1)
	})
}

func TestStateHashIsDeterministicForMultipleUpdate(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.SetState(address1, key1, val1)
		s.SetState(address2, key2, val2)
		s.SetState(address3, key3, val3)
	})
}

func TestStateHashIsDeterministicForMultipleAccountCreations(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestStateHashIsDeterministicForMultipleAccountModifications(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.Suicide(address2)
		s.Suicide(address1)
	})
}

func TestStateHashIsDeterministicForMultipleBalanceUpdates(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.AddBalance(address1, big.NewInt(12))
		s.AddBalance(address2, big.NewInt(14))
		s.AddBalance(address3, big.NewInt(16))
		s.SubBalance(address3, big.NewInt(8))
	})
}

func TestStateHashIsDeterministicForMultipleNonceUpdates(t *testing.T) {
	testStateDbHashAfterModification(t, func(s StateDB) {
		s.SetNonce(address1, 12)
		s.SetNonce(address2, 14)
		s.SetNonce(address3, 18)
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
