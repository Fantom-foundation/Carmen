package state

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}
	address4 = common.Address{0x04}

	key1 = common.Key{0x01}
	key2 = common.Key{0x02}
	key3 = common.Key{0x03}

	val0 = common.Value{0x00}
	val1 = common.Value{0x01}
	val2 = common.Value{0x02}
	val3 = common.Value{0x03}

	balance1 = common.Balance{0x01}
	balance2 = common.Balance{0x02}
	balance3 = common.Balance{0x03}

	nonce1 = common.Nonce{0x01}
	nonce2 = common.Nonce{0x02}
	nonce3 = common.Nonce{0x03}
)

func TestAccountLifeCycleState_CanBePrinted(t *testing.T) {
	tests := []struct {
		state accountLifeCycleState
		print string
	}{
		{kNonExisting, "NonExisting"},
		{kExists, "Exists"},
		{kSuicided, "Suicided"},
		{accountLifeCycleState(23), "?"},
	}

	for _, test := range tests {
		if want, got := test.print, test.state.String(); want != got {
			t.Errorf("unexpected print of state %v, wanted %v", got, want)
		}
	}
}

func TestAccountClearingState_CanBePrinted(t *testing.T) {
	tests := []struct {
		state accountClearingState
		print string
	}{
		{noClearing, "noClearing"},
		{pendingClearing, "pendingClearing"},
		{cleared, "cleared"},
		{clearedAndTainted, "clearedAndTainted"},
		{accountClearingState(23), "?"},
	}

	for _, test := range tests {
		if want, got := test.print, test.state.String(); want != got {
			t.Errorf("unexpected print of state %v, wanted %v", got, want)
		}
	}
}

func TestStateDB_ImplementsStateDbInterface(t *testing.T) {
	var db stateDB
	var _ StateDB = &db
}

func TestStateDB_CanBeCreatedWithDefaultCacheSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	CreateCustomStateDBUsing(mock /* request default= */, 0)
}

func TestStateDB_AccountsCanBeCreatedAndDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_CreateAccountSetsNonceCodeAndBalanceToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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
		t.Errorf("code not initialized to zero-length code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-length code")
	}
}

func TestStateDB_CreateAccountSetsStorageToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)

	if got := db.GetState(address1, key1); got != (common.Value{}) {
		t.Errorf("state not initialized with zero")
	}
}

func TestStateDB_RecreatingAnAccountSetsStorageToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_RecreatingAccountSetsNonceCodeAndBalanceToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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
		t.Errorf("code not initialized to zero-length code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-length code")
	}
}

func TestStateDB_RecreatingAccountResetsStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account is non-existing, it gets recreated.
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.Nonce{0, 0, 0, 0, 0, 0, 0, 1}}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_RecreatingAccountResetsStorageButRetainsNewState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists with some state.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)

	// At the end the account is recreated with the new state.
	mock.EXPECT().Apply(uint64(123), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(12)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
		Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: val2}},
	})

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

func TestStateDB_DestroyingRecreatedAccountIsNotResettingClearingState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account exists with some state.
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.Suicide(address1)
	if db.(*stateDB).clearedAccounts[address1] != pendingClearing {
		t.Errorf("destroyed account is not marked for clearing")
	}

	db.CreateAccount(address1)
	if db.(*stateDB).clearedAccounts[address1] != cleared {
		t.Errorf("recreated account was not cleared")
	}

	db.GetState(address1, key1) // should not reach the store (no expectation stated above)

	db.Suicide(address1)
	if db.(*stateDB).clearedAccounts[address1] != cleared {
		t.Errorf("destroyed recreated account is no longer considered cleared")
	}

	db.GetState(address1, key1) // should not reach the store (no expectation stated above)
}

func TestStateDB_StorageOfDestroyedAccountIsStillAccessibleTillEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists with some values inside.
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

func TestStateDB_StoreDataCacheIsResetAfterSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)
	zero := common.Value{}

	// Initially the account exists and has a slot value set.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	// During the processing the account is deleted.
	mock.EXPECT().Apply(uint64(1), common.Update{})
	mock.EXPECT().Apply(uint64(2), common.Update{
		DeletedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})
	mock.EXPECT().Apply(uint64(3), common.Update{})

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

func TestStateDB_RollbackToKnownCommittedStateProducesCorrectResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val2, nil)

	s := db.Snapshot()
	db.SetState(address1, key1, val1)

	if want, got := val2, db.GetCommittedState(address1, key1); want != got {
		t.Errorf("unexpected committed state, wanted %v, got %v", want, got)
	}

	if want, got := val1, db.GetState(address1, key1); want != got {
		t.Errorf("unexpected committed state, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(s)

	if want, got := val2, db.GetState(address1, key1); want != got {
		t.Errorf("unexpected committed state, wanted %v, got %v", want, got)
	}
}

func TestStateDB_ClearedAndTaintedAccountsAreTrackedCorrectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetState(address1, key1, val1)

	if want, got := noClearing, db.(*stateDB).clearedAccounts[address1]; want != got {
		t.Errorf("unexpected clearing state, wanted %v, got %v", want, got)
	}

	db.Suicide(address1)

	if want, got := pendingClearing, db.(*stateDB).clearedAccounts[address1]; want != got {
		t.Errorf("unexpected clearing state, wanted %v, got %v", want, got)
	}

	db.EndTransaction()

	if want, got := cleared, db.(*stateDB).clearedAccounts[address1]; want != got {
		t.Errorf("unexpected clearing state, wanted %v, got %v", want, got)
	}

	backup := db.Snapshot()
	db.SetState(address1, key1, val2)

	if want, got := clearedAndTainted, db.(*stateDB).clearedAccounts[address1]; want != got {
		t.Errorf("unexpected clearing state, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(backup)

	if want, got := cleared, db.(*stateDB).clearedAccounts[address1]; want != got {
		t.Errorf("unexpected clearing state, wanted %v, got %v", want, got)
	}
}

func TestStateDB_RevertToUnknownSnapshotIsDetected(t *testing.T) {
	for i := -2; i < 2; i++ {
		ctrl := gomock.NewController(t)
		mock := NewMockState(ctrl)
		mock.EXPECT().Check().AnyTimes()
		db := CreateStateDBUsing(mock)

		if err := db.Check(); err != nil {
			t.Errorf("initial StateDB is not error free")
		}

		backup := db.Snapshot()
		db.RevertToSnapshot(i)

		if i == backup {
			if err := db.Check(); err != nil {
				t.Errorf("revert to existing snapshot should be fine, got error %v", err)
			}
		} else {
			if err := db.Check(); err == nil {
				t.Errorf("Revert to unknown snapshot %d should fail", i)
			}
		}
	}
}

func TestStateDB_RollingBackSuicideRestoresBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	initialBalance, err := common.ToBalance(big.NewInt(5))
	if err != nil {
		t.Fatalf("failed to prepare initial balance: %v", err)
	}

	// Initially the account exists with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(initialBalance, nil)

	// In the transaction we delete and restore the account.
	db.BeginTransaction()

	// Initially the old balance should be present.
	if want, got := big.NewInt(5), db.GetBalance(address1); want.Cmp(got) != 0 {
		t.Errorf("unexpected balance, wanted %v, got %v", want, got)
	}

	snapshot := db.Snapshot()
	db.Suicide(address1)

	// after a suicide the balance should be empty
	if want, got := big.NewInt(0), db.GetBalance(address1); want.Cmp(got) != 0 {
		t.Errorf("unexpected balance, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(snapshot)

	// the rollback of the suicide should restore the balance
	if want, got := big.NewInt(5), db.GetBalance(address1); want.Cmp(got) != 0 {
		t.Errorf("unexpected balance, wanted %v, got %v", want, got)
	}
}

func TestStateDB_RollingBackSuicideRestoresValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account exists with a view stored values.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().GetStorage(address1, key2).Return(val2, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

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

func TestStateDB_DestroyingAndRecreatingAnAccountInTheSameTransactionCallsDeleteAndCreateAccountOnStateDb(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account exists with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Check().AnyTimes()

	// The account is to be re-created.
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_DoubleDestroyedAccountThatIsOnceRolledBackIsStillCleared(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially the account exists with a view stored values.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Check().AnyTimes()

	// The account is to be re-created.
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_RecreatingExistingAccountSetsNonceAndCodeToZeroAndPreservesBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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
		t.Errorf("code not initialized to zero-length code")
	}

	if got := db.GetCodeSize(address1); got != 0 {
		t.Errorf("code not initialized to zero-length code")
	}
}

func TestStateDB_CreateAccountCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_RollingBackAccountCreationRestoresStoredData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// in this test storage value of a pre-existing account is fetched into the
	// StateDB, then reset as part of a account creation call, and restored during
	// a rollback of the account creation.
	value0 := common.Value{0}
	value1 := common.Value{1}
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(value1, nil)

	snapshot := db.Snapshot()

	value := db.GetState(address1, key1)
	if value != value1 {
		t.Errorf("unexpected value in slot, wanted %v, got %v", value1, value)
	}

	db.CreateAccount(address1)

	value = db.GetState(address1, key1)
	if value != value0 {
		t.Errorf("unexpected value in slot after deletion, wanted %v, got %v", value0, value)
	}

	db.RevertToSnapshot(snapshot)

	value = db.GetState(address1, key1)
	if value != value1 {
		t.Errorf("unexpected value in slot after rollback, wanted %v, got %v", value1, value)
	}
}

func TestStateDB_SuicideIndicatesExistingAccountAsBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SetCodeShouldNotStopSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_RepeatedSuicide(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Check().AnyTimes()

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
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1, Balance: newBalance}},
		Nonces:          []common.NonceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
		Slots:           []common.SlotUpdate{{Account: address1, Key: key2, Value: val2}},
	})

	// The changes are applied to the state at the end of the block.
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_SuicideIndicatesUnknownAccountAsNotBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SuicideIndicatesDeletedAccountAsNotBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SuicideRemovesBalanceFromAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SuicideCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SuicideIsExecutedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		DeletedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	db.Suicide(address1)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_SuicideCanBeCanceledThroughRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// the nonce and code will be set at the end of the block since suicide is canceled.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		Nonces: []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(5)}},
		Codes:  []common.CodeUpdate{{Account: address1, Code: []byte{1, 2, 3}}},
	})

	db.SetNonce(address1, 5)
	db.SetCode(address1, []byte{1, 2, 3})

	snapshot := db.Snapshot()
	db.Suicide(address1)
	db.RevertToSnapshot(snapshot)

	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_CreatedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is created at the end of the transaction.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

	db.CreateAccount(address1)
	db.SetNonce(address1, 1) // the account must not be empty
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_CreatedAccountsAreForgottenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The created account is only created once, and nonces and code are initialized.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})
	mock.EXPECT().Apply(uint64(2), common.Update{})

	db.CreateAccount(address1)
	db.SetNonce(address1, 1)
	db.EndTransaction()
	db.EndBlock(1)
	db.EndBlock(2)
}

func TestStateDB_CreatedAccountsAreDiscardedOnEndOfAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Needs to check whether the account already existed before the creation.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})
	mock.EXPECT().Apply(uint64(2), common.Update{})

	db.CreateAccount(address1)
	db.AbortTransaction()
	db.EndBlock(1)
	db.EndBlock(2)
}

func TestStateDB_DeletedAccountsAreStoredAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		DeletedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(0)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_DeletedAccountsRetainCodeUntilEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

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

func TestStateDB_DeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestStateDB_CreatedAndDeletedAccountsAreDeletedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The new account is deleted at the end of the transaction.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_CreatedAndDeletedAccountsAreIgnoredAtAbortedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate a non-existing account.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.CreateAccount(address1)
	db.Suicide(address1)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestStateDB_EmptyAccountsAreRecognized(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_SettingTheBalanceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SettingTheBalanceCreatesAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	addedBalance := big.NewInt(5)
	balance, _ := common.ToBalance(addedBalance)

	// The account have not existed - must be created by AddBalance call.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1, Balance: balance}},
	})

	db.AddBalance(address1, addedBalance)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after adding balance")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_AddingZeroBalanceCreatesAccountThatIsImplicitlyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially, the account does not exist, and it is not created, since it remains empty.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.AddBalance(address1, big.NewInt(0))
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after adding balance")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_SubtractingZeroBalanceCreatesAccountThatIsImplicitlyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Initially, the account does not exist, and it is not created, since it remains empty.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.SubBalance(address1, big.NewInt(0))
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after subtracting balance")
	}

	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_AddingNegativeBalancesLeadsToBalanceReduction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	initialBalance, err := common.ToBalance(big.NewInt(5))
	if err != nil {
		t.Fatalf("failed to create small balance: %v", err)
	}

	// Initially the account exists and has a balance of 5 tokens.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(initialBalance, nil)

	balance := db.GetBalance(address1)
	if got, want := balance, big.NewInt(5); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}

	db.AddBalance(address1, big.NewInt(2))
	balance = db.GetBalance(address1)
	if got, want := balance, big.NewInt(7); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}

	db.AddBalance(address1, big.NewInt(-3))
	balance = db.GetBalance(address1)
	if got, want := balance, big.NewInt(4); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}
}

func TestStateDB_SubtractingNegativeBalancesLeadsToBalanceIncrease(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	initialBalance, err := common.ToBalance(big.NewInt(5))
	if err != nil {
		t.Fatalf("failed to create small balance: %v", err)
	}

	// Initially the account exists and has a balance of 5 tokens.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetBalance(address1).Return(initialBalance, nil)

	balance := db.GetBalance(address1)
	if got, want := balance, big.NewInt(5); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}

	db.SubBalance(address1, big.NewInt(2))
	balance = db.GetBalance(address1)
	if got, want := balance, big.NewInt(3); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}

	db.SubBalance(address1, big.NewInt(-3))
	balance = db.GetBalance(address1)
	if got, want := balance, big.NewInt(6); got.Cmp(want) != 0 {
		t.Errorf("Unexpected balance, wanted %v, got %v", got, want)
	}
}

func TestStateDB_ProducingANegativeBalanceCausesTheBlockToFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Check().AnyTimes()

	db.SubBalance(address1, big.NewInt(1))

	db.EndBlock(1)

	if err := db.Check(); err == nil {
		t.Errorf("expected end of block to fail, but no error was produced")
	}
}

func TestStateDB_IncreasingTheBalanceBeyondItsMaximumValueCausesTheBlockToFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Check().AnyTimes()

	tooHighBalance := new(big.Int).Lsh(big.NewInt(1), common.BalanceSize*8)
	db.AddBalance(address1, tooHighBalance)

	db.EndBlock(1)

	if err := db.Check(); err == nil {
		t.Errorf("expected end of block to fail, but no error was produced")
	}
}

func TestStateDB_SettingTheNonceMakesAccountNonEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its nonce and code set to zero.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_SettingTheNonceToZeroMakesAccountEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// An empty account must have its nonce and code set to zero.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).AnyTimes().Return(false, nil)
	mock.EXPECT().GetBalance(address1).Return(common.Balance{0}, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{0}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	if !db.Empty(address1) {
		t.Errorf("Empty account not recognized as such")
	}
	db.SetNonce(address1, 0)
	if !db.Empty(address1) {
		t.Errorf("Account with nonce == 0 should be considered empty")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_CreatesAccountOnNonceSetting(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account does not exist, is expected to be created automatically.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(1)}},
	})

	db.SetNonce(address1, 1)
	if !db.Exist(address1) {
		t.Errorf("Account does not exist after setting the nonce")
	}
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_GetBalanceReturnsFreshCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_BalancesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_BalancesAreOnlyReadOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_BalancesCanBeSnapshottedAndReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_BalanceIsWrittenToStateIfChangedAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)
	balance, _ = common.ToBalance(big.NewInt(12))
	mock.EXPECT().Apply(uint64(1), common.Update{
		Balances: []common.BalanceUpdate{{Account: address1, Balance: balance}},
	})
	mock.EXPECT().Apply(uint64(2), common.Update{})

	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock(1)

	// The second end-of-block should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock(2)
}

func TestStateDB_BalanceOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Only the last value is to be written to the state.
	// The balance is expected to be read and the updated value to be written to the state.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	balance, _ = common.ToBalance(big.NewInt(14))
	mock.EXPECT().Apply(uint64(1), common.Update{
		Balances: []common.BalanceUpdate{{Account: address1, Balance: balance}},
	})
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.AddBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(3))
	db.EndTransaction()
	db.AddBalance(address1, big.NewInt(2))
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_BalanceUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(2), common.Update{})

	db.AddBalance(address1, big.NewInt(10))
	db.SubBalance(address1, big.NewInt(5))
	db.SubBalance(address1, big.NewInt(5))
	db.EndTransaction()
	db.EndBlock(2)
}

func TestStateDB_BalanceIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Balance is only read, never written.
	want := big.NewInt(10)
	balance, _ := common.ToBalance(want)
	mock.EXPECT().GetBalance(address1).Return(balance, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.AddBalance(address1, big.NewInt(10))
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestStateDB_NoncesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	var want uint64 = 12
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(want), nil)

	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestStateDB_NoncesAreOnlyReadOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_NoncesCanBeWrittenAndReadWithoutStateAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_NoncesOfANonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The nonce is fetched, and its default is zero.
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(0), nil)

	var want uint64 = 0
	if got := db.GetNonce(address1); got != want {
		t.Errorf("error retrieving nonce, wanted %v, got %v", want, got)
	}
}

func TestStateDB_NonceOfADeletedAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The side-effects of the creation of the account in the first transactions are expected.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(12)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_NonceOfADeletedAccountGetsResetAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_NoncesCanBeSnapshottedAndReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_NoncesOnlySetCanBeReverted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_NoncesIsWrittenToStateIfChangedAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// The updated value is expected to be written to the state.
	mock.EXPECT().Apply(uint64(1), common.Update{
		Nonces: []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(10)}},
	})
	mock.EXPECT().Apply(uint64(2), common.Update{})
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetNonce(address1, 10)
	db.EndTransaction()
	db.EndBlock(1)

	// The second end-of-transaction should not trigger yet another update.
	db.EndTransaction()
	db.EndBlock(2)
}

func TestStateDB_NoncesOnlyFinalValueIsWrittenAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Only the last value is to be written to the state.
	mock.EXPECT().Apply(uint64(1), common.Update{
		Nonces: []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(12)}},
	})
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetNonce(address1, 10)
	db.SetNonce(address1, 11)
	db.EndTransaction()
	db.SetNonce(address1, 12)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_NoncesUnchangedValuesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Nonce is only read, never written.
	mock.EXPECT().Apply(uint64(1), common.Update{})
	mock.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)
	// SetNonce create the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	value := db.GetNonce(address1)
	db.SetNonce(address1, value)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_NoncesIsNotWrittenToStateIfTransactionIsAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetNonce create the account if it does not exist
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.SetNonce(address1, 10)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestStateDB_ValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestStateDB_CommittedValuesAreReadFromState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	if got := db.GetCommittedState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestStateDB_CommittedValuesAreOnlyFetchedOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Set up the expectation that the store will be called once.
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)

	db.GetCommittedState(address1, key1)
	db.GetCommittedState(address1, key1)
}

func TestStateDB_CommittedValuesCanBeFetchedAfterValueBeingWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_SettingValuesCreatesAccountsImplicitly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)

	db.SetState(address1, key1, val1)
	if !db.Exist(address1) {
		t.Errorf("no implicit account creation by SetState")
	}
}

func TestStateDB_ImplicitAccountCreatedBySetStateIsDroppedSinceEmptyIfNothingElseIsSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The account is not created at the end of the transaction, nor is the value set.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().GetNonce(address1).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.SetState(address1, key1, val1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_EmptyAccountsDeletedAtEndOfTransactionsAreCleaned(t *testing.T) {
	// This issue was discovered using Aida Stochastic fuzzing. State information
	// was not properly cleaned at the end of consecutive transactions writing
	// storage values into empty accounts.
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)
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

func TestStateDB_FetchedCommittedValueIsNotResetInRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The committed state is only read ones
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

func TestStateDB_WrittenValuesCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}
}

func TestStateDB_WrittenValuesCanBeUpdated(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)

	db.SetState(address1, key1, val1)
	if got := db.GetState(address1, key1); got != val1 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val1, got)
	}

	db.SetState(address1, key1, val2)
	if got := db.GetState(address1, key1); got != val2 {
		t.Errorf("error retrieving state value, wanted %v, got %v", val2, got)
	}
}

func TestStateDB_WrittenValuesCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_UpdatedValuesAreCommittedToStateAtEndBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), sameEffectAs{common.Update{
		Slots: []common.SlotUpdate{
			{Account: address1, Key: key1, Value: val1},
			{Account: address1, Key: key2, Value: val2},
		},
	}})

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_RevertedValuesAreNotCommitted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
	})

	db.SetState(address1, key1, val1)
	snapshot := db.Snapshot()
	db.SetState(address1, key2, val2)
	db.RevertToSnapshot(snapshot)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_NothingIsCommittedOnTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Should test whether the account exists, nothing else.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.SetState(address1, key1, val1)
	db.SetState(address1, key2, val2)
	db.AbortTransaction()
	db.EndBlock(1)
}

func TestStateDB_OnlyFinalValueIsStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val3}},
	})

	db.SetState(address1, key1, val1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val3)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_UndoneValueUpdateIsNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Only expect a read but no update.
	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetStorage(address1, key1).Return(val1, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	val := db.GetState(address1, key1)
	db.SetState(address1, key1, val2)
	db.EndTransaction()
	db.SetState(address1, key1, val)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_ValueIsCommittedAtEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_CanBeUsedForMultipleBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Times(3).Return(true, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{
		Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
	})
	mock.EXPECT().Apply(uint64(2), common.Update{
		Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val2}},
	})
	mock.EXPECT().Apply(uint64(3), common.Update{
		Slots: []common.SlotUpdate{{Account: address1, Key: key1, Value: val3}},
	})

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

func TestStateDB_CodesCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodesCanBeSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(false, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCode(address1); !bytes.Equal(got, want) {
		t.Errorf("error retrieving code, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeUpdatesCoveredByRollbacks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_ReadCodesAreNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().GetCode(address1).Return(want, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

	db.GetCode(address1)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_UpdatedCodesAreStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().Apply(uint64(1), common.Update{
		Codes: []common.CodeUpdate{{Account: address1, Code: want}},
	})

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_UpdatedCodesAreStoredOnlyOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	mock.EXPECT().Apply(uint64(1), common.Update{
		Codes: []common.CodeUpdate{{Account: address1, Code: want}},
	})
	mock.EXPECT().Apply(uint64(2), common.Update{})
	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)

	// No store on second time
	db.EndTransaction()
	db.EndBlock(2)
}

func TestStateDB_SettingCodesCreatesAccountsImplicitly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(false, nil)
	want := []byte{0xAC, 0xDC}
	mock.EXPECT().Apply(uint64(1), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: want}},
	})

	db.SetCode(address1, want)
	db.EndTransaction()
	db.EndBlock(1)
}

func TestStateDB_CodeSizeCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 2
	mock.EXPECT().GetCodeSize(address1).Return(want, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeSizeCanBeReadAfterModification(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// SetCode creates the account if it does not exist
	mock.EXPECT().Exists(address1).Return(true, nil)

	want := []byte{0xAC, 0xDC}
	db.SetCode(address1, want)

	if got := db.GetCodeSize(address1); got != len(want) {
		t.Errorf("error retrieving code size, wanted %v, got %v", len(want), got)
	}
}

func TestStateDB_CodeSizeOfANonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := 0
	mock.EXPECT().GetCodeSize(address1).Return(0, nil)

	if got := db.GetCodeSize(address1); got != want {
		t.Errorf("error retrieving code size, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeSizeOfADeletedAccountIsZeroAfterEndOfTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_CodeHashOfNonExistingAccountIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// The state DB is asked for the accounts existence, but not for the hash.
	mock.EXPECT().Exists(address1).Return(false, nil)

	want := common.Hash{}
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeHashOfAnExistingAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// Simulate an existing account with empty code.
	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().GetCodeHash(address1).Return(common.GetKeccak256Hash([]byte{}), nil)

	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeHashOfNewlyCreatedAccountIsTheHashOfTheEmptyCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// At the start the account does not exist.
	mock.EXPECT().Exists(address1).Return(false, nil)

	db.CreateAccount(address1)
	want := common.GetKeccak256Hash([]byte{})
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestStateDB_CodeHashCanBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	want := common.Hash{0xAC, 0xDC}
	mock.EXPECT().GetCodeHash(address1).Return(want, nil)
	mock.EXPECT().Exists(address1).Return(true, nil)

	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestStateDB_SetCodeSizeCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
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

func TestStateDB_CodeHashCanBeReadAfterModification(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)

	code := []byte{0xAC, 0xDC}
	db.SetCode(address1, code)

	want := common.GetKeccak256Hash(code)
	if got := db.GetCodeHash(address1); got != want {
		t.Errorf("error retrieving code hash, wanted %v, got %v", want, got)
	}
}

func TestStateDB_InitialRefundIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	if got := db.GetRefund(); got != 0 {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
}

func TestStateDB_RefundCanBeModified(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_RefundBelowZeroIsAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	var want uint64 = 0
	if got := db.GetRefund(); got != want {
		t.Errorf("initial refund is not 0, got: %v", got)
	}
	db.AddRefund(12)
	want += 12
	if got := db.GetRefund(); got != want {
		t.Errorf("failed to update refund, wanted %v, got %v", want, got)
	}

	if err := db.Check(); err != nil {
		t.Fatalf("unexpected error while handling refunds: %v", err)
	}

	db.SubRefund(14)
	if err := db.Check(); err == nil {
		t.Fatalf("expected an error when reducing refunds below 0, got nothing")
	}
}

func TestStateDB_AddedRefundCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_RemovedRefundCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_RefundIsResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_RefundIsResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AccessedAddressesCanBeAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AccessedAddressesCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AccessedAddressesAreResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.EndTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at end of transaction")
	}
}

func TestStateDB_AccessedAddressesAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddAddressToAccessList(address1)
	db.AbortTransaction()
	if db.IsAddressInAccessList(address1) {
		t.Errorf("Accessed addresses not cleared at abort of transaction")
	}
}

func TestStateDB_AccessedSlotsCanBeAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AddingSlotToAccessListAddsAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AccessedSlotsCanBeRolledBack(t *testing.T) {
	ctrl := gomock.NewController(t)
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

func TestStateDB_AccessedSlotsAreResetAtTransactionEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.EndTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at end of transaction")
	}
}

func TestStateDB_AccessedAddressedAreResetAtTransactionAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	db.AddSlotToAccessList(address1, key1)
	db.AbortTransaction()
	if a, b := db.IsSlotInAccessList(address1, key1); a || b {
		t.Errorf("Accessed slot not cleared at abort of transaction")
	}
}

// EIP-161: At the end of the transaction, any account touched by the execution of that transaction
// which is now empty SHALL instead become non-existent (i.e. deleted).

func TestStateDB_DeletesEmptyAccountsEip161(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	b12, err := common.ToBalance(big.NewInt(12))
	if err != nil {
		t.Fatalf("failed to set up test case: %v", err)
	}

	mock.EXPECT().Check().AnyTimes()

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
		t.Errorf("Empty account have not been deleted at the end of the transaction")
	}
	db.EndTransaction()

	// The account is deleted in the state at the end of the block
	mock.EXPECT().Apply(uint64(1), common.Update{
		DeletedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})
	db.EndBlock(1)
}

func TestStateDB_NeverCreatesEmptyAccountsEip161(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Exists(address1).Return(false, nil)
	mock.EXPECT().Exists(address2).Return(false, nil)
	mock.EXPECT().Exists(address3).Return(false, nil)
	mock.EXPECT().GetNonce(address2).Return(common.Nonce{}, nil)
	mock.EXPECT().GetNonce(address3).Return(common.Nonce{}, nil)
	mock.EXPECT().GetCodeSize(address2).Return(0, nil)
	mock.EXPECT().Apply(uint64(1), common.Update{})

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

func TestStateDB_SuicidedAccountNotRecreatedBySettingBalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()

	// Simulate a existing account.
	mock.EXPECT().Exists(address1).Return(true, nil)
	// The account will be deleted.
	mock.EXPECT().Apply(uint64(1), common.Update{
		DeletedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1}},
		Nonces:          []common.NonceUpdate{{Account: address1}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: []byte{}}},
	})

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

func TestStateDB_StateDBCanNotEndABlockIfCommitIsNotAllowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := createStateDBWith(mock, 1024, false /*canApplyChanges*/)

	mock.EXPECT().Check().AnyTimes()

	if err := db.Check(); err != nil {
		t.Errorf("unexpected error in fresh instance: %v", err)
	}

	db.EndBlock(1)

	if err := db.Check(); err == nil {
		t.Errorf("expected error after attempt, got %v", err)
	}
}

func TestStateDB_Copy(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateNonCommittableStateDBUsing(mock)

	mock.EXPECT().Exists(gomock.Any()).Return(true, nil).AnyTimes()
	mock.EXPECT().GetBalance(address1).Return(common.Balance{}, nil)

	db.BeginTransaction()
	db.AddBalance(address1, big.NewInt(2))
	db.SetNonce(address1, 8)
	db.SetCode(address1, []byte{0x12})
	db.SetState(address2, key3, val1)
	db.EndTransaction()

	cp := db.Copy()
	// state introduced by the previous tx should be readable from the copy
	if cp.GetBalance(address1).Cmp(big.NewInt(2)) != 0 {
		t.Errorf("balance not copied")
	}
	if cp.GetNonce(address1) != 8 {
		t.Errorf("nonce not copied")
	}
	if !bytes.Equal(cp.GetCode(address1), []byte{0x12}) {
		t.Errorf("code not copied")
	}
	if cp.GetState(address2, key3) != val1 {
		t.Errorf("storage not copied")
	}
}

func TestStateDB_LogsCanBeAddedAndRetrieved(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	log1 := &common.Log{Address: address1}
	log2 := &common.Log{Address: address2}
	log3 := &common.Log{Address: address3}

	var want []*common.Log
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.AddLog(log1)
	want = append(want, log1)
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.AddLog(log2)
	want = append(want, log2)
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.AddLog(log3)
	want = append(want, log3)
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}
}

func TestStateDB_LogsAreResetAtEndOfBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Check().AnyTimes()
	mock.EXPECT().Apply(uint64(0), common.Update{})

	log1 := &common.Log{Address: address1}
	log2 := &common.Log{Address: address2}
	log3 := &common.Log{Address: address3}

	db.AddLog(log1)
	db.AddLog(log2)
	db.AddLog(log3)

	want := []*common.Log{log1, log2, log3}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.EndBlock(0)

	want = []*common.Log{}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}
}

func TestStateDB_LogsAreCoveredByRollbacks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	log1 := &common.Log{Address: address1}
	log2 := &common.Log{Address: address2}
	log3 := &common.Log{Address: address3}

	s1 := db.Snapshot()
	db.AddLog(log1)
	s2 := db.Snapshot()
	db.AddLog(log2)
	s3 := db.Snapshot()
	db.AddLog(log3)

	want := []*common.Log{log1, log2, log3}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(s3)
	want = []*common.Log{log1, log2}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(s2)
	want = []*common.Log{log1}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}

	db.RevertToSnapshot(s1)
	want = []*common.Log{}
	if got := db.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Errorf("reported invalid log list, wanted %v, got %v", want, got)
	}
}

func TestStateDB_BeginAndEndEpochsHaveNoEffect(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	// All this test does is making sure that those calls do not trigger
	// any operation on the underlying state.
	db.BeginEpoch()
	db.EndEpoch(1)
}

func TestStateDB_GetHashObtainsHashFromUnderlyingState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	hash := common.Hash{1, 2, 3}
	mock.EXPECT().GetHash().Return(hash, nil)

	if want, got := hash, db.GetHash(); want != got {
		t.Errorf("unexpected hash, wanted %d, got %d", want, got)
	}
}

func TestStateDB_CollectsErrorsAndReportsThemDuringACheck(t *testing.T) {
	injectedError := fmt.Errorf("injected error")
	tests := map[string]struct {
		setExpectations func(state *MockState)
		applyOperation  func(db StateDB)
	}{
		"exits": {
			setExpectations: func(state *MockState) {
				state.EXPECT().Exists(gomock.Any()).Return(false, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.Exist(address1)
			},
		},
		"balance": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetBalance(gomock.Any()).Return(common.Balance{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetBalance(address1)
			},
		},
		"nonce": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetNonce(gomock.Any()).Return(common.Nonce{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetNonce(address1)
			},
		},
		"code": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetCode(gomock.Any()).Return([]byte{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetCode(address1)
			},
		},
		"code-hash": {
			setExpectations: func(state *MockState) {
				state.EXPECT().Exists(address1).Return(true, nil)
				state.EXPECT().GetCodeHash(gomock.Any()).Return(common.Hash{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetCodeHash(address1)
			},
		},
		"code-size": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetCodeSize(gomock.Any()).Return(0, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetCodeSize(address1)
			},
		},
		"storage": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetStorage(gomock.Any(), gomock.Any()).Return(common.Value{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetState(address1, key1)
			},
		},
		"apply": {
			setExpectations: func(state *MockState) {
				state.EXPECT().Exists(address1).Return(true, nil)
				state.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(injectedError)
			},
			applyOperation: func(db StateDB) {
				db.SetNonce(address1, 12)
				db.EndBlock(2)
			},
		},
		"get-hash": {
			setExpectations: func(state *MockState) {
				state.EXPECT().GetHash().Return(common.Hash{}, injectedError)
			},
			applyOperation: func(db StateDB) {
				db.GetHash()
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			state := NewMockState(ctrl)
			state.EXPECT().Check().AnyTimes()
			db := CreateStateDBUsing(state)

			if err := db.Check(); err != nil {
				t.Errorf("unexpected error at begin of test: %v", err)
			}

			test.setExpectations(state)
			test.applyOperation(db)
			if err := db.Check(); !errors.Is(err, injectedError) {
				t.Errorf("Failed to capture DB error, wanted %v, got %v", injectedError, err)
			}
		})
	}
}

func TestStateDB_CanCollectMoreThanOneError(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().Check().AnyTimes()

	issue1 := fmt.Errorf("injected issue 1")
	issue2 := fmt.Errorf("injected issue 2")
	issue3 := fmt.Errorf("injected issue 3")
	state.EXPECT().GetNonce(address1).Return(common.Nonce{}, issue1)
	state.EXPECT().GetNonce(address2).Return(common.Nonce{}, issue2)
	state.EXPECT().GetNonce(address3).Return(common.Nonce{}, issue3)

	db.GetNonce(address1)
	db.GetNonce(address2)
	db.GetNonce(address3)

	err := db.Check()
	if !errors.Is(err, issue1) {
		t.Errorf("failed to record issue %v", issue1)
	}
	if !errors.Is(err, issue2) {
		t.Errorf("failed to record issue %v", issue2)
	}
	if !errors.Is(err, issue3) {
		t.Errorf("failed to record issue %v", issue3)
	}
}

func TestStateDB_NoApplyWhenErrorsHaveBeenEncountered(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().Check().AnyTimes()

	issue := fmt.Errorf("injected issue")
	state.EXPECT().GetNonce(address1).Return(common.Nonce{1}, nil)
	state.EXPECT().GetNonce(address2).Return(common.Nonce{}, issue)
	state.EXPECT().Apply(uint64(1), gomock.Any()).Return(nil)

	db.GetNonce(address1)
	db.EndBlock(1)

	db.GetNonce(address2)
	db.EndBlock(2)
}

func TestStateDB_ErrorsAreReportedDuringFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().Check().AnyTimes()

	issueA := fmt.Errorf("injected issue A")
	issueB := fmt.Errorf("injected issue B")
	state.EXPECT().GetNonce(address1).Return(common.Nonce{}, issueA)
	state.EXPECT().Flush().Return(issueB)

	db.GetNonce(address1)

	err := db.Flush()
	if !errors.Is(err, issueA) {
		t.Errorf("collected issue not reported by Flush()")
	}
	if !errors.Is(err, issueB) {
		t.Errorf("flush issue not reported by Flush()")
	}
}

func TestStateDB_ErrorsAreReportedDuringClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().Check().AnyTimes()

	issueA := fmt.Errorf("injected issue A")
	issueB := fmt.Errorf("injected issue B")
	issueC := fmt.Errorf("injected issue C")
	state.EXPECT().GetNonce(address1).Return(common.Nonce{}, issueA)
	state.EXPECT().Flush().Return(issueB)
	state.EXPECT().Close().Return(issueC)

	db.GetNonce(address1)

	err := db.Close()
	if !errors.Is(err, issueA) {
		t.Errorf("collected issue not reported by Close()")
	}
	if !errors.Is(err, issueB) {
		t.Errorf("flush issue not reported by Close()")
	}
	if !errors.Is(err, issueC) {
		t.Errorf("close issue not reported by Close()")
	}
}

func TestStateDB_GetArchiveStateDbCanProduceArchiveAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	archive := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().GetArchiveState(uint64(12)).Return(archive, nil)
	archive.EXPECT().GetNonce(address1).Return(common.ToNonce(10), nil)

	history, err := db.GetArchiveStateDB(12)
	if err != nil {
		t.Fatalf("Unexpected error during archive lookup: %v", err)
	}

	if want, got := uint64(10), history.GetNonce(address1); want != got {
		t.Errorf("invalid nonce, wanted %v, got %v", want, got)
	}
}

func TestStateDB_GetArchiveStateDbProducesDistinctStateDbInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	archive := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().GetArchiveState(gomock.Any()).AnyTimes().Return(archive, nil)

	history, err := db.GetArchiveStateDB(12)
	if err != nil {
		t.Fatalf("Unexpected error during archive lookup: %v", err)
	}
	dbA := history.(*nonCommittableStateDB).stateDB

	history, err = db.GetArchiveStateDB(14)
	if err != nil {
		t.Fatalf("Unexpected error during archive lookup: %v", err)
	}
	dbB := history.(*nonCommittableStateDB).stateDB

	if dbA == dbB {
		t.Errorf("StateDB instance in archives are not distinct, got %v and %v", dbA, dbB)
	}
}

func TestStateDB_GetArchiveStateDbRecyclesNonCommittableStateDbInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)
	archive := NewMockState(ctrl)
	db := CreateStateDBUsing(state)

	state.EXPECT().GetArchiveState(gomock.Any()).AnyTimes().Return(archive, nil)

	// This test aims to verify that a released StateDB is recycled.
	reuseSeen := false
	seen := map[*stateDB]struct{}{}
	for i := 0; i < 10; i++ {
		history, err := db.GetArchiveStateDB(12)
		if err != nil {
			t.Fatalf("Unexpected error during archive lookup: %v", err)
		}

		// Test whether this state DB is a version seen before.
		db := history.(*nonCommittableStateDB).stateDB
		if _, found := seen[db]; found {
			reuseSeen = true
			break
		}
		seen[db] = struct{}{}
		history.Release()
	}

	if !reuseSeen {
		t.Errorf("no reuse detected")
	}
}

func TestStateDB_NonCommittableStateDbCanBeReleasedMultipleTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	state := CreateStateDBUsing(mock)

	db := nonCommittableStateDB{state.(*stateDB)}
	db.Release()
	if db.stateDB != nil {
		t.Errorf("state DB was not released")
	}
	db.Release()
}

func TestStateDB_GetArchiveStateDbFailsIfThereTheArchiveAccessFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	injectedError := fmt.Errorf("injected error")
	mock.EXPECT().GetArchiveState(uint64(12)).Return(nil, injectedError)

	if _, err := db.GetArchiveStateDB(12); !errors.Is(err, injectedError) {
		t.Errorf("retrieving the archive state should have failed, wanted %v, got %v", injectedError, err)
	}
}

func TestStateDB_GetArchiveBlockHeightReturnsHeightOfArchive(t *testing.T) {
	tests := map[string]struct {
		height uint64
		empty  bool
		err    error
	}{
		"empty":     {0, true, nil},
		"non-empty": {12, false, nil},
		"missing":   {0, false, NoArchiveError},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			state := NewMockState(ctrl)
			db := CreateStateDBUsing(state)

			state.EXPECT().GetArchiveBlockHeight().Return(test.height, test.empty, test.err)

			height, empty, err := db.GetArchiveBlockHeight()
			if want, got := test.height, height; want != got {
				t.Errorf("unexpected height, wanted %v, got %v", want, got)
			}
			if want, got := test.empty, empty; want != got {
				t.Errorf("unexpected empty flag, wanted %v, got %v", want, got)
			}
			if want, got := test.err, err; want != got {
				t.Errorf("unexpected error, wanted %v, got %v", want, got)
			}

		})
	}
}

func TestStateDB_ProvidesTransactionChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(gomock.Any()).Return(true, nil).AnyTimes()
	mock.EXPECT().GetBalance(gomock.Any()).Return(balance1, nil)

	db.BeginTransaction()
	db.AddBalance(address1, big.NewInt(1))
	db.SetNonce(address2, 2)
	db.SetCode(address3, []byte{0x12})
	db.SetState(address4, key1, val1)
	changes := db.GetTransactionChanges()

	for _, addr := range []common.Address{address1, address2, address3, address4} {
		_, exists := changes[addr]
		if !exists {
			t.Errorf("account %x missing in the set of changed accounts", addr)
		}
	}
	if changes[address4][0] != key1 {
		t.Errorf("slot %x missing in the set of changed slots", key1)
	}
}

func TestStateDB_BulkLoadReachesState(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	balance, _ := common.ToBalance(big.NewInt(12))
	code := []byte{1, 2, 3}

	mock.EXPECT().Apply(uint64(0), common.Update{
		CreatedAccounts: []common.Address{address1},
		Balances:        []common.BalanceUpdate{{Account: address1, Balance: balance}},
		Nonces:          []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(14)}},
		Codes:           []common.CodeUpdate{{Account: address1, Code: code}},
		Slots:           []common.SlotUpdate{{Account: address1, Key: key1, Value: val1}},
	})
	mock.EXPECT().Flush().Return(nil)
	mock.EXPECT().GetHash().Return(common.Hash{}, nil)

	load := db.StartBulkLoad(0)
	load.CreateAccount(address1)
	load.SetBalance(address1, big.NewInt(12))
	load.SetNonce(address1, 14)
	load.SetState(address1, key1, val1)
	load.SetCode(address1, code)

	load.Close()
}

func TestStateDB_BulkLoadSetBalanceFailsForInvalidBalances(t *testing.T) {
	tests := map[string]*big.Int{
		"negative": big.NewInt(-1),
		"toBig":    big.NewInt(0).Lsh(big.NewInt(1), 256),
	}

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			bulk := bulkLoad{}
			if len(bulk.errs) != 0 {
				t.Fatalf("initial bulk load instance is not error free")
			}
			bulk.SetBalance(address1, value)
			if len(bulk.errs) == 0 {
				t.Errorf("balance issue not detected")
			}
		})
	}
}

func TestStateDB_BulkLoadApplyDetectsInconsistencies(t *testing.T) {
	bulk := bulkLoad{}
	if len(bulk.errs) != 0 {
		t.Fatalf("initial bulk load instance is not error free")
	}
	bulk.SetNonce(address1, 12)
	bulk.SetNonce(address1, 14)
	if len(bulk.errs) != 0 {
		t.Fatalf("unexpected error while writing data to bulk load instance: %v", errors.Join(bulk.errs...))
	}
	bulk.apply()
	if len(bulk.errs) == 0 {
		t.Errorf("inconsistent update issue not detected")
	}
}

func TestStateDB_BulkLoadApplyForwardsUpdateIssues(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)

	injectedError := fmt.Errorf("injected error")
	state.EXPECT().Apply(uint64(12), common.Update{
		Nonces: []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(14)}},
	}).Return(injectedError)

	bulk := bulkLoad{
		block: 12,
		db:    createStateDBWith(state, 0, true),
	}
	if len(bulk.errs) != 0 {
		t.Fatalf("initial bulk load instance is not error free")
	}
	bulk.SetNonce(address1, 14)
	if len(bulk.errs) != 0 {
		t.Fatalf("unexpected error while writing data to bulk load instance: %v", errors.Join(bulk.errs...))
	}
	bulk.apply()
	got := errors.Join(bulk.errs...)
	if !errors.Is(got, injectedError) {
		t.Errorf("missing expected error, wanted %v, got %v", injectedError, got)
	}
}

func TestStateDB_BulkLoadCloseReportsApplyIssues(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)

	injectedError := fmt.Errorf("injected error")
	state.EXPECT().Apply(uint64(12), common.Update{
		Nonces: []common.NonceUpdate{{Account: address1, Nonce: common.ToNonce(14)}},
	}).Return(injectedError)

	bulk := bulkLoad{
		block: 12,
		db:    createStateDBWith(state, 0, true),
	}
	if len(bulk.errs) != 0 {
		t.Fatalf("initial bulk load instance is not error free")
	}
	bulk.SetNonce(address1, 14)
	if len(bulk.errs) != 0 {
		t.Fatalf("unexpected error while writing data to bulk load instance: %v", errors.Join(bulk.errs...))
	}
	got := bulk.Close()
	if !errors.Is(got, injectedError) {
		t.Errorf("missing expected error, wanted %v, got %v", injectedError, got)
	}
}

func TestStateDB_BulkLoadCloseReportsFlushIssues(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)

	injectedError := fmt.Errorf("injected error")
	state.EXPECT().Apply(uint64(12), common.Update{}).Return(nil)
	state.EXPECT().Flush().Return(injectedError)

	bulk := bulkLoad{
		block: 12,
		db:    createStateDBWith(state, 0, true),
	}
	if len(bulk.errs) != 0 {
		t.Fatalf("initial bulk load instance is not error free")
	}
	got := bulk.Close()
	if !errors.Is(got, injectedError) {
		t.Errorf("missing expected error, wanted %v, got %v", injectedError, got)
	}
}

func TestStateDB_BulkLoadCloseReportsHashingIssues(t *testing.T) {
	ctrl := gomock.NewController(t)
	state := NewMockState(ctrl)

	injectedError := fmt.Errorf("injected error")
	state.EXPECT().Apply(uint64(12), common.Update{}).Return(nil)
	state.EXPECT().Flush().Return(nil)
	state.EXPECT().GetHash().Return(common.Hash{}, injectedError)

	bulk := bulkLoad{
		block: 12,
		db:    createStateDBWith(state, 0, true),
	}
	if len(bulk.errs) != 0 {
		t.Fatalf("initial bulk load instance is not error free")
	}
	got := bulk.Close()
	if !errors.Is(got, injectedError) {
		t.Errorf("missing expected error, wanted %v, got %v", injectedError, got)
	}
}

func TestStateDB_ThereCanBeMultipleBulkLoadPhases(t *testing.T) {
	const N = 10

	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Apply(gomock.Any(), gomock.Any()).AnyTimes()
	mock.EXPECT().Flush().Times(N).Return(nil)
	mock.EXPECT().GetHash().Times(N).Return(common.Hash{}, nil)

	for i := 0; i < N; i++ {
		load := db.StartBulkLoad(uint64(i))
		load.CreateAccount(address1)
		load.SetNonce(address1, uint64(i))
		if err := load.Close(); err != nil {
			t.Errorf("bulk-insert failed: %v", err)
		}
	}
}

func TestStateDB_GetMemoryFootprintIsReturnedAndNotZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := CreateStateDBUsing(mock)

	mock.EXPECT().Exists(address1).Return(true, nil)
	mock.EXPECT().Exists(address2).Return(false, nil)
	mock.EXPECT().GetBalance(address1).Return(common.Balance{10}, nil)
	mock.EXPECT().GetCode(address1).Return([]byte{1, 2, 3}, nil)
	mock.EXPECT().GetCodeHash(address1).Return(common.Hash{3, 2, 1}, nil)
	mock.EXPECT().GetMemoryFootprint().Return(common.NewMemoryFootprint(12))

	// Make sure that there is some data in the caches.
	db.AddBalance(address1, big.NewInt(12))
	db.SetNonce(address1, 12)
	db.SetNonce(address2, 0)
	db.GetCode(address1)
	db.GetCodeHash(address1)
	db.SetState(address1, key2, val3)
	db.AddSlotToAccessList(address1, key2)

	fp := db.GetMemoryFootprint()
	if fp == nil || fp.Total() == 0 {
		t.Errorf("invalid memory footprint: %v", fp)
	}

	components := []struct {
		name       string
		mayBeEmpty bool
	}{
		{"state", false},
		{"accounts", false},
		{"balances", false},
		{"nonces", false},
		{"codes", false},
		{"slots", false},
		{"accessedAddresses", false},
		{"accessedSlots", false},
		{"writtenSlots", false},
		{"storedDataCache", false},
		{"reincarnation", true},
		{"emptyCandidates", false},
	}

	for _, component := range components {
		child := fp.GetChild(component.name)
		if child == nil {
			t.Errorf("missing component %s", component.name)
		} else if !component.mayBeEmpty && child.Total() == 0 {
			t.Errorf("empty component %s", component.name)
		}
	}

}

func TestBulkLoad_CloseResetsLocalCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := createStateDBWith(mock, 0, true)

	gomock.InOrder(
		mock.EXPECT().Exists(address1),
		mock.EXPECT().Exists(address2),
		mock.EXPECT().Exists(address3),
		mock.EXPECT().Apply(uint64(1), gomock.Any()),
		mock.EXPECT().Flush(),
		mock.EXPECT().GetHash().Return(common.Hash{}, nil),
	)

	// fill the db with some accounts
	db.AddBalance(address1, big.NewInt(100))
	db.SetNonce(address1, uint64(1))
	db.SetCode(address1, []byte{1})

	db.AddBalance(address2, big.NewInt(200))
	db.SetNonce(address2, uint64(2))
	db.SetCode(address2, []byte{2})

	db.AddBalance(address3, big.NewInt(300))
	db.SetNonce(address3, uint64(3))
	db.SetCode(address3, []byte{3})

	// Local cache should not be empty after filling the database
	if len(db.accounts) == 0 {
		t.Error("local accounts cache must not be empty")
	}
	if len(db.balances) == 0 {
		t.Error("local balances cache must not be empty")
	}
	if len(db.nonces) == 0 {
		t.Error("local nonces cache must not be empty")
	}
	if len(db.codes) == 0 {
		t.Error("local codes cache must not be empty")
	}

	bl := db.StartBulkLoad(1)
	err := bl.Close()
	if err != nil {
		t.Errorf("failed to close bulk-load; %v", err)
	}

	// Local cache must be empty
	if len(db.accounts) != 0 {
		t.Error("local accounts cache must be empty")
	}
	if len(db.balances) != 0 {
		t.Error("local balances cache must be empty")
	}
	if len(db.nonces) != 0 {
		t.Error("local nonces cache must be empty")
	}
	if len(db.codes) != 0 {
		t.Error("local codes cache must be empty")
	}
}

func TestBulkload_AskingDbForDataAfterStartingBulkloadDoesNotCauseError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockState(ctrl)
	db := createStateDBWith(mock, 0, true)

	gomock.InOrder(
		mock.EXPECT().Exists(address2),
		mock.EXPECT().Exists(address1),
		mock.EXPECT().Apply(uint64(1), gomock.Any()),
		mock.EXPECT().Flush(),
		mock.EXPECT().GetHash().Return(common.Hash{}, nil),
		mock.EXPECT().Exists(address2),
		mock.EXPECT().Apply(uint64(1), gomock.Any()),
		mock.EXPECT().Flush(),
		mock.EXPECT().GetHash().Return(common.Hash{}, nil),
	)

	// Pre-inserted one account
	db.CreateAccount(address2)

	bl := db.StartBulkLoad(1)
	// Database does not contain account with address1, hence this account is inserted
	db.Exist(address1)

	// Local cache must be empty
	if len(db.accounts) != 2 {
		t.Errorf("local cache has wrong number of accounts; got: %v, want: %v", len(db.accounts), 2)
	}
	// Account exists, so we should not need to create it in the bulk-load
	bl.SetBalance(address1, big.NewInt(100))
	bl.SetNonce(address1, uint64(1))
	bl.SetCode(address1, []byte{1})
	err := bl.Close()
	if err != nil {
		t.Errorf("failed to close bulk-load; %v", err)
	}

	bl = db.StartBulkLoad(1)
	db.Exist(address2)
	// Account exists, so we should not need to create it in the bulk-load
	bl.SetBalance(address2, big.NewInt(200))
	bl.SetNonce(address2, uint64(2))
	bl.SetCode(address2, []byte{2})
	err = bl.Close()
	if err != nil {
		t.Errorf("failed to close bulk-load; %v", err)
	}
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

type sameEffectAs struct {
	want common.Update
}

func (m sameEffectAs) Matches(x any) bool {
	got, ok := x.(common.Update)
	if !ok {
		return false
	}
	if err := got.Normalize(); err != nil {
		return false
	}
	return reflect.DeepEqual(got, m.want)
}

func (m sameEffectAs) String() string {
	return fmt.Sprintf("Same effect as %v", m.want)
}
