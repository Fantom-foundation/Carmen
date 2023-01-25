package state

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// Update summarizes the effective changes to the state DB at the end of a block.
// It combines changes to the account state (created or deleted), balances, nonces
// codes, and slot updates.
//
// An example use of an update would look like this:
//
//	// Create an update.
//	update := Update{}
//	// Fill in changes.
//	// Note: for each type of change, updates must be in order and unique.
//	update.AppendCreateAccount(..)
//	update.AppendCreateAccount(..)
//	...
//	// Optionally, check that the provided data is valid (sorted and unique).
//	err := update.Check()
//
// Valid instances can then be forwarded to the State as a block update.
type Update struct {
	deletedAccounts []common.Address
	createdAccounts []common.Address
	balances        []balanceUpdate
	nonces          []nonceUpdate
	codes           []codeUpdate
	slots           []slotUpdate
}

// AppendCreateAccount registers an account to be deleted in this block. Delete
// operations are the first to be carried out, leading to a clearing of the
// account's storage. Subsequent account creations or balance / nonce / slot
// updates will take affect after the deletion of the account.
func (u *Update) AppendDeleteAccount(addr common.Address) {
	u.AppendDeleteAccounts([]common.Address{addr})
}

// AppendDeleteAccounts is the same as AppendDeleteAccount, but for a slice.
func (u *Update) AppendDeleteAccounts(addr []common.Address) {
	u.deletedAccounts = append(u.deletedAccounts, addr...)
}

// AppendCreateAccount registers a new account to be created in this block.
// This takes affect after deleting the accounts listed in this update.
func (u *Update) AppendCreateAccount(addr common.Address) {
	u.AppendCreateAccounts([]common.Address{addr})
}

// AppendCreateAccounts is the same as AppendCreateAccount, but for a slice.
func (u *Update) AppendCreateAccounts(addr []common.Address) {
	u.createdAccounts = append(u.createdAccounts, addr...)
}

// AppendBalanceUpdate registers a balance update to be conducted.
func (u *Update) AppendBalanceUpdate(addr common.Address, balance common.Balance) {
	u.balances = append(u.balances, balanceUpdate{addr, balance})
}

// AppendNonceUpdate registers a nonce update to be conducted.
func (u *Update) AppendNonceUpdate(addr common.Address, nonce common.Nonce) {
	u.nonces = append(u.nonces, nonceUpdate{addr, nonce})
}

// AppendCodeUpdate registers a code update to be conducted.
func (u *Update) AppendCodeUpdate(addr common.Address, code []byte) {
	u.codes = append(u.codes, codeUpdate{addr, code})
}

// AppendSlotUpdate registers a slot value update to be conducted
func (u *Update) AppendSlotUpdate(addr common.Address, key common.Key, value common.Value) {
	u.slots = append(u.slots, slotUpdate{addr, key, value})
}

// Check verifies that all updates are unique and in order.
func (u *Update) Check() error {
	accountLess := func(a, b *common.Address) bool {
		return a.Compare(b) < 0
	}
	if !isSortedAndUnique(u.createdAccounts, accountLess) {
		return fmt.Errorf("created accounts are not in order or unique")
	}
	if !isSortedAndUnique(u.deletedAccounts, accountLess) {
		return fmt.Errorf("deleted accounts are not in order or unique")
	}

	balanceLess := func(a, b *balanceUpdate) bool {
		return accountLess(&a.account, &b.account)
	}

	if !isSortedAndUnique(u.balances, balanceLess) {
		return fmt.Errorf("balance updates are not in order or unique")
	}

	nonceLess := func(a, b *nonceUpdate) bool {
		return accountLess(&a.account, &b.account)
	}

	if !isSortedAndUnique(u.nonces, nonceLess) {
		return fmt.Errorf("nonce updates are not in order or unique")
	}

	codeLess := func(a, b *codeUpdate) bool {
		return accountLess(&a.account, &b.account)
	}

	if !isSortedAndUnique(u.codes, codeLess) {
		return fmt.Errorf("nonce updates are not in order or unique")
	}

	slotLess := func(a, b *slotUpdate) bool {
		accountCompare := a.account.Compare(&b.account)
		return accountCompare < 0 || (accountCompare == 0 && a.key.Compare(&b.key) < 0)
	}

	if !isSortedAndUnique(u.slots, slotLess) {
		return fmt.Errorf("storage updates are not in order or unique")
	}

	return nil
}

// apply distributes the updates combined in a Update struct to individual update calls.
// This is intended as the default implementation for the Go, C++, and Mock state. However,
// implementations may chose to implement specialized versions.
func (u *Update) apply(s State) error {
	for _, addr := range u.deletedAccounts {
		if err := s.DeleteAccount(addr); err != nil {
			return err
		}
	}
	for _, addr := range u.createdAccounts {
		if err := s.CreateAccount(addr); err != nil {
			return err
		}
	}
	for _, change := range u.balances {
		if err := s.SetBalance(change.account, change.balance); err != nil {
			return err
		}
	}
	for _, change := range u.nonces {
		if err := s.SetNonce(change.account, change.nonce); err != nil {
			return err
		}
	}
	for _, change := range u.codes {
		if err := s.SetCode(change.account, change.code); err != nil {
			return err
		}
	}
	for _, change := range u.slots {
		if err := s.SetStorage(change.account, change.key, change.value); err != nil {
			return err
		}
	}
	return nil
}

type balanceUpdate struct {
	account common.Address
	balance common.Balance
}

type nonceUpdate struct {
	account common.Address
	nonce   common.Nonce
}

type codeUpdate struct {
	account common.Address
	code    []byte
}

type slotUpdate struct {
	account common.Address
	key     common.Key
	value   common.Value
}

func isSortedAndUnique[T any](list []T, less func(a, b *T) bool) bool {
	for i := 0; i < len(list)-1; i++ {
		if !less(&list[i], &list[i+1]) {
			return false
		}
	}
	return true
}
