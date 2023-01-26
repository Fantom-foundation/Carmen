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

const updateEncodingVersion byte = 0

func UpdateFromBytes(data []byte) (Update, error) {
	if len(data) < 1+6*2 {
		return Update{}, fmt.Errorf("invalid encoding, too few bytes")
	}
	if data[0] != updateEncodingVersion {
		return Update{}, fmt.Errorf("unknown encoding version: %d", data[0])
	}

	data = data[1:]
	deletedAccountSize := readUint16(data[0:])
	createdAccountSize := readUint16(data[2:])
	balancesSize := readUint16(data[4:])
	codesSize := readUint16(data[6:])
	noncesSize := readUint16(data[8:])
	slotsSize := readUint16(data[10:])

	data = data[12:]

	res := Update{}

	// Read list of deleted accounts
	if deletedAccountSize > 0 {
		if len(data) < int(deletedAccountSize)*len(common.Address{}) {
			return res, fmt.Errorf("invalid encoding, truncated address list")
		}
		res.deletedAccounts = make([]common.Address, deletedAccountSize)
		for i := 0; i < int(deletedAccountSize); i++ {
			copy(res.deletedAccounts[i][:], data[:])
			data = data[len(common.Address{}):]
		}
	}

	// Read list of created accounts
	if createdAccountSize > 0 {
		if len(data) < int(createdAccountSize)*len(common.Address{}) {
			return res, fmt.Errorf("invalid encoding, truncated address list")
		}
		res.createdAccounts = make([]common.Address, createdAccountSize)
		for i := 0; i < int(createdAccountSize); i++ {
			copy(res.createdAccounts[i][:], data[:])
			data = data[len(common.Address{}):]
		}
	}

	// Read list of balance updates
	if balancesSize > 0 {
		if len(data) < int(balancesSize)*(len(common.Address{})+len(common.Balance{})) {
			return res, fmt.Errorf("invalid encoding, balance list truncated")
		}
		res.balances = make([]balanceUpdate, balancesSize)
		for i := 0; i < int(balancesSize); i++ {
			copy(res.balances[i].account[:], data[:])
			data = data[len(common.Address{}):]
			copy(res.balances[i].balance[:], data[:])
			data = data[len(common.Balance{}):]
		}
	}

	// Read list of code updates
	if codesSize > 0 {
		res.codes = make([]codeUpdate, codesSize)
		for i := 0; i < int(codesSize); i++ {
			if len(data) < len(common.Address{})+2 {
				return res, fmt.Errorf("invalid encoding, truncated code list")
			}
			copy(res.codes[i].account[:], data[:])
			data = data[len(common.Address{}):]
			codeLength := readUint16(data)
			data = data[2:]
			if len(data) < int(codeLength) {
				return res, fmt.Errorf("invalid encoding, truncated code")
			}
			res.codes[i].code = make([]byte, codeLength)
			copy(res.codes[i].code[:], data[0:codeLength])
			data = data[codeLength:]
		}
	}

	// Read list of nonce updates
	if noncesSize > 0 {
		if len(data) < int(noncesSize)*(len(common.Address{})+len(common.Nonce{})) {
			return res, fmt.Errorf("invalid encoding, nonce list truncated")
		}
		res.nonces = make([]nonceUpdate, noncesSize)
		for i := 0; i < int(noncesSize); i++ {
			copy(res.nonces[i].account[:], data[:])
			data = data[len(common.Address{}):]
			copy(res.nonces[i].nonce[:], data[:])
			data = data[len(common.Nonce{}):]
		}
	}

	// Read list of slot updates
	if slotsSize > 0 {
		if len(data) < int(slotsSize)*(len(common.Address{})+len(common.Key{})+len(common.Value{})) {
			return res, fmt.Errorf("invalid encoding, slot list truncated")
		}
		res.slots = make([]slotUpdate, slotsSize)
		for i := 0; i < int(slotsSize); i++ {
			copy(res.slots[i].account[:], data[:])
			data = data[len(common.Address{}):]
			copy(res.slots[i].key[:], data[:])
			data = data[len(common.Key{}):]
			copy(res.slots[i].value[:], data[:])
			data = data[len(common.Value{}):]
		}
	}

	return res, nil
}

func (u *Update) ToBytes() []byte {
	const addrLength = len(common.Address{})
	size := 1 + 6*2 // version + sizes
	size += len(u.deletedAccounts) * addrLength
	size += len(u.createdAccounts) * addrLength
	size += len(u.balances) * (addrLength + len(common.Balance{}))
	size += len(u.nonces) * (addrLength + len(common.Nonce{}))
	size += len(u.slots) * (addrLength + len(common.Key{}) + len(common.Value{}))
	for _, cur := range u.codes {
		size += addrLength + 2 + len(cur.code)
	}

	res := make([]byte, 0, size)

	res = append(res, updateEncodingVersion)
	res = appendUint16(res, len(u.deletedAccounts))
	res = appendUint16(res, len(u.createdAccounts))
	res = appendUint16(res, len(u.balances))
	res = appendUint16(res, len(u.codes))
	res = appendUint16(res, len(u.nonces))
	res = appendUint16(res, len(u.slots))

	for _, addr := range u.deletedAccounts {
		res = append(res, addr[:]...)
	}
	for _, addr := range u.createdAccounts {
		res = append(res, addr[:]...)
	}
	for _, cur := range u.balances {
		res = append(res, cur.account[:]...)
		res = append(res, cur.balance[:]...)
	}
	for _, cur := range u.codes {
		res = append(res, cur.account[:]...)
		res = appendUint16(res, len(cur.code))
		res = append(res, cur.code...)
	}
	for _, cur := range u.nonces {
		res = append(res, cur.account[:]...)
		res = append(res, cur.nonce[:]...)
	}
	for _, cur := range u.slots {
		res = append(res, cur.account[:]...)
		res = append(res, cur.key[:]...)
		res = append(res, cur.value[:]...)
	}

	return res
}

func readUint16(data []byte) uint16 {
	return uint16(data[0])<<8 | uint16(data[1])
}

func appendUint16(data []byte, value int) []byte {
	data = append(data, byte(value>>8))
	data = append(data, byte(value))
	return data
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
func (u *Update) apply(s directUpdateState) error {
	for _, addr := range u.deletedAccounts {
		if err := s.deleteAccount(addr); err != nil {
			return err
		}
	}
	for _, addr := range u.createdAccounts {
		if err := s.createAccount(addr); err != nil {
			return err
		}
	}
	for _, change := range u.balances {
		if err := s.setBalance(change.account, change.balance); err != nil {
			return err
		}
	}
	for _, change := range u.nonces {
		if err := s.setNonce(change.account, change.nonce); err != nil {
			return err
		}
	}
	for _, change := range u.codes {
		if err := s.setCode(change.account, change.code); err != nil {
			return err
		}
	}
	for _, change := range u.slots {
		if err := s.setStorage(change.account, change.key, change.value); err != nil {
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
