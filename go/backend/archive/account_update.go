// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package archive

import (
	"encoding/binary"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"

	"hash"
	"sort"
)

// AccountUpdate combines the updates applied to a single account in one block.
// Its main intention is to be utilized as the diff unit for hashing incremental updates on accounts in archives.
type AccountUpdate struct {
	created    bool
	deleted    bool
	hasBalance bool
	balance    amount.Amount
	hasNonce   bool
	nonce      common.Nonce
	hasCode    bool
	code       []byte
	storage    []AccountSlotUpdate
}

type AccountSlotUpdate struct {
	Key   common.Key
	Value common.Value
}

// AccountUpdatesFrom process a common.Update into a map of AccountUpdate
func AccountUpdatesFrom(update *common.Update) ([]common.Address, map[common.Address]*AccountUpdate) {
	accountUpdates := make(map[common.Address]*AccountUpdate)

	get := func(address common.Address) *AccountUpdate {
		au, exists := accountUpdates[address]
		if !exists {
			au = new(AccountUpdate)
			accountUpdates[address] = au
		}
		return au
	}

	for _, address := range update.CreatedAccounts {
		get(address).created = true
	}
	for _, address := range update.DeletedAccounts {
		get(address).deleted = true
	}
	for _, balanceUpdate := range update.Balances {
		accountUpdate := get(balanceUpdate.Account)
		accountUpdate.hasBalance = true
		accountUpdate.balance = balanceUpdate.Balance
	}
	for _, nonceUpdate := range update.Nonces {
		accountUpdate := get(nonceUpdate.Account)
		accountUpdate.hasNonce = true
		accountUpdate.nonce = nonceUpdate.Nonce
	}
	for _, codeUpdate := range update.Codes {
		accountUpdate := get(codeUpdate.Account)
		accountUpdate.hasCode = true
		accountUpdate.code = codeUpdate.Code
	}
	for _, slotUpdate := range update.Slots {
		accountUpdate := get(slotUpdate.Account)
		accountUpdate.storage = append(accountUpdate.storage, AccountSlotUpdate{
			Key:   slotUpdate.Key,
			Value: slotUpdate.Value,
		})
	}

	// get sorted list of updated accounts
	accounts := make([]common.Address, len(accountUpdates))
	i := 0
	for account := range accountUpdates {
		accounts[i] = account
		i++
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Compare(&accounts[j]) < 0 })

	return accounts, accountUpdates
}

func (au *AccountUpdate) GetHash(hasher hash.Hash) common.Hash {
	// The hash of an account update is computed by hashing a byte string composed as follows:
	// * a byte summarizing account change events:
	//   * bit 0 is set if the account is created,
	//   * bit 1 is set if the account is deleted.
	//   * bit 2 is set if the account balance is changed.
	//   * bit 3 is set if the account nonce is changed.
	//   * bit 4 is set if the account code is changed.
	// * the 16 byte of the updated balance, if it was updated.
	// * the 8 byte of the updated nonce, if it was updated.
	// * the 4 byte of the new code size followed by the new code itself, if it was updated.
	// * the concatenated list of updated slots.

	hasher.Reset()
	var stateChange byte
	if au.created {
		stateChange |= 1
	}
	if au.deleted {
		stateChange |= 2
	}
	if au.hasBalance {
		stateChange |= 4
	}
	if au.hasNonce {
		stateChange |= 8
	}
	if au.hasCode {
		stateChange |= 16
	}
	hasher.Write([]byte{stateChange})
	if au.hasBalance {
		b := au.balance.Bytes32()
		hasher.Write(b[:])
	}
	if au.hasNonce {
		hasher.Write(au.nonce[:])
	}
	if au.hasCode {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], uint32(len(au.code)))
		hasher.Write(size[:])
		hasher.Write(au.code)
	}
	for _, slotUpdate := range au.storage {
		hasher.Write(slotUpdate.Key[:])
		hasher.Write(slotUpdate.Value[:])
	}
	hashOfUpdate := hasher.Sum(nil)
	return *(*common.Hash)(hashOfUpdate)
}
