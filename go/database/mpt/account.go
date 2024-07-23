// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
)

// AccountInfo is the per-account information stored for each account in the
// State (excluding the storage root).
type AccountInfo struct {
	Nonce    common.Nonce
	Balance  amount.Amount
	CodeHash common.Hash
}

// IsEmpty checks whether the account information is empty, and thus, the
// default value. All accounts not present in an MPT are implicitly empty. Also
// no empty accounts may be explicitly stored.
func (a *AccountInfo) IsEmpty() bool {
	return *a == AccountInfo{}
}

// ----------------------------------------------------------------------------
//                           AccountInfo Encoder
// ----------------------------------------------------------------------------

type AccountInfoEncoder struct{}

func (AccountInfoEncoder) GetEncodedSize() int {
	return common.AddressSize + amount.BytesLength + common.HashSize
}

func (AccountInfoEncoder) Store(dst []byte, info *AccountInfo) {
	copy(dst[0:], info.Nonce[:])
	b := info.Balance.Bytes32()
	copy(dst[common.NonceSize:], b[:])
	copy(dst[common.NonceSize+amount.BytesLength:], info.CodeHash[:])
}

func (AccountInfoEncoder) Load(src []byte, info *AccountInfo) {
	copy(info.Nonce[:], src[0:])
	info.Balance = amount.NewFromBytes(src[common.NonceSize : common.NonceSize+amount.BytesLength]...)
	copy(info.CodeHash[:], src[common.NonceSize+amount.BytesLength:])
}
