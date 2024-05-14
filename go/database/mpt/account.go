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
)

// AccountInfo is the per-account information stored for each account in the
// State (excluding the storage root).
type AccountInfo struct {
	Nonce    common.Nonce
	Balance  common.Balance
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
	return common.AddressSize + common.BalanceSize + common.HashSize
}

func (AccountInfoEncoder) Store(dst []byte, info *AccountInfo) {
	copy(dst[0:], info.Nonce[:])
	copy(dst[common.NonceSize:], info.Balance[:])
	copy(dst[common.NonceSize+common.BalanceSize:], info.CodeHash[:])
}

func (AccountInfoEncoder) Load(src []byte, info *AccountInfo) {
	copy(info.Nonce[:], src[0:])
	copy(info.Balance[:], src[common.NonceSize:])
	copy(info.CodeHash[:], src[common.NonceSize+common.BalanceSize:])
}
