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

func (AccountInfoEncoder) Store(dst []byte, info *AccountInfo) error {
	copy(dst[0:], info.Nonce[:])
	copy(dst[common.NonceSize:], info.Balance[:])
	copy(dst[common.NonceSize+common.BalanceSize:], info.CodeHash[:])
	return nil
}

func (AccountInfoEncoder) Load(src []byte, info *AccountInfo) error {
	copy(info.Nonce[:], src[0:])
	copy(info.Balance[:], src[common.NonceSize:])
	copy(info.CodeHash[:], src[common.NonceSize+common.BalanceSize:])
	return nil
}
