package s4

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

type AccountInfo struct {
	Nonce    common.Nonce
	Balance  common.Balance
	CodeHash common.Hash
}

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
