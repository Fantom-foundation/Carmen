// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package amount

import (
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
)

// BytesLength is the length of the byte representation of an amount.
const BytesLength = 32

// Amount is a 256-bit unsigned integer used for token values like balances.
type Amount struct {
	internal uint256.Int
}

// New creates a new Amount from up to 4 uint64 arguments. The
// arguments are given in the Big Endian order. No argument results in a value of zero.
// The constructor panics if more than 4 arguments are given.
func New(args ...uint64) Amount {
	if len(args) > 4 {
		panic("too many arguments")
	}
	result := Amount{}
	offset := 4 - len(args)
	for i := 0; i < len(args); i++ {
		result.internal[3-i-offset] = args[i]
	}
	return result
}

// NewFromUint256 creates a new amount from an uint256.
func NewFromUint256(value *uint256.Int) Amount {
	return Amount{internal: *value}
}

// NewFromBytes creates a new Amount instance from up to 32 byte arguments.
// The arguments are given in the Big Endian order. No argument results in a
// value of zero. The constructor panics if more than 32 arguments are given.
func NewFromBytes(bytes ...byte) Amount {
	if len(bytes) > 32 {
		panic("too many arguments")
	}
	result := Amount{}
	result.internal.SetBytes(bytes)
	return result
}

// NewFromBigInt creates a new Amount instance from a big.Int.
func NewFromBigInt(b *big.Int) (Amount, error) {
	if b == nil {
		return New(), nil
	}
	if b.Sign() < 0 {
		return Amount{}, fmt.Errorf("cannot construct Amount from negative big.Int")
	}
	result := uint256.Int{}
	overflow := result.SetFromBig(b)
	if overflow {
		return Amount{}, fmt.Errorf("big.Int has more than 256 bits")
	}
	return Amount{internal: result}, nil
}

// Uint64 returns the amount as an uint64. The result is only valid if `IsUint64()` returns true.
func (a Amount) Uint64() uint64 {
	return a.internal.Uint64()
}

// IsZero returns true if the amount is zero.
func (a Amount) IsZero() bool {
	return a.internal.IsZero()
}

// IsUint64 returns true if the amount is representable as an uint64.
func (a Amount) IsUint64() bool {
	return a.internal.IsUint64()
}

// ToBig returns a bigInt version of the amount.
func (a Amount) ToBig() *big.Int {
	return a.internal.ToBig()
}

// String returns the string representation of the amount.
func (a Amount) String() string {
	return a.internal.String()
}

// Uint256 returns the amount as an uint256.
func (a Amount) Uint256() uint256.Int {
	return a.internal
}

// Bytes32 returns the amount as a 32 byte array.
func (a Amount) Bytes32() [32]byte {
	return a.internal.Bytes32()
}

// Add returns the sum of two amounts.
func Add(a, b Amount) Amount {
	result := Amount{}
	result.internal.Add(&a.internal, &b.internal)
	return result
}

// AddOverflow returns the sum of two amounts and a boolean indicating overflow.
func AddOverflow(a, b Amount) (Amount, bool) {
	result := Amount{}
	_, overflow := result.internal.AddOverflow(&a.internal, &b.internal)
	return result, overflow
}

// Sub returns the difference of two amounts.
func Sub(a, b Amount) Amount {
	result := Amount{}
	result.internal.Sub(&a.internal, &b.internal)
	return result
}

// SubUnderflow returns the difference of two amounts and a boolean indicating underflow.
func SubUnderflow(a, b Amount) (Amount, bool) {
	result := Amount{}
	_, underflow := result.internal.SubOverflow(&a.internal, &b.internal)
	return result, underflow
}

// Max returns the maximum amount.
func Max() Amount {
	return New(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
}
