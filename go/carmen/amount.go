package carmen

import (
	"math/big"

	"github.com/holiman/uint256"
)

// Amount is a 256-bit unsigned integer used for token values like balances.
type Amount struct {
	internal uint256.Int
}

// NewAmount creates a new amount from a list of uint64 arguments.
// The constructor panics if more than 2 arguments are provided.
// If no arguments are provided, the amount is set to zero.
// If one argument is provided, the amount is set to the value of the argument.
// If two arguments are provided, the amount is set to the value of the first
// argument shifted left by 64 bits and added to the value of the second argument (arg1*2^64 + arg2).
func NewAmount(args ...uint64) Amount {
	result := uint256.Int{}
	switch len(args) {
	case 0:
		result.SetUint64(0)
	case 1:
		result.SetUint64(args[0])
	case 2:
		high := uint256.NewInt(args[0])
		high.Lsh(high, 64) // Shift left by 64 bits
		low := uint256.NewInt(args[1])
		result.Set(high.Add(high, low)) // Combine high and low parts
	default:
		panic("NewAmount supports up to 1 arguments only")
	}
	return Amount{internal: result}
}

// NewAmountFromUint256 creates a new amount from an uint256.
func NewAmountFromUint256(value *uint256.Int) Amount {
	return Amount{internal: *value}
}

// NewAmountFromBytes creates a new Amount instance from up to 32 byte arguments.
// The arguments are given in the order from most significant to the least
// significant by padding leading zeros as needed. No argument results in a
// value of zero.
func NewAmountFromBytes(bytes ...byte) Amount {
	if len(bytes) > 32 {
		panic("Too many arguments")
	}
	result := Amount{}
	result.internal.SetBytes(bytes)
	return result
}

// NewAmountFromBigInt creates a new Amount instance from a big.Int.
// The constructor panics if the big.Int is negative or has more than 256 bits.
func NewAmountFromBigInt(b *big.Int) Amount {
	if b == nil {
		return NewAmount()
	}
	if b.Cmp(big.NewInt(0)) == -1 {
		panic("Cannot construct Amount from negative big.Int")
	}
	result := uint256.Int{}
	overflow := result.SetFromBig(b)
	if overflow {
		panic("Cannot construct U256 from big.Int with more than 256 bits")
	}
	return Amount{internal: result}
}

// Uint64 returns the amount as an uint64.
func (a Amount) Uint64() uint64 {
	return a.internal.Uint64()
}

// IsZero returns true if the amount is zero.
func (a Amount) IsZero() bool {
	return a.internal.IsZero()
}

// ToBig returns a bigInt version of the amount.
func (a Amount) ToBig() *big.Int {
	return a.internal.ToBig()
}

// Add returns the sum of two amounts.
func (a Amount) Add(b Amount) Amount {
	result := Amount{}
	result.internal.Add(&a.internal, &b.internal)
	return result
}

// Sub returns the difference of two amounts.
func (a Amount) Sub(b Amount) Amount {
	result := Amount{}
	result.internal.Sub(&a.internal, &b.internal)
	return result
}

// String returns the string representation of the amount.
func (a Amount) String() string {
	return a.internal.String()
}
