// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
)

func TestAmount_NewAmount(t *testing.T) {
	// test empty amount
	empty := NewAmount()
	if empty.Uint64() != 0 {
		t.Errorf("empty amount should be zero")
	}

	// test amount with single value
	amount := NewAmount(100)
	if amount.Uint64() != 100 {
		t.Errorf("amount should be 100")
	}

	// the amount should be 3*2^64 + 1234
	amount = NewAmount(3, 1234)

	// instantiate the amount using big.Int
	high := big.NewInt(3)
	high.Lsh(high, 64)
	low := big.NewInt(1234)
	result := new(big.Int).Add(high, low)

	if amount != NewAmountFromBigInt(result) {
		t.Errorf("amount should be 3*2^64 + 1234")
	}
}

func TestAmount_NewAmountFromUint256(t *testing.T) {
	tests := []struct {
		in  *uint256.Int
		out Amount
	}{
		{uint256.NewInt(0), NewAmount()},
		{uint256.NewInt(1), NewAmount(1)},
		{uint256.NewInt(256), NewAmount(256)},
	}

	for _, test := range tests {
		got := NewAmountFromUint256(test.in)
		want := test.out
		if want != got {
			t.Errorf("failed to convert %v to Amount, wanted %v, got %v", test.in, want, got)
		}
	}
}

func TestAmount_NewAmountFromBytes(t *testing.T) {
	// test empty amount
	empty := NewAmountFromBytes()
	if empty != NewAmount() {
		t.Errorf("empty amount should be zero")
	}

	// test amount with single value
	amount := NewAmountFromBytes([]byte{1, 2, 3, 4}...)
	if amount != NewAmount(0x01020304) {
		t.Errorf("amount should be 0x01020304")
	}
}

func TestAmount_NewAmountFromBigInt(t *testing.T) {
	// test amount with single value
	amount := NewAmountFromBigInt(big.NewInt(100))
	if amount != NewAmount(100) {
		t.Errorf("amount should be 100")
	}
}

func TestAmount_IsZero(t *testing.T) {
	zero := NewAmount()
	if !zero.IsZero() {
		t.Errorf("zero amount should be zero")
	}
	one := NewAmount(1)
	if one.IsZero() {
		t.Errorf("one amount should not be zero")
	}
}

func TestAmount_Add(t *testing.T) {
	amount := NewAmount(100)
	res := amount.Add(NewAmount(100))
	if res != NewAmount(200) {
		t.Errorf("amount should be 200")
	}
}

func TestAmount_Sub(t *testing.T) {
	amount := NewAmount(100)
	res := amount.Sub(NewAmount(50))
	if res != NewAmount(50) {
		t.Errorf("amount should be 50")
	}
}
