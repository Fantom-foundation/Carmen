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
	tests := []struct {
		name      string
		args      []uint64
		want      Amount
		wantPanic bool
	}{
		{"No arguments", []uint64{}, Amount{[4]uint64{0, 0, 0, 0}}, false},
		{"One argument", []uint64{1}, Amount{[4]uint64{1, 0, 0, 0}}, false},
		{"Two arguments", []uint64{1, 2}, Amount{[4]uint64{2, 1, 0, 0}}, false},
		{"Three arguments", []uint64{1, 2, 3}, Amount{[4]uint64{3, 2, 1, 0}}, false},
		{"Four arguments", []uint64{1, 2, 3, 4}, Amount{[4]uint64{4, 3, 2, 1}}, false},
		{"Too many arguments", []uint64{1, 2, 3, 4, 5}, Amount{}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !test.wantPanic {
						t.Errorf("NewAmount() panicked unexpectedly: %v", r)
					}
				} else if test.wantPanic {
					t.Errorf("NewAmount() did not panic")
				}
			}()
			if got, want := NewAmount(test.args...), test.want; got != want {
				t.Errorf("wrong result, got %v, want %v", got, want)
			}
		})
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
		{new(uint256.Int).Lsh(uint256.NewInt(1), 64), NewAmount(1, 0)},
		{new(uint256.Int).Lsh(uint256.NewInt(1), 128), NewAmount(1, 0, 0)},
		{new(uint256.Int).Lsh(uint256.NewInt(1), 192), NewAmount(1, 0, 0, 0)},
	}

	for _, test := range tests {
		if got, want := NewAmountFromUint256(test.in), test.out; want != got {
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

	amount := NewAmountFromBytes(1, 2, 3, 4)
	if amount != NewAmount(0x01020304) {
		t.Errorf("amount should be 0x01020304")
	}

	// test amount with more than 8 bytes to test word order
	amount = NewAmountFromBytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
	if amount != NewAmountFromUint256(uint256.MustFromHex("0x102030405060708090A0B0C0D0E0F10")) {
		t.Errorf("amount should be 0x102030405060708090A0B0C0D0E0F10")
	}
}

func TestAmount_NewAmountFromBigInt(t *testing.T) {
	// test amount with single value
	amount, err := NewAmountFromBigInt(big.NewInt(100))
	if err != nil {
		t.Errorf("failed to create amount from big.Int: %v", err)
	}
	if amount != NewAmount(100) {
		t.Errorf("amount should be 100")
	}

	// test negativne amount
	_, err = NewAmountFromBigInt(big.NewInt(-100))
	if err == nil {
		t.Errorf("negative amount should not be allowed")
	}

	// test amount with more than 256 bits
	_, err = NewAmountFromBigInt(new(big.Int).Lsh(big.NewInt(1), 256))
	if err == nil {
		t.Errorf("amount with more than 256 bits should not be allowed")
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

func TestAmount_IsUint64(t *testing.T) {
	amount := NewAmount(100)
	if !amount.IsUint64() {
		t.Errorf("amount should be representable as uint64")
	}

	amount = NewAmount(1, 0, 0, 0)
	if amount.IsUint64() {
		t.Errorf("amount should not be representable as uint64")
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
