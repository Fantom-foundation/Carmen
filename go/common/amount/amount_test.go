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
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
)

func TestAmount_New(t *testing.T) {
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
						t.Errorf("New() panicked unexpectedly: %v", r)
					}
				} else if test.wantPanic {
					t.Errorf("New() did not panic")
				}
			}()
			if got, want := New(test.args...), test.want; got != want {
				t.Errorf("wrong result, got %v, want %v", got, want)
			}
		})
	}
}

func TestAmount_NewFromUint256(t *testing.T) {
	tests := []struct {
		in  *uint256.Int
		out Amount
	}{
		{uint256.NewInt(0), New()},
		{uint256.NewInt(1), New(1)},
		{uint256.NewInt(256), New(256)},
		{new(uint256.Int).Lsh(uint256.NewInt(1), 64), New(1, 0)},
		{new(uint256.Int).Lsh(uint256.NewInt(1), 128), New(1, 0, 0)},
		{new(uint256.Int).Lsh(uint256.NewInt(1), 192), New(1, 0, 0, 0)},
	}

	for _, test := range tests {
		if got, want := NewFromUint256(test.in), test.out; want != got {
			t.Errorf("failed to convert %v to Amount, wanted %v, got %v", test.in, want, got)
		}
	}
}

func TestAmount_NewFromBytes(t *testing.T) {
	// test empty amount
	empty := NewFromBytes()
	if empty != New() {
		t.Errorf("empty amount should be zero")
	}

	amount := NewFromBytes(1, 2, 3, 4)
	if amount != New(0x01020304) {
		t.Errorf("amount should be 0x01020304")
	}

	// test amount with more than 8 bytes to test word order
	amount = NewFromBytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
	if amount != NewFromUint256(uint256.MustFromHex("0x102030405060708090A0B0C0D0E0F10")) {
		t.Errorf("amount should be 0x102030405060708090A0B0C0D0E0F10")
	}
}

func TestAmount_NewFromBigInt(t *testing.T) {
	// test amount with single value
	amount, err := NewFromBigInt(big.NewInt(100))
	if err != nil {
		t.Errorf("failed to create amount from big.Int: %v", err)
	}
	if amount != New(100) {
		t.Errorf("amount should be 100")
	}

	// test negativne amount
	_, err = NewFromBigInt(big.NewInt(-100))
	if err == nil {
		t.Errorf("negative amount should not be allowed")
	}

	// test amount with more than 256 bits
	_, err = NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), 256))
	if err == nil {
		t.Errorf("amount with more than 256 bits should not be allowed")
	}
}

func TestAmount_IsZero(t *testing.T) {
	zero := New()
	if !zero.IsZero() {
		t.Errorf("zero amount should be zero")
	}
	one := New(1)
	if one.IsZero() {
		t.Errorf("one amount should not be zero")
	}
}

func TestAmount_IsUint64(t *testing.T) {
	amount := New(100)
	if !amount.IsUint64() {
		t.Errorf("amount should be representable as uint64")
	}

	amount = New(1, 0, 0, 0)
	if amount.IsUint64() {
		t.Errorf("amount should not be representable as uint64")
	}
}

func TestAmount_Add(t *testing.T) {
	if got, want := Add(New(50), New(150)), New(200); got != want {
		t.Errorf("wrong amount: got %v, wanted: %v", got, want)
	}
}

func TestAmount_AddOverflow(t *testing.T) {
	res, overflow := AddOverflow(New(1), New(1))
	if overflow {
		t.Errorf("overflow should not happen")
	}
	if got, want := res, New(2); got != want {
		t.Errorf("wrong amount: got %v, wanted: %v", got, want)
	}

	_, overflow = AddOverflow(New(1), Max())
	if !overflow {
		t.Errorf("overflow should happen")
	}
}

func TestAmount_Sub(t *testing.T) {
	if got, want := Sub(New(150), New(50)), New(100); got != want {
		t.Errorf("wrong amount: got %v, wanted: %v", got, want)
	}
}

func TestAmount_SubUnderflow(t *testing.T) {
	res, underflow := SubUnderflow(New(2), New(1))
	if underflow {
		t.Errorf("underflow should not happen")
	}
	if got, want := res, New(1); got != want {
		t.Errorf("wrong amount: got %v, wanted: %v", got, want)
	}

	_, underflow = SubUnderflow(New(1), New(2))
	if !underflow {
		t.Errorf("underflow should happen")
	}
}

func TestAmount_ToUint256(t *testing.T) {
	amount := New(100)
	if got, want := amount.Uint256(), uint256.NewInt(100); got.Cmp(want) != 0 {
		t.Errorf("wrong amount: got %v, wanted: %v", amount, want)
	}
}

func TestAmount_ToBigInt(t *testing.T) {
	amount := New(100)
	if got, want := amount.ToBig(), big.NewInt(100); got.Cmp(want) != 0 {
		t.Errorf("wrong amount: got %v, wanted: %v", amount, want)
	}
}

func TestAmount_Bytes32(t *testing.T) {
	x := New(1, 2, 3, 4)
	xBytes := x.Bytes32()
	if !bytes.Equal(xBytes[:], []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4}) {
		t.Fail()
	}
}

func TestAmount_Max(t *testing.T) {
	if got, want := Max(), New(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF); got != want {
		t.Errorf("wrong amount: got %v, wanted: %v", got, want)
	}
}
