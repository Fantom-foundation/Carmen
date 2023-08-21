package common

import (
	"fmt"
	"math/big"
	"testing"
)

var balance_value_pairs = []struct {
	i *big.Int
	b Balance
}{
	{big.NewInt(0), Balance{}},
	{big.NewInt(1), Balance{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
	{big.NewInt(2), Balance{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}},
	{big.NewInt(256), Balance{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}},
	{big.NewInt(1 << 32), Balance{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}},
	{maxBalance, Balance{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}},
}

func TestBigIntToBalanceConversion(t *testing.T) {
	for _, pair := range balance_value_pairs {
		balance, err := ToBalance(pair.i)
		if err != nil {
			t.Errorf("Failed to convert %v to balance: %v", pair.i, err)
		}
		if balance != pair.b {
			t.Errorf("Incorrect conversion of numeric value %v into balance - wanted %v, got %v", pair.i, pair.b, balance)
		}
	}
}

func TestBalanceToBigIntConversion(t *testing.T) {
	for _, pair := range balance_value_pairs {
		val := pair.b.ToBigInt()
		if val.Cmp(pair.i) != 0 {
			t.Errorf("Incorrect conversion of balance %v into numeric value - wanted %v, got %v", pair.b, pair.i, val)
		}
	}
}

func TestNegativeValuesCanNotBeConvertedToBalances(t *testing.T) {
	if _, err := ToBalance(big.NewInt(-1)); err == nil {
		t.Errorf("Converting negative values should have raised an error.")
	}
}

func TestTooLargeValuesCanNotBeConvertedToBalances(t *testing.T) {
	too_large := (&big.Int{}).Add(maxBalance, one)
	if _, err := ToBalance(too_large); err == nil {
		t.Errorf("Converting values exceeding the maximum blance value have raised an error.")
	}
}

var nonce_value_pairs = []struct {
	i uint64
	n Nonce
}{
	{0, Nonce{}},
	{1, Nonce{0, 0, 0, 0, 0, 0, 0, 1}},
	{2, Nonce{0, 0, 0, 0, 0, 0, 0, 2}},
	{256, Nonce{0, 0, 0, 0, 0, 0, 1, 0}},
	{1 << 32, Nonce{0, 0, 0, 1, 0, 0, 0, 0}},
	{^uint64(0), Nonce{255, 255, 255, 255, 255, 255, 255, 255}},
}

func TestUint64ToNonceConversion(t *testing.T) {
	for _, pair := range nonce_value_pairs {
		nonce := ToNonce(pair.i)
		if nonce != pair.n {
			t.Errorf("Incorrect conversion of numeric value %v into nonce - wanted %v, got %v", pair.i, pair.n, nonce)
		}
	}
}

func TestNonceToUint64Conversion(t *testing.T) {
	for _, pair := range nonce_value_pairs {
		val := pair.n.ToUint64()
		if val != pair.i {
			t.Errorf("Incorrect conversion of nonce %v into numeric value - wanted %v, got %v", pair.n, pair.i, val)
		}
	}
}

func TestKeccak256NilHashesLikeEmptyList(t *testing.T) {
	nil_hash := GetKeccak256Hash(nil)
	empty_hash := GetKeccak256Hash([]byte{})
	if nil_hash != empty_hash {
		t.Errorf("nil does not hash like empty slice, got %x, wanted %x", nil_hash, empty_hash)
	}
}

func TestKeccak256KnownHashes(t *testing.T) {
	inputs := []struct {
		plain, hash string
	}{
		{"", "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"},
		{"a", "3ac225168df54212a25c1c01fd35bebfea408fdac2e31ddd6f80a4bbf9a5f1cb"},
		{"abc", "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"},
	}
	for _, input := range inputs {
		hash := GetKeccak256Hash([]byte(input.plain))
		if input.hash != fmt.Sprintf("%x", hash) {
			t.Errorf("invalid hash: %x (expected %s)", hash, input.hash)
		}
	}
}

func TestHashFromString(t *testing.T) {
	tests := []struct {
		input  string
		result Hash
	}{
		{"0000000000000000000000000000000000000000000000000000000000000000", Hash{}},
		{"1000000000000000000000000000000000000000000000000000000000000000", Hash{0x10}},
		{"1200000000000000000000000000000000000000000000000000000000000000", Hash{0x12}},
		{"123456789abcdefABCDEF0000000000000000000000000000000000000000000", Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xfa, 0xbc, 0xde, 0xf0}},
	}

	for _, test := range tests {
		if got, want := HashFromString(test.input), test.result; got != want {
			t.Errorf("failed to parse %s: expected %v, got %v", test.input, want, got)
		}
	}
}
