package common

import (
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
