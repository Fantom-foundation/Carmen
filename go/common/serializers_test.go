package common_test

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestAddressSerializer(t *testing.T) {
	var s common.AddressSerializer
	var _ common.Serializer[common.Address] = s
}

func TestKeySerializer(t *testing.T) {
	var s common.KeySerializer
	var _ common.Serializer[common.Key] = s
}

func TestValueSerializer(t *testing.T) {
	var s common.ValueSerializer
	var _ common.Serializer[common.Value] = s
}

func TestHashSerializer(t *testing.T) {
	var s common.HashSerializer
	var _ common.Serializer[common.Hash] = s
}

func TestNonceSerializer(t *testing.T) {
	var s common.NonceSerializer
	var _ common.Serializer[common.Nonce] = s
}
func TestBalanceSerializer(t *testing.T) {
	var s common.BalanceSerializer
	var _ common.Serializer[common.Balance] = s
}
