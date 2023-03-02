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

func TestSlotIdxSerializer32(t *testing.T) {
	var s common.SlotIdxSerializer32
	var _ common.Serializer[common.SlotIdx1[uint32]] = s

	// convert back and forth
	slotIdx := common.SlotIdx1[uint32]{
		AddressIdx: 123,
		KeyIdx:     456,
	}
	b := s.ToBytes(slotIdx)
	slotIdx2 := s.FromBytes(b)

	if slotIdx != slotIdx2 {
		t.Errorf("Conversion fails: %x := %x", slotIdx, slotIdx2)
	}
}
