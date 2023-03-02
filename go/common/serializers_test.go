package common_test

import (
	"bytes"
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
	var s common.SlotIdx32Serializer
	var _ common.Serializer[common.SlotIdx[uint32]] = s

	// convert back and forth
	slotIdx := common.SlotIdx[uint32]{
		AddressIdx: 123,
		KeyIdx:     456,
	}
	b := s.ToBytes(slotIdx)
	slotIdx2 := s.FromBytes(b)

	if slotIdx != slotIdx2 {
		t.Errorf("Conversion fails: %x := %x", slotIdx, slotIdx2)
	}
}

func TestSlotIdx32KeySerializer(t *testing.T) {
	var s common.SlotIdx32KeySerializer
	var _ common.Serializer[common.SlotIdxKey[uint32]] = s

	// convert back and forth
	slotIdx := common.SlotIdxKey[uint32]{
		AddressIdx: 0x87654321,
		Key:        common.Key{0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x99},
	}
	b := s.ToBytes(slotIdx)
	slotIdx2 := s.FromBytes(b)
	b2 := s.ToBytes(slotIdx2)
	b3 := make([]byte, s.Size())
	s.CopyBytes(slotIdx2, b3)

	if slotIdx != slotIdx2 {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b2) {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b3) {
		t.Errorf("Conversion fails")
	}
}

func TestSlotReincValueSerializer(t *testing.T) {
	var s common.SlotReincValueSerializer
	var _ common.Serializer[common.SlotReincValue] = s

	// convert back and forth
	slotValue := common.SlotReincValue{
		Reincarnation: 0x87654321,
		Value:         common.Value{0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x99},
	}
	b := s.ToBytes(slotValue)
	slotValue2 := s.FromBytes(b)
	b2 := s.ToBytes(slotValue2)
	b3 := make([]byte, s.Size())
	s.CopyBytes(slotValue2, b3)

	if slotValue != slotValue2 {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b2) {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b3) {
		t.Errorf("Conversion fails")
	}
}
