package common

import (
	"bytes"
	"testing"
)

var (
	slotA = &SlotIdx[uint32]{uint32(10), uint32(20)}
	slotB = &SlotIdx[uint32]{uint32(30), uint32(40)}
	slotC = &SlotIdx[uint32]{uint32(10), uint32(40)}
)

func TestSlotAddressDifferComparator(t *testing.T) {
	if slotA.Compare(slotA) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotA.Compare(slotB) >= 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotB.Compare(slotA) <= 0 {
		t.Errorf("Wrong comparator error")
	}
}

func TestSlotAddressSameComparator(t *testing.T) {
	if slotC.Compare(slotC) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotA.Compare(slotC) >= 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotC.Compare(slotA) <= 0 {
		t.Errorf("Wrong comparator error")
	}
}

func TestSlotIdx32KeySerializer(t *testing.T) {
	var s SlotIdx32KeySerializer
	var _ Serializer[SlotIdxKey[uint32]] = s

	// convert back and forth
	slotIdx := SlotIdxKey[uint32]{
		AddressIdx: 0x87654321,
		Key:        Key{0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x99},
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
	var s SlotReincValueSerializer
	var _ Serializer[SlotReincValue] = s

	// convert back and forth
	slotValue := SlotReincValue{
		Reincarnation: 0x87654321,
		Value:         Value{0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0x12, 0x99},
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

func TestSlotIdxSerializer32(t *testing.T) {
	var s SlotIdx32Serializer
	var _ Serializer[SlotIdx[uint32]] = s

	// convert back and forth
	slotIdx := SlotIdx[uint32]{
		AddressIdx: 123,
		KeyIdx:     456,
	}
	b := s.ToBytes(slotIdx)
	slotIdx2 := s.FromBytes(b)

	if slotIdx != slotIdx2 {
		t.Errorf("Conversion fails: %x := %x", slotIdx, slotIdx2)
	}
}
