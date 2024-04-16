//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package common_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/exp/rand"
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

func TestSerializers(t *testing.T) {
	loops := rand.Intn(10_000)

	t.Run("TestSerializers_Address", func(t *testing.T) {
		var a common.Address
		const size = 20
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Address](t, a, size, common.AddressSerializer{})
	})

	t.Run("TestSerializers_Key", func(t *testing.T) {
		var a common.Key
		const size = 32
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Key](t, a, size, common.KeySerializer{})
	})

	t.Run("TestSerializers_Value", func(t *testing.T) {
		var a common.Value
		const size = 32
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Value](t, a, size, common.ValueSerializer{})
	})

	t.Run("TestSerializers_Hash", func(t *testing.T) {
		var a common.Hash
		const size = 32
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Hash](t, a, size, common.HashSerializer{})
	})

	t.Run("TestSerializers_AccountState", func(t *testing.T) {
		var a common.AccountState = 253
		testSerializer[common.AccountState](t, a, 1, common.AccountStateSerializer{})
	})

	t.Run("TestSerializers_Balance", func(t *testing.T) {
		var a common.Balance
		const size = common.BalanceSize
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Balance](t, a, size, common.BalanceSerializer{})
	})

	t.Run("TestSerializers_Nonce", func(t *testing.T) {
		var a common.Nonce
		const size = common.NonceSize
		for i := 1; i < loops; i++ {
			a[i%size]++
		}
		testSerializer[common.Nonce](t, a, size, common.NonceSerializer{})
	})

	t.Run("TestSerializers_Identifier32", func(t *testing.T) {
		var a = uint32(loops)
		const size = 4
		testSerializer[uint32](t, a, size, common.Identifier32Serializer{})
	})

	t.Run("TestSerializers_Identifier64", func(t *testing.T) {
		var a = uint64(loops)
		const size = 8
		testSerializer[uint64](t, a, size, common.Identifier64Serializer{})
	})

	t.Run("TestSerializers_SlotIdx32Serializer", func(t *testing.T) {
		var a common.SlotIdx[uint32]
		a.KeyIdx = uint32(loops)
		a.AddressIdx = uint32(loops)
		const size = 4 + 4
		testSerializer[common.SlotIdx[uint32]](t, a, size, common.SlotIdx32Serializer{})
	})

	t.Run("TestSerializers_SlotIdx32KeySerializer", func(t *testing.T) {
		var key common.Key
		const keySize = 32
		for i := 1; i < loops; i++ {
			key[i%keySize]++
		}

		var a common.SlotIdxKey[uint32]
		a.Key = key
		a.AddressIdx = uint32(loops)
		const size = 4
		testSerializer[common.SlotIdxKey[uint32]](t, a, keySize+size, common.SlotIdx32KeySerializer{})
	})

	t.Run("TestSerializers_ReincarnationSerializer", func(t *testing.T) {
		var a = common.Reincarnation(loops)
		const size = 4
		testSerializer[common.Reincarnation](t, a, size, common.ReincarnationSerializer{})
	})
}

func testSerializer[T comparable](t *testing.T, val T, size int, serializer common.Serializer[T]) {
	t.Helper()
	serialized := serializer.ToBytes(val)

	if got, want := serializer.FromBytes(serialized), val; got != want {
		t.Errorf("recovered value do not match: %v != %v", got, want)
	}

	got := make([]byte, size)
	serializer.CopyBytes(val, got)
	if !slices.Equal(got, serialized) {
		t.Errorf("recovered value do not match: %v != %v", got, serialized)
	}

	if got, want := serializer.Size(), size; got != want {
		t.Errorf("sizes do not match: %v != %v", got, want)
	}
}
