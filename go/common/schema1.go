// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import "encoding/binary"

// SlotIdx represents an index into slotIndex for schema1
type SlotIdx[I Identifier] struct {
	AddressIdx I
	KeyIdx     I
}

// Compare slots first by the address and then by the key if the addresses are the same.
// It returns zero when both addresses and keys are the same
// otherwise it returns a negative number when A is lower than B
// or a positive number when A is higher than B.
func (a *SlotIdx[I]) Compare(b *SlotIdx[I]) int {
	if a.AddressIdx > b.AddressIdx {
		return 1
	}
	if a.AddressIdx < b.AddressIdx {
		return -1
	}

	if a.KeyIdx > b.KeyIdx {
		return 1
	}
	if a.KeyIdx < b.KeyIdx {
		return -1
	}

	return 0
}

type SlotIdx32Comparator struct{}

func (c SlotIdx32Comparator) Compare(a, b *SlotIdx[uint32]) int {
	return a.Compare(b)
}

type SlotIdx32Hasher struct{}

func (s SlotIdx32Hasher) Hash(a *SlotIdx[uint32]) uint64 {
	var h uint64 = 17
	var prime uint64 = 31

	h = h*prime + uint64(a.AddressIdx)
	h = h*prime + uint64(a.KeyIdx)

	return h
}

// SlotIdx32Serializer is a Serializer of the SlotIdx[uint32] type
type SlotIdx32Serializer struct{}

func (a SlotIdx32Serializer) ToBytes(value SlotIdx[uint32]) []byte {
	res := make([]byte, 0, 8)
	res = binary.LittleEndian.AppendUint32(res, value.AddressIdx)
	res = binary.LittleEndian.AppendUint32(res, value.KeyIdx)
	return res
}
func (a SlotIdx32Serializer) CopyBytes(value SlotIdx[uint32], out []byte) {
	binary.LittleEndian.PutUint32(out[0:4], value.AddressIdx)
	binary.LittleEndian.PutUint32(out[4:8], value.KeyIdx)
}
func (a SlotIdx32Serializer) FromBytes(bytes []byte) SlotIdx[uint32] {
	value := SlotIdx[uint32]{
		AddressIdx: binary.LittleEndian.Uint32(bytes[0:4]),
		KeyIdx:     binary.LittleEndian.Uint32(bytes[4:8]),
	}
	return value
}
func (a SlotIdx32Serializer) Size() int {
	return 8 // two 32bit integers
}
