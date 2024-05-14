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

// SlotIdxKey represents an index into slotIndex for schema3
type SlotIdxKey[I Identifier] struct {
	AddressIdx I
	Key        Key
}

// Compare slots first by the address and then by the key if the addresses are the same.
// It returns zero when both addresses and keys are the same
// otherwise it returns a negative number when A is lower than B
// or a positive number when A is higher than B.
func (a *SlotIdxKey[I]) Compare(b *SlotIdxKey[I]) int {
	if a.AddressIdx > b.AddressIdx {
		return 1
	}
	if a.AddressIdx < b.AddressIdx {
		return -1
	}

	return a.Key.Compare(&b.Key)
}

type SlotIdx32KeyComparator struct{}

func (c SlotIdx32KeyComparator) Compare(a, b *SlotIdxKey[uint32]) int {
	return a.Compare(b)
}

type SlotIdx32KeyHasher struct {
	KeyHasher
}

func (s SlotIdx32KeyHasher) Hash(a *SlotIdxKey[uint32]) uint64 {
	var h uint64 = 17
	var prime uint64 = 31

	h = h*prime + uint64(a.AddressIdx)
	h = h*prime + s.KeyHasher.Hash(&a.Key)

	return h
}

// SlotIdx32KeySerializer is a Serializer of the SlotIdx[uint32] type
type SlotIdx32KeySerializer struct{}

func (a SlotIdx32KeySerializer) ToBytes(value SlotIdxKey[uint32]) []byte {
	res := make([]byte, 4+32)
	binary.LittleEndian.PutUint32(res[0:4], value.AddressIdx)
	copy(res[4:4+32], value.Key[:])
	return res
}
func (a SlotIdx32KeySerializer) CopyBytes(value SlotIdxKey[uint32], out []byte) {
	binary.LittleEndian.PutUint32(out[0:4], value.AddressIdx)
	copy(out[4:4+32], value.Key[:])
}
func (a SlotIdx32KeySerializer) FromBytes(bytes []byte) SlotIdxKey[uint32] {
	value := SlotIdxKey[uint32]{
		AddressIdx: binary.LittleEndian.Uint32(bytes[0:4]),
		Key:        *(*Key)(bytes[4 : 4+32]),
	}
	return value
}
func (a SlotIdx32KeySerializer) Size() int {
	return 4 + 32 // 32bit integer + Key
}
