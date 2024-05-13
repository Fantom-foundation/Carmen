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

// SlotReincValue represents a value into valuesStore for schema3
type SlotReincValue struct {
	Reincarnation
	Value
}

type SlotReincValueSerializer struct {
	ReincarnationSerializer Identifier32Serializer
	ValueSerializer
}

func (a SlotReincValueSerializer) ToBytes(value SlotReincValue) []byte {
	res := make([]byte, 4+32)
	binary.LittleEndian.PutUint32(res[0:4], uint32(value.Reincarnation))
	copy(res[4:4+32], value.Value[:])
	return res
}
func (a SlotReincValueSerializer) CopyBytes(value SlotReincValue, out []byte) {
	binary.LittleEndian.PutUint32(out[0:4], uint32(value.Reincarnation))
	copy(out[4:4+32], value.Value[:])
}
func (a SlotReincValueSerializer) FromBytes(bytes []byte) SlotReincValue {
	value := SlotReincValue{
		Reincarnation: Reincarnation(binary.LittleEndian.Uint32(bytes[0:4])),
		Value:         *(*Value)(bytes[4 : 4+32]),
	}
	return value
}
func (a SlotReincValueSerializer) Size() int {
	return 4 + 32
}

// Reincarnation is a type for the reincarnation counter
type Reincarnation uint32

// ReincarnationSerializer is a Serializer of the uint32 reincarnation type
type ReincarnationSerializer struct{}

func (a ReincarnationSerializer) ToBytes(value Reincarnation) []byte {
	return binary.LittleEndian.AppendUint32([]byte{}, uint32(value))
}
func (a ReincarnationSerializer) CopyBytes(value Reincarnation, out []byte) {
	binary.LittleEndian.PutUint32(out, uint32(value))
}
func (a ReincarnationSerializer) FromBytes(bytes []byte) Reincarnation {
	return Reincarnation(binary.LittleEndian.Uint32(bytes))
}
func (a ReincarnationSerializer) Size() int {
	return 4
}
