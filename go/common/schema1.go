package common

import "encoding/binary"

// SlotIdx1 represents an index into slotIndex for schema1
type SlotIdx1[I Identifier] struct {
	AddressIdx I
	KeyIdx     I
}

// Compare slots first by the address and then by the key if the addresses are the same.
// It returns zero when both addresses and keys are the same
// otherwise it returns a negative number when A is lower than B
// or a positive number when A is higher than B.
func (a *SlotIdx1[I]) Compare(b *SlotIdx1[I]) int {
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

func (c SlotIdx32Comparator) Compare(a, b *SlotIdx1[uint32]) int {
	return a.Compare(b)
}

type SlotIdx32Hasher struct{}

func (s SlotIdx32Hasher) Hash(a *SlotIdx1[uint32]) uint64 {
	var h uint64 = 17
	var prime uint64 = 31

	h = h*prime + uint64(a.AddressIdx)
	h = h*prime + uint64(a.KeyIdx)

	return h
}

// SlotIdxSerializer32 is a Serializer of the SlotIdx[uint32] type
type SlotIdxSerializer32 struct {
	identifierSerializer32 Identifier32Serializer
}

func (a SlotIdxSerializer32) ToBytes(value SlotIdx1[uint32]) []byte {
	res := make([]byte, 0, 8)
	res = binary.LittleEndian.AppendUint32(res, value.AddressIdx)
	res = binary.LittleEndian.AppendUint32(res, value.KeyIdx)
	return res
}
func (a SlotIdxSerializer32) CopyBytes(value SlotIdx1[uint32], out []byte) {
	binary.LittleEndian.PutUint32(out[0:4], value.AddressIdx)
	binary.LittleEndian.PutUint32(out[4:8], value.KeyIdx)
}
func (a SlotIdxSerializer32) FromBytes(bytes []byte) SlotIdx1[uint32] {
	value := SlotIdx1[uint32]{
		AddressIdx: binary.LittleEndian.Uint32(bytes[0:4]),
		KeyIdx:     binary.LittleEndian.Uint32(bytes[4:8]),
	}
	return value
}
func (a SlotIdxSerializer32) Size() int {
	return 8 // two 32bit integers
}
