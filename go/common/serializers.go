package common

import "encoding/binary"

// AddressSerializer is a Serializer of the Address type
type AddressSerializer struct{}

func (a AddressSerializer) ToBytes(address Address) []byte {
	return address[:]
}
func (a AddressSerializer) FromBytes(bytes []byte) Address {
	var address Address
	copy(address[:], bytes)
	return address
}
func (a AddressSerializer) Size() int {
	return 20
}

// KeySerializer is a Serializer of the Key type
type KeySerializer struct{}

func (a KeySerializer) ToBytes(key Key) []byte {
	return key[:]
}
func (a KeySerializer) FromBytes(bytes []byte) Key {
	var key Key
	copy(key[:], bytes)
	return key
}
func (a KeySerializer) Size() int {
	return 32
}

// ValueSerializer is a Serializer of the Value type
type ValueSerializer struct{}

func (a ValueSerializer) ToBytes(value Value) []byte {
	return value[:]
}
func (a ValueSerializer) FromBytes(bytes []byte) Value {
	var value Value
	copy(value[:], bytes)
	return value
}
func (a ValueSerializer) Size() int {
	return 32
}

// HashSerializer is a Serializer of the Hash type
type HashSerializer struct{}

func (a HashSerializer) ToBytes(hash Hash) []byte {
	return hash[:]
}
func (a HashSerializer) FromBytes(bytes []byte) Hash {
	var hash Hash
	copy(hash[:], bytes)
	return hash
}
func (a HashSerializer) Size() int {
	return 32
}

// BalanceSerializer is a Serializer of the Value type
type BalanceSerializer struct{}

func (a BalanceSerializer) ToBytes(value Balance) []byte {
	return value[:]
}
func (a BalanceSerializer) FromBytes(bytes []byte) Balance {
	var value Balance
	copy(value[:], bytes)
	return value
}
func (a BalanceSerializer) Size() int {
	return 32
}

// NonceSerializer is a Serializer of the Value type
type NonceSerializer struct{}

func (a NonceSerializer) ToBytes(value Nonce) []byte {
	return value[:]
}
func (a NonceSerializer) FromBytes(bytes []byte) Nonce {
	var value Nonce
	copy(value[:], bytes)
	return value
}
func (a NonceSerializer) Size() int {
	return 32
}

// SlotIdxSerializer32 is a Serializer of the Value type
type SlotIdxSerializer32 struct {
	identifierSerializer32 Identifier32Serializer
}

func (a SlotIdxSerializer32) ToBytes(value SlotIdx[uint32]) []byte {
	b1 := a.identifierSerializer32.ToBytes(value.AddressIdx)
	b2 := a.identifierSerializer32.ToBytes(value.KeyIdx)
	return append(b1, b2...)
}
func (a SlotIdxSerializer32) FromBytes(bytes []byte) SlotIdx[uint32] {
	value := SlotIdx[uint32]{
		AddressIdx: a.identifierSerializer32.FromBytes(bytes[0:4]),
		KeyIdx:     a.identifierSerializer32.FromBytes(bytes[4:8]),
	}
	return value
}
func (a SlotIdxSerializer32) Size() int {
	return 8 // two 32bit integers
}

// Identifier32Serializer is a Serializer of the uint32 Identifier type
type Identifier32Serializer struct{}

func (a Identifier32Serializer) ToBytes(value uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{}, value)
}
func (a Identifier32Serializer) FromBytes(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}
func (a Identifier32Serializer) Size() int {
	return 4
}
