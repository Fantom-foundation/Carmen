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
type SlotIdxSerializer32 struct{}

func (a SlotIdxSerializer32) ToBytes(value SlotIdx[uint32]) []byte {
	b := binary.LittleEndian.AppendUint32([]byte{}, value.AddressIdx)
	return binary.LittleEndian.AppendUint32(b, value.KeyIdx)
}
func (a SlotIdxSerializer32) FromBytes(bytes []byte) SlotIdx[uint32] {
	value := SlotIdx[uint32]{
		AddressIdx: binary.LittleEndian.Uint32(bytes[0:4]),
		KeyIdx:     binary.LittleEndian.Uint32(bytes[4:8]),
	}
	return value
}
func (a SlotIdxSerializer32) Size() int {
	return 8 // two 32bit integers
}
