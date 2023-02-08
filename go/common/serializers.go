package common

import "encoding/binary"

// AddressSerializer is a Serializer of the Address type
type AddressSerializer struct{}

func (a AddressSerializer) ToBytes(address Address) []byte {
	return address[:]
}
func (a AddressSerializer) CopyBytes(address Address, out []byte) {
	copy(out, address[:])
}
func (a AddressSerializer) FromBytes(bytes []byte) Address {
	return *(*Address)(bytes)
}

func (a AddressSerializer) Size() int {
	return AddressSize
}

// KeySerializer is a Serializer of the Key type
type KeySerializer struct{}

func (a KeySerializer) ToBytes(key Key) []byte {
	return key[:]
}
func (a KeySerializer) CopyBytes(key Key, out []byte) {
	copy(out, key[:])
}
func (a KeySerializer) FromBytes(bytes []byte) Key {
	return *(*Key)(bytes)
}
func (a KeySerializer) Size() int {
	return KeySize
}

// ValueSerializer is a Serializer of the Value type
type ValueSerializer struct{}

func (a ValueSerializer) ToBytes(value Value) []byte {
	return value[:]
}
func (a ValueSerializer) CopyBytes(value Value, out []byte) {
	copy(out, value[:])
}
func (a ValueSerializer) FromBytes(bytes []byte) Value {
	return *(*Value)(bytes)
}
func (a ValueSerializer) Size() int {
	return ValueSize
}

// HashSerializer is a Serializer of the Hash type
type HashSerializer struct{}

func (a HashSerializer) ToBytes(hash Hash) []byte {
	return hash[:]
}
func (a HashSerializer) CopyBytes(hash Hash, out []byte) {
	copy(out, hash[:])
}
func (a HashSerializer) FromBytes(bytes []byte) Hash {
	return *(*Hash)(bytes)
}
func (a HashSerializer) Size() int {
	return HashSize
}

// AccountStateSerializer is a Serializer of the AccountState type
type AccountStateSerializer struct{}

func (a AccountStateSerializer) ToBytes(value AccountState) []byte {
	return []byte{byte(value)}
}
func (a AccountStateSerializer) CopyBytes(value AccountState, out []byte) {
	out[0] = byte(value)
}
func (a AccountStateSerializer) FromBytes(bytes []byte) AccountState {
	return AccountState(bytes[0])
}
func (a AccountStateSerializer) Size() int {
	return 1
}

// BalanceSerializer is a Serializer of the Balance type
type BalanceSerializer struct{}

func (a BalanceSerializer) ToBytes(value Balance) []byte {
	return value[:]
}
func (a BalanceSerializer) CopyBytes(value Balance, out []byte) {
	copy(out, value[:])
}
func (a BalanceSerializer) FromBytes(bytes []byte) Balance {
	return *(*Balance)(bytes)
}
func (a BalanceSerializer) Size() int {
	return BalanceSize
}

// NonceSerializer is a Serializer of the Nonce type
type NonceSerializer struct{}

func (a NonceSerializer) ToBytes(value Nonce) []byte {
	return value[:]
}
func (a NonceSerializer) CopyBytes(value Nonce, out []byte) {
	copy(out, value[:])
}
func (a NonceSerializer) FromBytes(bytes []byte) Nonce {
	return *(*Nonce)(bytes)
}
func (a NonceSerializer) Size() int {
	return NonceSize
}

// SlotIdxSerializer32 is a Serializer of the SlotIdx[uint32] type
type SlotIdxSerializer32 struct {
	identifierSerializer32 Identifier32Serializer
}

func (a SlotIdxSerializer32) ToBytes(value SlotIdx[uint32]) []byte {
	res := make([]byte, 0, 8)
	res = binary.LittleEndian.AppendUint32(res, value.AddressIdx)
	res = binary.LittleEndian.AppendUint32(res, value.KeyIdx)
	return res
}
func (a SlotIdxSerializer32) CopyBytes(value SlotIdx[uint32], out []byte) {
	binary.LittleEndian.PutUint32(out[0:4], value.AddressIdx)
	binary.LittleEndian.PutUint32(out[4:8], value.KeyIdx)
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

// Identifier32Serializer is a Serializer of the uint32 Identifier type
type Identifier32Serializer struct{}

func (a Identifier32Serializer) ToBytes(value uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{}, value)
}
func (a Identifier32Serializer) CopyBytes(value uint32, out []byte) {
	binary.BigEndian.PutUint32(out, value)
}
func (a Identifier32Serializer) FromBytes(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}
func (a Identifier32Serializer) Size() int {
	return 4
}

// Identifier64Serializer is a Serializer of the uint64 Identifier type
type Identifier64Serializer struct{}

func (a Identifier64Serializer) ToBytes(value uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{}, value)
}
func (a Identifier64Serializer) CopyBytes(value uint64, out []byte) {
	binary.BigEndian.PutUint64(out, value)
}
func (a Identifier64Serializer) FromBytes(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}
func (a Identifier64Serializer) Size() int {
	return 8
}
