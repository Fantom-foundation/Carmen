package common

import (
	"encoding/binary"
	"fmt"
	"math/big"
)

// Serializer allows to convert the type to a slice of bytes and back
type Serializer[T any] interface {
	// ToBytes serialize the type to bytes
	ToBytes(T) []byte
	// FromBytes deserialize the type from bytes
	FromBytes([]byte) T
	// Size provides the size of the type when serialized (bytes)
	Size() int // size in bytes when serialized
}

// Identifier is a type allowing to address an item in the Store.
type Identifier interface {
	uint64 | uint32
}

// Address is an EVM-like account address.
type Address [20]byte

// Key is an EVM-like key of a storage slot.
type Key [32]byte

// Hash is an Ethereum-like hash of a state.
type Hash [32]byte

// Balance is an Ethereum-like account balance
type Balance [16]byte

// Nonce is an Ethereum-like nonce
type Nonce [8]byte

// Value is an Ethereum-like smart contract memory slot
type Value [32]byte

type SlotIdx[I Identifier] struct {
	AddressIdx I
	KeyIdx     I
}

var (
	one        = big.NewInt(1)
	maxBalance = getMaxBalance()
)

func getMaxBalance() *big.Int {
	res := big.NewInt(1)
	res = res.Lsh(one, uint(len(Balance{})*8))
	res = res.Sub(res, one)
	return res
}

// ToBalance converts the provided integer value into balance. The function fails with an error if
//  - the provided integer value is nil
//  - the provided integer value is negative
//  - the provided integer value is > MAX_BALANCE = 2^128-1
func ToBalance(value *big.Int) (res Balance, err error) {
	if value == nil {
		return res, fmt.Errorf("unable to convert nil to a balance")
	}
	if value.Sign() < 0 {
		return res, fmt.Errorf("negative numbers can not be converted to balances, got %v", value)
	}
	if value.Cmp(maxBalance) > 0 {
		return res, fmt.Errorf("value exceeds maximum value of balances: %v > %v", value, maxBalance)
	}
	// Encodes the numeric value into bytes using big-endian byte order.
	value.FillBytes(res[:])
	return
}

// ToBigInt interprets the provide balance as a numeric value and returns it.
func (b *Balance) ToBigInt() *big.Int {
	res := &big.Int{}
	// Interprets bytes in b as a positive integer using big-endion byte order.
	return res.SetBytes(b[:])
}

// ToNonce converts the provided integer into a Nonce. Nonces encode integers in BigEndian byte order.
func ToNonce(value uint64) (res Nonce) {
	binary.BigEndian.PutUint64(res[:], value)
	return
}

// ToUint64 converts the value of a nonce into a integer value.
func (n *Nonce) ToUint64() uint64 {
	return binary.BigEndian.Uint64(n[:])
}
