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

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	"golang.org/x/crypto/sha3"
)

// Serializer allows to convert the type to a slice of bytes and back
type Serializer[T any] interface {
	// ToBytes serialize the type to bytes
	ToBytes(T) []byte
	// CopyBytes serialize the type into a provided slice
	CopyBytes(T, []byte)
	// FromBytes deserialize the type from bytes
	FromBytes([]byte) T
	// Size provides the size of the type when serialized (bytes)
	Size() int // size in bytes when serialized
}

// PageSize of 4kB I/O efficient
const PageSize = 1 << 12

// Comparator is an interface for comparing two items
type Comparator[T any] interface {
	Compare(a, b *T) int
}

type HashProvider interface {
	GetStateHash() (Hash, error)
}

// Identifier is a type allowing to address an item in the Store.
type Identifier interface {
	uint64 | uint32
}

// AddressSize is the size of Ethereum-like address.
const AddressSize = 20

// Address is an EVM-like account address.
type Address [AddressSize]byte

// KeySize is the size of EVM-like storage slot key.
const KeySize = 32

// Key is an EVM-like key of a storage slot.
type Key [KeySize]byte

// HashSize is the byte-size of the Hash type
const HashSize = 32

// Hash is an Ethereum-like hash of a state.
type Hash [HashSize]byte

// NonceSize is the size of Ethereum-like nonce.
const NonceSize = 8

// Nonce is an Ethereum-like nonce.
type Nonce [NonceSize]byte

// ValueSize is the size of EVM-like storage slot value.
const ValueSize = 32

// Value is an Ethereum-like smart contract memory slot.
type Value [ValueSize]byte

// AccountState is the base type of account states enum.
type AccountState byte

const (
	// Unknown is the state of an unknown account (=default value).
	Unknown AccountState = 0
	// Exists is the state of an open account.
	Exists AccountState = 1
)

func (s AccountState) String() string {
	switch s {
	case Unknown:
		return "unknown"
	case Exists:
		return "exists"
	}
	return "invalid"
}

func (a *Address) Compare(b *Address) int {
	return bytes.Compare(a[:], b[:])
}

func (a *Key) Compare(b *Key) int {
	return bytes.Compare(a[:], b[:])
}

func (a *Hash) Compare(b *Hash) int {
	return bytes.Compare(a[:], b[:])
}

type AddressComparator struct{}

func (c AddressComparator) Compare(a, b *Address) int {
	return a.Compare(b)
}

type KeyComparator struct{}

func (c KeyComparator) Compare(a, b *Key) int {
	return a.Compare(b)
}

type HashComparator struct{}

func (c HashComparator) Compare(a, b *Hash) int {
	return a.Compare(b)
}

type Uint32Comparator struct{}

func (c Uint32Comparator) Compare(a, b *uint32) int {
	if *a > *b {
		return 1
	}
	if *a < *b {
		return -1
	}

	return 0
}

type Uint64Comparator struct{}

func (c Uint64Comparator) Compare(a, b *uint64) int {
	if *a > *b {
		return 1
	}
	if *a < *b {
		return -1
	}

	return 0
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

// GetKeccak256Hash computes the Keccak256 hash of the given data.
func GetKeccak256Hash(data []byte) Hash {
	hasher := sha3.NewLegacyKeccak256()
	return GetHash(hasher, data)
}

// GetHash computes the hash of the given data using the given hashing aglorithm.
func GetHash(h hash.Hash, data []byte) (res Hash) {
	h.Reset()
	h.Write(data)
	copy(res[:], h.Sum(nil)[:])
	return
}

const prime = 31

type AddressHasher struct{}

// Hash implements non-cryptographical hash to be used in maps
func (s AddressHasher) Hash(data *Address) uint64 {
	// enumerate all indexes for the best performance, even a for-loop adds 25% overhead
	h := uint64(17)
	h = h*prime + binary.BigEndian.Uint64(data[0:8])
	h = h*prime + binary.BigEndian.Uint64(data[8:16])
	h = h*prime + uint64(binary.BigEndian.Uint32(data[16:20]))

	return h
}

type KeyHasher struct{}

// Hash implements non-cryptographical hash to be used in maps
func (s KeyHasher) Hash(data *Key) uint64 {
	// enumerate all indexes for the best performance, even a for-loop adds 25% overhead
	h := uint64(17)
	h = h*prime + binary.BigEndian.Uint64(data[0:8])
	h = h*prime + binary.BigEndian.Uint64(data[8:16])
	h = h*prime + binary.BigEndian.Uint64(data[16:24])
	h = h*prime + binary.BigEndian.Uint64(data[24:32])

	return h
}

type UInt32Hasher struct{}

func (s UInt32Hasher) Hash(a *uint32) uint64 {
	return uint64(*a)
}

type HashHasher struct{}

func (s HashHasher) Hash(data *Hash) uint64 {
	h := uint64(17)
	h = h*prime + binary.BigEndian.Uint64(data[0:8])
	h = h*prime + binary.BigEndian.Uint64(data[8:16])
	h = h*prime + binary.BigEndian.Uint64(data[16:24])
	h = h*prime + binary.BigEndian.Uint64(data[24:32])

	return h
}

func (h Hash) ToBytes() []byte {
	return h[:]
}

func (a Address) String() string {
	return fmt.Sprintf("%x", a[:])
}

func (a Key) String() string {
	return fmt.Sprintf("%x", a[:])
}

// HashFromString converst a 64-character long hex string into a hash.
// The operation is slow and mainly intended for producing readable test
// cases. The operation will panic if the provided hash is mailformed.
func HashFromString(str string) Hash {
	if len(str) != 64 {
		panic(fmt.Sprintf("invalid hash-string length, expected %d, got %d", 64, len(str)))
	}
	bytes, err := hex.DecodeString(str)
	if err != nil {
		panic(fmt.Sprintf("invalid hex string `%s`: %v", str, err))
	}
	res := Hash{}
	copy(res[:], bytes)
	return res
}

func AddressFromNumber(num int) (address Address) {
	addr := binary.BigEndian.AppendUint32([]byte{}, uint32(num))
	copy(address[:], addr)
	return
}
