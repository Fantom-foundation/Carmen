package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math/big"

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

// BalanceSize is the size of Ethereum-like balance.
const BalanceSize = 16

// Balance is an Ethereum-like account balance.
type Balance [BalanceSize]byte

// NonceSize is the size of Ethereum-like nonce.
const NonceSize = 8

// Nonce is an Ethereum-like nonce.
type Nonce [NonceSize]byte

// ValueSize is the size of EVM-like storage slot value.
const ValueSize = 32

// Value is an Ethereum-like smart contract memory slot.
type Value [ValueSize]byte

type SlotIdx[I Identifier] struct {
	AddressIdx I
	KeyIdx     I
}

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

type AddressComparator struct{}

func (c AddressComparator) Compare(a, b *Address) int {
	return a.Compare(b)
}

type KeyComparator struct{}

func (c KeyComparator) Compare(a, b *Key) int {
	return a.Compare(b)
}

type Identifier32Comparator struct{}

func (c Identifier32Comparator) Compare(a, b *SlotIdx[uint32]) int {
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
//   - the provided integer value is nil
//   - the provided integer value is negative
//   - the provided integer value is > MAX_BALANCE = 2^128-1
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

// GetSha256Hash computes the Sha256 hash of the given data.
func GetSha256Hash(data []byte) Hash {
	hasher := sha256.New()
	return GetHash(hasher, data)
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
	h = h*prime + uint64(data[0])
	h = h*prime + uint64(data[1])
	h = h*prime + uint64(data[2])
	h = h*prime + uint64(data[3])
	h = h*prime + uint64(data[4])
	h = h*prime + uint64(data[5])
	h = h*prime + uint64(data[6])
	h = h*prime + uint64(data[7])
	h = h*prime + uint64(data[8])
	h = h*prime + uint64(data[9])
	h = h*prime + uint64(data[10])
	h = h*prime + uint64(data[11])
	h = h*prime + uint64(data[12])
	h = h*prime + uint64(data[13])
	h = h*prime + uint64(data[14])
	h = h*prime + uint64(data[15])
	h = h*prime + uint64(data[16])
	h = h*prime + uint64(data[17])
	h = h*prime + uint64(data[18])
	h = h*prime + uint64(data[19])

	return h
}

type KeyHasher struct{}

// Hash implements non-cryptographical hash to be used in maps
func (s KeyHasher) Hash(data *Key) uint64 {
	// enumerate all indexes for the best performance, even a for-loop adds 25% overhead
	h := uint64(17)
	h = h*prime + uint64(data[0])
	h = h*prime + uint64(data[1])
	h = h*prime + uint64(data[2])
	h = h*prime + uint64(data[3])
	h = h*prime + uint64(data[4])
	h = h*prime + uint64(data[5])
	h = h*prime + uint64(data[6])
	h = h*prime + uint64(data[7])
	h = h*prime + uint64(data[8])
	h = h*prime + uint64(data[9])
	h = h*prime + uint64(data[10])
	h = h*prime + uint64(data[11])
	h = h*prime + uint64(data[12])
	h = h*prime + uint64(data[13])
	h = h*prime + uint64(data[14])
	h = h*prime + uint64(data[15])
	h = h*prime + uint64(data[16])
	h = h*prime + uint64(data[17])
	h = h*prime + uint64(data[18])
	h = h*prime + uint64(data[19])
	h = h*prime + uint64(data[20])
	h = h*prime + uint64(data[21])
	h = h*prime + uint64(data[22])
	h = h*prime + uint64(data[23])
	h = h*prime + uint64(data[24])
	h = h*prime + uint64(data[25])
	h = h*prime + uint64(data[26])
	h = h*prime + uint64(data[27])
	h = h*prime + uint64(data[28])
	h = h*prime + uint64(data[29])
	h = h*prime + uint64(data[30])
	h = h*prime + uint64(data[31])

	return h
}

type SlotIdxHasher struct{}

func (s SlotIdxHasher) Hash(a *SlotIdx[uint32]) uint64 {
	var h uint64 = 17
	var prime uint64 = 31

	h = h*prime + uint64(a.AddressIdx)
	h = h*prime + uint64(a.KeyIdx)

	return h
}

type UInt32Hasher struct{}

func (s UInt32Hasher) Hash(a *uint32) uint64 {
	return uint64(*a)
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
