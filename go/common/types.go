package common

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
type Balance [32]byte

// Nonce is an Ethereum-like nonce
type Nonce [32]byte

// Value is an Ethereum-like smart contract memory slot
type Value [32]byte
