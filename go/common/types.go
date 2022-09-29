package common

const (
	// HashLength is the expected length of the hash
	HashLength = 32
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

// Address is an EVM-compatible account address.
type Address [20]byte

// Key is an EVM-compatible key of a storage slot.
type Key [32]byte

// Value is an EVM-compatible value of a storage slot.
type Value [32]byte

// Hash is an Ethereum-compatible hash of a state.
type Hash [HashLength]byte
