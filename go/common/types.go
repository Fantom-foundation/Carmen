package common

const (
	// HashLength is the expected length of the hash
	HashLength = 32
)

type Serializer[T any] interface {
	ToBytes(T) []byte
	SetBytes([]byte) T
	Size() int // size in bytes when serialized
}

type Identifier interface {
	uint64 | uint32
}

type Address [20]byte
type Key [32]byte
type Value [32]byte

type Hash [HashLength]byte

// BytesToHash sets b to hash.
// If b is larger than len(h), b will be cropped from the left.
func BytesToHash(b []byte) Hash {
	var h Hash
	h.SetBytes(b)
	return h
}

func (h *Hash) SetBytes(b []byte) {
	copy(h[HashLength-len(b):], b)
}

// Bytes gets the byte representation of the underlying hash.
func (h Hash) Bytes() []byte { return h[:] }
