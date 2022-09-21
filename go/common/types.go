package common

const (
	// HashLength is the expected length of the hash
	HashLength = 32
)

type Serializable interface {
	comparable
	ToBytes() []byte
}

type Identifier interface {
	uint64 | uint32
}

type Hash [32]byte

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
