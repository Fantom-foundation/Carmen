package common

type Serializable interface {
	ToBytes() []byte
	SetBytes([]byte) bool
	Size() int // size in bytes when serialized
}

type Identifier interface {
	uint64 | uint32
}

type Hash [32]byte
