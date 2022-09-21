package common

type Serializable interface {
	ToBytes() []byte
}

type Identifier interface {
	uint64 | uint32
}

type Hash [32]byte
