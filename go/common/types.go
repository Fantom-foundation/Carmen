package common

type Serializable interface {
	ToBytes() []byte
}

type Identifier interface {
	uint64 | uint32
}

type Hash [32]byte

type StringSerializable string

func (str StringSerializable) ToBytes() []byte {
	return str.ToBytes()
}
