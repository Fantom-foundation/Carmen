package state

type Address [20]byte

func (a Address) ToBytes() []byte {
	return a[:]
}

type Key [32]byte

func (k Key) ToBytes() []byte {
	return k[:]
}

type Value [32]byte

func (v Value) ToBytes() []byte {
	return v[:]
}

type State interface {
	GetBalance(Address)
	GetNonce(address Address) uint64
	GetStorage(address Address, key Key) Value
}
