package state

type Address [20]byte

func (a Address) ToBytes() []byte {
	return a[:]
}
func (a *Address) SetBytes(bytes []byte) bool {
	return copy(a[:], bytes) == 20
}
func (a *Address) Size() int {
	return 20
}

type Key [32]byte

func (k Key) ToBytes() []byte {
	return k[:]
}
func (k *Key) SetBytes(bytes []byte) bool {
	return copy(k[:], bytes) == 32
}
func (k *Key) Size() int {
	return 32
}

type Value [32]byte

func (v Value) ToBytes() []byte {
	return v[:]
}
func (v *Value) SetBytes(bytes []byte) bool {
	return copy(v[:], bytes) == 32
}
func (v *Value) Size() int {
	return 32
}

type State interface {
	GetBalance(Address)
	GetNonce(address Address) uint64
	GetStorage(address Address, key Key) Value
}
