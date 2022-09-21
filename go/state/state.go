package state

type Address [20]byte
type Key [32]byte
type Value [32]byte

type State interface {
	GetBalance(Address)
	GetNonce(address Address) uint64
	GetStorage(address Address, key Key) Value
}
