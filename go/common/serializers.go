package common

type AddressSerializer struct{}

func (a AddressSerializer) ToBytes(address Address) []byte {
	return address[:]
}
func (a AddressSerializer) SetBytes(bytes []byte) Address {
	var address Address
	copy(address[:], bytes)
	return address
}
func (a AddressSerializer) Size() int {
	return 20
}

type KeySerializer struct{}

func (a KeySerializer) ToBytes(address Key) []byte {
	return address[:]
}
func (a KeySerializer) SetBytes(bytes []byte) Key {
	var key Key
	copy(key[:], bytes)
	return key
}
func (a KeySerializer) Size() int {
	return 32
}

type ValueSerializer struct{}

func (a ValueSerializer) ToBytes(address Value) []byte {
	return address[:]
}
func (a ValueSerializer) SetBytes(bytes []byte) Value {
	var value Value
	copy(value[:], bytes)
	return value
}
func (a ValueSerializer) Size() int {
	return 32
}
