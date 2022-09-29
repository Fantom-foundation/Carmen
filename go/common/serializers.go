package common

// AddressSerializer is a Serializer of the Address type
type AddressSerializer struct{}

func (a AddressSerializer) ToBytes(address Address) []byte {
	return address[:]
}
func (a AddressSerializer) FromBytes(bytes []byte) Address {
	var address Address
	copy(address[:], bytes)
	return address
}
func (a AddressSerializer) Size() int {
	return 20
}

// KeySerializer is a Serializer of the Key type
type KeySerializer struct{}

func (a KeySerializer) ToBytes(key Key) []byte {
	return key[:]
}
func (a KeySerializer) FromBytes(bytes []byte) Key {
	var key Key
	copy(key[:], bytes)
	return key
}
func (a KeySerializer) Size() int {
	return 32
}

// ValueSerializer is a Serializer of the Value type
type ValueSerializer struct{}

func (a ValueSerializer) ToBytes(value Value) []byte {
	return value[:]
}
func (a ValueSerializer) FromBytes(bytes []byte) Value {
	var value Value
	copy(value[:], bytes)
	return value
}
func (a ValueSerializer) Size() int {
	return 32
}

// HashSerializer is a Serializer of the Hash type
type HashSerializer struct{}

func (a HashSerializer) ToBytes(hash Hash) []byte {
	return hash[:]
}
func (a HashSerializer) FromBytes(bytes []byte) Hash {
	var hash Hash
	copy(hash[:], bytes)
	return hash
}
func (a HashSerializer) Size() int {
	return 32
}
