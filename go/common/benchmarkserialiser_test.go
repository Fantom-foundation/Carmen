package common

import "testing"

var addressSink Address
var addressPointerSink *Address

func BenchmarkFromBytesSerializer(b *testing.B) {
	serializer := AddressSerializerVariants{}
	address := make([]byte, 20)
	for i := 1; i <= b.N; i++ {
		address[0] = byte(i)
		addressSink = serializer.FromBytes(address)
	}
}

func BenchmarkCastBytesSerializer(b *testing.B) {
	serializer := AddressSerializerVariants{}
	address := make([]byte, 20)
	for i := 1; i <= b.N; i++ {
		address[0] = byte(i)
		addressSink = serializer.Cast(address)
	}
}

func BenchmarkFromBytesPtrSerializer(b *testing.B) {
	serializer := AddressSerializerVariants{}
	address := make([]byte, 20)
	for i := 1; i <= b.N; i++ {
		address[0] = byte(i)
		addressPointerSink = serializer.FromBytesPtr(address)
	}
}

func BenchmarkCastPtrBytesSerializer(b *testing.B) {
	serializer := AddressSerializerVariants{}
	address := make([]byte, 20)
	for i := 1; i <= b.N; i++ {
		address[0] = byte(i)
		addressPointerSink = serializer.CastPtr(address)
	}
}

type AddressSerializerVariants struct{}

func (a AddressSerializerVariants) FromBytes(bytes []byte) Address {
	var address Address
	copy(address[:], bytes)
	return address
}
func (a AddressSerializerVariants) FromBytesPtr(bytes []byte) *Address {
	var address Address
	copy(address[:], bytes)
	return &address
}

func (a AddressSerializerVariants) Cast(bytes []byte) Address {
	return *(*Address)(bytes)
}
func (a AddressSerializerVariants) CastPtr(bytes []byte) *Address {
	return (*Address)(bytes)
}
