// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

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
