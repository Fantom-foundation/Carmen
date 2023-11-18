package common

import (
	"fmt"
	"testing"
)

func TestKeccakCpp_ProducesSameHashAsGo(t *testing.T) {
	tests := [][]byte{
		nil,
		[]byte{},
		[]byte{1, 2, 3},
		make([]byte, 128),
		make([]byte, 1024),
	}
	for _, test := range tests {
		want := keccak256Go(test)
		got := keccak256Cpp(test)
		if want != got {
			t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
		}
	}
}

func TestKeccakCpp_AddressSpecializationProducesSameHash(t *testing.T) {
	tests := []Address{
		{},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, test := range tests {
		want := keccak256Cpp(test[:])
		got := keccak256AddressCpp(test)
		if want != got {
			t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
		}
	}
}
func benchmark(b *testing.B, hasher func([]byte)) {
	for i := 1; i < 1<<22; i <<= 3 {
		b.Run(fmt.Sprintf("size=%d", i), func(b *testing.B) {
			data := make([]byte, i)
			for i := 0; i < b.N; i++ {
				hasher(data)
			}
		})
	}
}

func BenchmarkKeccakGo(b *testing.B) {
	benchmark(b, func(data []byte) {
		keccak256Go(data)
	})
}

func BenchmarkKeccakCpp(b *testing.B) {
	benchmark(b, func(data []byte) {
		keccak256Cpp(data)
	})
}

func BenchmarkKeccakGoAddressGeneric(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256Cpp(addr[:])
	}
}

func BenchmarkKeccakCppAddressGeneric(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256Cpp(addr[:])
	}
}

func BenchmarkKeccakCppAddressSpecialized(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256AddressCpp(addr)
	}
}
