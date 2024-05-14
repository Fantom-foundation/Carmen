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

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestKeccakC_ProducesSameHashAsGo(t *testing.T) {
	tests := [][]byte{
		nil,
		{},
		{1, 2, 3},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		make([]byte, 128),
		make([]byte, 1024),
	}
	for _, test := range tests {
		want := keccak256_Go(test)
		got := keccak256_C(test)
		if want != got {
			t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
		}
	}
}

func TestKeccakC_AddressSpecializationProducesSameHashAsGenericVersion(t *testing.T) {
	tests := []Address{
		{},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
	}

	// Test each individual bit.
	for i := 0; i < 20*8; i++ {
		addr := Address{}
		addr[i/8] = 1 << i % 8
		tests = append(tests, addr)
	}

	// Add some random inputs as well.
	r := rand.New(rand.NewSource(99))
	for i := 0; i < 10; i++ {
		addr := Address{}
		r.Read(addr[:])
		tests = append(tests, addr)
	}

	t.Run("keccak256_C_Address", func(t *testing.T) {
		t.Parallel()
		for _, test := range tests {
			want := keccak256_Go(test[:])
			got := keccak256_C_Address(test)
			if want != got {
				t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
			}
		}
	})

	t.Run("Keccak256ForAddress", func(t *testing.T) {
		t.Parallel()
		for _, test := range tests {
			want := keccak256_Go(test[:])
			got := Keccak256ForAddress(test)
			if want != got {
				t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
			}
		}
	})
}

func TestKeccakC_KeySpecializationProducesSameHashAsGenericVersion(t *testing.T) {
	tests := []Key{
		{},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2},
	}

	// Test each individual bit.
	for i := 0; i < 32*8; i++ {
		key := Key{}
		key[i/8] = 1 << i % 8
		tests = append(tests, key)
	}

	// Add some random inputs as well.
	r := rand.New(rand.NewSource(99))
	for i := 0; i < 10; i++ {
		key := Key{}
		r.Read(key[:])
		tests = append(tests, key)
	}

	t.Run("keccak256_C_Key", func(t *testing.T) {
		t.Parallel()
		for _, test := range tests {
			want := keccak256_Go(test[:])
			got := keccak256_C_Key(test)
			if want != got {
				t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
			}
		}
	})

	t.Run("Keccak256ForKey", func(t *testing.T) {
		t.Parallel()
		for _, test := range tests {
			want := keccak256_Go(test[:])
			got := Keccak256ForKey(test)
			if want != got {
				t.Errorf("unexpected hash for %v, wanted %v, got %v", test, want, got)
			}
		}
	})
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
		keccak256_Go(data)
	})
}

func BenchmarkKeccakC(b *testing.B) {
	benchmark(b, func(data []byte) {
		keccak256_C(data)
	})
}

func BenchmarkKeccakGoAddressGeneric(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256_Go(addr[:])
	}
}

func BenchmarkKeccakCAddressGeneric(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256_C(addr[:])
	}
}

func BenchmarkKeccakCAddressSpecialized(b *testing.B) {
	addr := Address{}
	for i := 0; i < b.N; i++ {
		keccak256_C_Address(addr)
	}
}

func BenchmarkKeccakGoKeyGeneric(b *testing.B) {
	key := Key{}
	for i := 0; i < b.N; i++ {
		keccak256_Go(key[:])
	}
}

func BenchmarkKeccakCKeyGeneric(b *testing.B) {
	key := Key{}
	for i := 0; i < b.N; i++ {
		keccak256_C(key[:])
	}
}

func BenchmarkKeccakCKeySpecialized(b *testing.B) {
	key := Key{}
	for i := 0; i < b.N; i++ {
		keccak256_C_Key(key)
	}
}
