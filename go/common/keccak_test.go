package common

import (
	"fmt"
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
