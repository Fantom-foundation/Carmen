package mpt

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func BenchmarkHashCache_Misses(b *testing.B) {
	hasher := NewKeyHasher()
	key := common.Key{}
	for i := 0; i < b.N; i++ {
		key = common.Key{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		hasher.Get(&key)
	}
	fmt.Printf("hits: %d, misses: %d\n", hasher.hits.Load(), hasher.misses.Load())
}

func BenchmarkHashCache_Hits(b *testing.B) {
	hasher := NewKeyHasher()
	key := common.Key{}
	for i := 0; i < b.N; i++ {
		hasher.Get(&key)
	}
	fmt.Printf("hits: %d, misses: %d\n", hasher.hits.Load(), hasher.misses.Load())
}
