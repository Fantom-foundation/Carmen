package common

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

var (
	globalHash = sha256.New()
	sink       []byte
)

// numBytes upper bound of number of bytes to hash
const numBytes = 1 << 21

// numKeys upper bound of number of 32B keys to hash
const numKeys = 1 << 12

// step expressed as 2^n exponent
const step = 3

// BenchmarkNewHashEveryLoop hashes a number of bytes and creates a new hash every loop
func BenchmarkNewHashEveryLoop(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("NewHashEveryLoop n %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				h := sha256.New()
				h.Write(data)
				sink = h.Sum(nil)
			}
		})
	}
}

// BenchmarkOneHashAllLoops hashes a number of bytes and creates one local hasher for all loops
func BenchmarkOneHashAllLoops(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		localHash := sha256.New()
		b.Run(fmt.Sprintf("OneHashAllLoops n %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				localHash.Reset()
				localHash.Write(data)
				sink = localHash.Sum(nil)
			}
		})
	}
}

// BenchmarkOneGlobalHash hashes a number of bytes and uses a global variable hasher
func BenchmarkOneGlobalHash(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("OneGlobalHash n %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				globalHash.Reset()
				globalHash.Write(data)
				sink = globalHash.Sum(nil)
			}
		})
	}
}

// BenchmarkHashKeyChain hashes a number of keys
func BenchmarkHashKeyChain(b *testing.B) {
	for i := 1; i <= numKeys; i = i << step {
		keys := make([]Key, i)
		hash := make([]byte, len(Hash{}))
		localHash := sha256.New()
		b.Run(fmt.Sprintf("HashKeyChain n %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				for _, key := range keys {
					localHash.Reset()
					localHash.Write(hash)
					localHash.Write(key[:])
					hash = localHash.Sum(nil)
				}
				sink = hash
			}
		})
	}
}
