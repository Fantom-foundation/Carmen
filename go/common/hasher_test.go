package common

import (
	"crypto/sha256"
	"testing"
)

const (
	LOOPS = 100000000
)

// run with e.g.  "-test.count 3" not tu run forever

var (
	globalHash = sha256.New()
)

func BenchmarkNewHashEveryLoop(t *testing.B) {
	// Create a new hasher every time
	var hash1 []byte
	for i := 1; i <= LOOPS; i++ {
		h := sha256.New()
		h.Write(hash1)
		h.Write([]byte{byte(i)})
		hash1 = h.Sum(nil)
	}
}

func BenchmarkOneHashAllLoops(t *testing.B) {
	// Create a hasher once
	var hash2 []byte
	localHash := sha256.New()
	for i := 1; i <= LOOPS; i++ {
		localHash.Reset()
		localHash.Write(hash2)
		localHash.Write([]byte{byte(i)})
		hash2 = localHash.Sum(nil)
	}
}

func BenchmarkOneGlobalHash(t *testing.B) {
	// Create a hasher once
	var hash3 []byte
	for i := 1; i <= LOOPS; i++ {
		globalHash.Reset()
		globalHash.Write(hash3)
		globalHash.Write([]byte{byte(i)})
		hash3 = globalHash.Sum(nil)
	}
}
