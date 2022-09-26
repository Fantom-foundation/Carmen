package common

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

const (
	//LOOPS = 1000000000 // use more loops for performance evaluation
	LOOPS = 10
)

var (
	globalHash = sha256.New()
)

func TestHashingPerformance(t *testing.T) {
	// Create a new hasher every time
	start1 := time.Now()
	var hash1 []byte
	for i := 1; i <= LOOPS; i++ {
		h := sha256.New()
		h.Write(hash1)
		h.Write([]byte{byte(i)})
		hash1 = h.Sum(nil)
	}
	fmt.Printf("Time new hash every time: %dms\n", time.Since(start1).Milliseconds())

	// Create a hasher once
	start2 := time.Now()
	var hash2 []byte
	localHash := sha256.New()
	for i := 1; i <= LOOPS; i++ {
		localHash.Reset()
		localHash.Write(hash2)
		localHash.Write([]byte{byte(i)})
		hash2 = localHash.Sum(nil)
	}
	fmt.Printf("Time single local hash: %dms\n", time.Since(start2).Milliseconds())

	// Create a hasher once
	start3 := time.Now()
	var hash3 []byte
	for i := 1; i <= LOOPS; i++ {
		globalHash.Reset()
		globalHash.Write(hash3)
		globalHash.Write([]byte{byte(i)})
		hash3 = globalHash.Sum(nil)
	}
	fmt.Printf("Time signle global hash: %dms\n", time.Since(start3).Milliseconds())

	if bytes.Compare(hash1, hash2) != 0 || bytes.Compare(hash1, hash3) != 0 {
		t.Errorf("Hashes do not match %s, %s, %s", hash1, hash2, hash3)
	}
}
