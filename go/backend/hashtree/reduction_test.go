package hashtree

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func getSha256Hash(data []byte) common.Hash {
	h := sha256.New()
	h.Write(data)
	var hash common.Hash
	h.Sum(hash[0:0])
	return hash
}

func reduceHashes(branchingFactor int, hashes []common.Hash) common.Hash {
	hash, err := ReduceHashes(branchingFactor, len(hashes), func(i int) (common.Hash, error) {
		return hashes[i], nil
	})
	if err != nil {
		panic(fmt.Sprintf("an error was produced where none should be allowed: %v", err))
	}
	return hash
}

func TestKnownHashes(t *testing.T) {
	// Tests the hashes for values 0x00, 0x11 ... 0x44 inserted in sequence.
	// reference hashes from the C++ implementation
	expectedHashes := []string{
		"0000000000000000000000000000000000000000000000000000000000000000",
		"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
		"6c5f701c7f179fe2f65970ec7105d8e5c156c94fdf5aaaa9583be12473c89f0f",
		"d8474951058dfc020b3d9b62b06528130543884b9520a7542be52a2f6344cad4",
		"3cd329e823a238ba7897f2ad62aeea1435a10999cbed87bec7fdd410a93d7096",
		"1964521cc4a514e44c4395c9a23ee88263b4463717d43b0c57808867bcabfd4f",
	}

	hashes := []common.Hash{}
	for i, expected := range expectedHashes {
		if hash := reduceHashes(3, hashes); fmt.Sprintf("%x", hash) != expected {
			t.Errorf("invalid hash for step %d, got %x, wanted %v", len(hashes), hash, expectedHashes[i])
		}
		hashes = append(hashes, getSha256Hash([]byte{byte(i<<4 | i)}))
	}
}
