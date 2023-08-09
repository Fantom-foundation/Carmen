package s4

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state/s4/rlp"
)

var emptyNodeHash = keccak256(rlp.Encode(rlp.String{}))

func TestMptHasher_EmptyNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(EmptyNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	if got, want := hash, emptyNodeHash; got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}

// The other node types are tested as part of the overall state hash tests.
