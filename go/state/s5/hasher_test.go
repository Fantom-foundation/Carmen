package s5

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
	"github.com/Fantom-foundation/Carmen/go/state/s5/rlp"
)

func TestMptHasher_EmptyNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(s4.EmptyNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	empty := rlp.Encode(rlp.List{})
	if got, want := hash, keccak256(empty); got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}

func TestMptHasher_ValueNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(&s4.ValueNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	empty := rlp.Encode(
		rlp.List{[]rlp.Item{
			rlp.String{}, // TODO: encode path here
			rlp.String{make([]byte, common.ValueSize)},
		}})
	if got, want := hash, keccak256(empty); got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}
