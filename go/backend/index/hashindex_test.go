package index

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0x01}
	B = common.Address{0x02}
)

const (
	HashA  = "21fc3f955c14305ed66b2f6064de082e8447f29048da3ab7c5c01090c1b722ab"
	HashAB = "e2f6dad46dbab4a98b5f5502b171c63780b94cade5d38badce241c9eecea4573"
)

func TestCommit(t *testing.T) {
	hashIndex := NewHashIndex[common.Address](common.AddressSerializer{})

	// the hash is the default one first
	h0, _ := hashIndex.Commit()

	if (h0 != common.Hash{}) {
		t.Errorf("The hash does not match the default one")
	}

	// the hash must change when adding a new item
	hashIndex.AddKey(A)
	ha1, _ := hashIndex.Commit()

	if h0 == ha1 {
		t.Errorf("The hash has not changed")
	}

	if fmt.Sprintf("%x\n", ha1) != fmt.Sprintf("%s\n", HashA) {
		t.Errorf("Hash is %x and not %s", ha1, HashA)
	}

	// try recursive hash with B and already indexed A
	hashIndex.AddKey(B)
	hb1, _ := hashIndex.Commit()

	if fmt.Sprintf("%x\n", hb1) != fmt.Sprintf("%s\n", HashAB) {
		t.Errorf("Hash is %x and not %s", hb1, HashAB)
	}

}
