//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package indexhash

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
	HashA  = "ff9226e320b1deb7fabecff9ac800cd8eb1e3fb7709c003e2effcce37eec68ed"
	HashAB = "c28553369c52e217564d3f5a783e2643186064498d1b3071568408d49eae6cbe"
)

func TestCommit(t *testing.T) {
	hashIndex := NewIndexHash[common.Address](common.AddressSerializer{})

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

func TestClear(t *testing.T) {
	hashIndex := NewIndexHash[common.Address](common.AddressSerializer{})

	// add a key and compute the hash
	h0, _ := hashIndex.Commit()
	if (h0 != common.Hash{}) {
		t.Errorf("the hash does not match the default one")
	}

	// the hash must change when adding a new item
	hashIndex.AddKey(A)
	ha1, _ := hashIndex.Commit()

	if h0 == ha1 {
		t.Errorf("the hash has not changed")
	}

	hashIndex.Clear()
	hnew, _ := hashIndex.Commit()
	if (hnew != common.Hash{}) {
		t.Errorf("the hash does not match the default one")
	}

	if len(hashIndex.keys) != 0 {
		t.Errorf("key list is not empty")
	}
}
