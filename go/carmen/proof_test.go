// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common/immutable"
	"golang.org/x/exp/slices"
	"testing"
)

func TestProof_CreateWitnessProofFromNodes(t *testing.T) {
	const N = 10

	wantElements := make([]Bytes, 0, N)
	b := make([]byte, 0, N)
	for i := 0; i < N; i++ {
		b = append(b, byte(i))
		wantElements = append(wantElements, immutable.NewBytes(b))
	}

	// proof will not be valid, but it can be still serialised back and forth
	proof := CreateWitnessProofFromNodes(wantElements...)
	if proof.IsValid() {
		t.Errorf("proof should be invalid")
	}
	gotElements := proof.GetElements()

	slices.SortFunc(gotElements, func(a, b Bytes) bool {
		return bytes.Compare(a.ToBytes(), b.ToBytes()) < 0
	})
	slices.SortFunc(wantElements, func(a, b Bytes) bool {
		return bytes.Compare(a.ToBytes(), b.ToBytes()) < 0
	})

	if got, want := gotElements, wantElements; !slices.Equal(got, want) {
		t.Errorf("unexpected proof elements: got %v, want %v", got, want)
	}
}
