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
	"slices"
	"sort"
	"testing"
)

func TestProof_CreateWitnessProofFromNodes(t *testing.T) {
	const N = 10

	nodes := make([]string, 0, N)
	var str string
	for i := 0; i < N; i++ {
		str += string(byte(i))
		nodes = append(nodes, str)
	}

	// proof will not be valid, but it can be still serialised back and forth
	proof := CreateWitnessProofFromNodes(nodes...)
	if proof.IsValid() {
		t.Errorf("proof should be invalid")
	}
	recovered := proof.GetElements()

	sort.Strings(nodes)
	sort.Strings(recovered)

	if got, want := recovered, nodes; !slices.Equal(got, want) {
		t.Errorf("unexpected proof elements: got %v, want %v", got, want)
	}
}
