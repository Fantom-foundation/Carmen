//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package memory

import (
	"io"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
)

func TestMemoryIndexImplements(t *testing.T) {
	var memory Index[common.Address, uint32]
	var _ index.Index[common.Address, uint32] = &memory
	var _ io.Closer = &memory
	var _ backend.Snapshotable = &memory
}

func TestStoringIntoMemoryIndex(t *testing.T) {
	memory := NewIndex[common.Address, uint32](common.AddressSerializer{})
	defer memory.Close()

	indexA, err := memory.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A; %s", err)
		return
	}
	if indexA != 0 {
		t.Errorf("first inserted is not 0")
		return
	}
	indexB, err := memory.GetOrAdd(B)
	if err != nil {
		t.Errorf("failed add of address B; %s", err)
		return
	}
	if indexB != 1 {
		t.Errorf("second inserted is not 1")
		return
	}

	if !memory.Contains(A) {
		t.Errorf("memory does not contains inserted A")
		return
	}
	if !memory.Contains(B) {
		t.Errorf("memory does not contains inserted B")
		return
	}
}

func TestMultipleAssigningOfOneIndex(t *testing.T) {
	memory := NewIndex[common.Address, uint32](common.AddressSerializer{})
	defer memory.Close()

	indexA, err := memory.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A1; %s", err)
		return
	}

	indexA2, err := memory.GetOrAdd(A)
	if err != nil {
		t.Errorf("failed add of address A2; %s", err)
		return
	}
	if indexA != indexA2 {
		t.Errorf("assigned two different indexes for the same address")
		return
	}

	indexA3, err := memory.Get(A)
	if err != nil {
		t.Errorf("failed get id of address A3; %s", err)
		return
	}
	if indexA2 != indexA3 {
		t.Errorf("Get returns different value than GetOrAdd")
		return
	}
}

func TestHash(t *testing.T) {
	memory := NewIndex[common.Address, uint32](common.AddressSerializer{})
	defer memory.Close()

	// the hash is the default one first
	h0, _ := memory.GetStateHash()

	if (h0 != common.Hash{}) {
		t.Errorf("The hash does not match the default one")
	}

	// the hash must change when adding a new item
	_, _ = memory.GetOrAdd(A)
	ha1, _ := memory.GetStateHash()

	if h0 == ha1 {
		t.Errorf("The hash has not changed")
	}

	// the hash remains the same when getting an existing item
	_, _ = memory.GetOrAdd(A)
	ha2, _ := memory.GetStateHash()

	if ha1 != ha2 {
		t.Errorf("The hash has changed")
	}

	// try recursive hash with B and already indexed A
	_, _ = memory.GetOrAdd(B)
	hb1, _ := memory.GetStateHash()

	// The hash must remain the same when adding still the same key
	_, _ = memory.GetOrAdd(B)
	hb2, _ := memory.GetStateHash()

	if hb1 != hb2 {
		t.Errorf("The hash has changed")
	}
}
