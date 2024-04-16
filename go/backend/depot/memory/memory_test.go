//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package memory

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"testing"
)

var A = []byte{0x01}
var B = []byte{0x02, 0x02}
var C = []byte{0x33, 0xCC}

func TestInMemoryStoreSnapshot(t *testing.T) {
	memory, err := NewDepot[uint64](64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(1, A) // A in snapshot1
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	snapshot1, err := memory.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot; %s", err)
	}

	part1Before, err := snapshot1.GetPart(0)
	if err != nil {
		t.Fatalf("failed to get snapshot part; %s", err)
	}
	proof1Before, err := snapshot1.GetProof(0)
	if err != nil {
		t.Fatalf("failed to get part proof; %s", err)
	}
	rootHash1Before := snapshot1.GetRootProof()

	err = memory.Set(1, B) // B in snapshot2
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	snapshot2, err := memory.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot; %s", err)
	}
	err = memory.Set(1, C) // C in store
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}

	part1After, err := snapshot1.GetPart(0)
	if err != nil {
		t.Fatalf("failed to get snapshot part; %s", err)
	}

	proof1After, err := snapshot1.GetProof(0)
	if err != nil {
		t.Fatalf("failed to get part proof; %s", err)
	}
	rootHash1After := snapshot1.GetRootProof()

	part2, err := snapshot2.GetPart(0)
	if err != nil {
		t.Fatalf("failed to get snapshot part; %s", err)
	}

	proof2, err := snapshot2.GetProof(0)
	if err != nil {
		t.Fatalf("failed to get part proof; %s", err)
	}
	rootHash2 := snapshot2.GetRootProof()

	if !bytes.Equal(part1Before.ToBytes(), part1After.ToBytes()) {
		t.Errorf("part bytes changed in one snapshot; %x != %x", part1Before.ToBytes(), part1After.ToBytes())
	}
	if !bytes.Equal(proof1Before.ToBytes(), proof1After.ToBytes()) {
		t.Errorf("proof bytes changed in one snapshot; %x != %x", proof1Before.ToBytes(), proof1After.ToBytes())
	}
	if !bytes.Equal(rootHash1Before.ToBytes(), rootHash1After.ToBytes()) {
		t.Errorf("root hash bytes changed in one snapshot; %x != %x", rootHash1Before.ToBytes(), rootHash1After.ToBytes())
	}

	if bytes.Equal(part1Before.ToBytes(), part2.ToBytes()) {
		t.Errorf("part bytes not changed between snapshots; %x == %x", part1Before.ToBytes(), part2.ToBytes())
	}
	if bytes.Equal(proof1Before.ToBytes(), proof2.ToBytes()) {
		t.Errorf("proof bytes not changed between snapshots; %x == %x", proof1Before.ToBytes(), proof2.ToBytes())
	}
	if bytes.Equal(rootHash1Before.ToBytes(), rootHash2.ToBytes()) {
		t.Errorf("root hash bytes not changed between snapshots; %x == %x", rootHash1Before.ToBytes(), rootHash2.ToBytes())
	}

	err = snapshot1.Release()
	if err != nil {
		t.Fatalf("failed to release snapshot; %s", err)
	}

	part2afterRelease, err := snapshot2.GetPart(0)
	if err != nil {
		t.Fatalf("failed to get snapshot part; %s", err)
	}
	if !bytes.Equal(part2.ToBytes(), part2afterRelease.ToBytes()) {
		t.Errorf("part bytes changed in one snapshot; %x != %x", part1Before.ToBytes(), part1After.ToBytes())
	}
}

func TestInMemoryStoreSnapshotRecovery(t *testing.T) {
	memory, err := NewDepot[uint64](64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(1, A)
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	err = memory.Set(3, B)
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	stateHash1, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to get state hash; %s", err)
	}

	snapshot1, err := memory.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot; %s", err)
	}
	snapshot1data := snapshot1.GetData()

	memory2, err := NewDepot[uint64](64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory2.Close()

	err = memory2.Restore(snapshot1data)
	if err != nil {
		t.Fatalf("failed to recover snapshot; %s", err)
	}

	val, err := memory2.Get(1)
	if err != nil {
		t.Fatalf("failed get from new memory; %s", err)
	}
	if !bytes.Equal(val, A) {
		t.Errorf("value loaded from recovered store does not match")
	}
	stateHash2, err := memory2.GetStateHash()
	if err != nil {
		t.Fatalf("failed to get state hash; %s", err)
	}
	if stateHash1 != stateHash2 {
		t.Errorf("recovered store hash does not match; %x != %x", stateHash1, stateHash2)
	}
}
