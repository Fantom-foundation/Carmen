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

package memory

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

func TestMemoryStoreImplements(t *testing.T) {
	var s Store[uint32, common.Value]
	var _ store.Store[uint32, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoringIntoMemoryStore(t *testing.T) {
	memory, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(1, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(2, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := memory.Get(5); value != (common.Value{}) {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := memory.Get(0); value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := memory.Get(1); value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := memory.Get(2); value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	memory, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(5, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(4, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(9, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, _ := memory.Get(1); value != (common.Value{}) {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, _ := memory.Get(5); value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, _ := memory.Get(4); value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, _ := memory.Get(9); value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInMemoryStore(t *testing.T) {
	memory, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
	if err != nil {
		t.Fatalf("failed to create memory store; %s", err)
	}
	defer memory.Close()

	initialHast, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}

	newHash, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if initialHast == newHash {
		t.Errorf("setting into the store have not changed the hash %x %x", initialHast, newHash)
	}
}

func TestInMemoryStoreSnapshot(t *testing.T) {
	memory, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
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
	memory, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
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

	memory2, err := NewStore[uint64, common.Value](common.ValueSerializer{}, 64, htmemory.CreateHashTreeFactory(3))
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
	if val != A {
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
