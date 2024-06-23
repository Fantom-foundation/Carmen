// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	gomock "go.uber.org/mock/gomock"
)

var PathLengthTracking = MptConfig{
	Hashing:                       EthereumLikeHashing,
	TrackSuffixLengthsInLeafNodes: true,
}

// ----------------------------------------------------------------------------
//                               Empty Node
// ----------------------------------------------------------------------------

func TestEmptyNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)

	addr := common.Address{1}

	empty := EmptyNode{}
	path := addressToNibbles(addr)
	if info, exists, err := empty.GetAccount(mgr, addr, path[:]); !info.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return empty info, got %v, exists %v, err %v", info, exists, err)
	}
}

func TestEmptyNode_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{1}
	info := AccountInfo{Nonce: common.Nonce{1}}

	// The state before the insert.
	ref, node := ctxt.Build(Empty{})

	// The state after the insert.
	afterRef, _ := ctxt.Build(&Account{dirty: true, address: addr, info: info, dirtyHash: true})

	// The operation is creating one account node.
	accountRef, _ := ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("empty node should never change")
	}
	if newRoot != accountRef {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", accountRef, newRoot)
	}

	ctxt.ExpectEqualTries(t, afterRef, accountRef)
}

func TestEmptyNode_SetAccount_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr := common.Address{1}
	info := AccountInfo{Nonce: common.Nonce{1}}

	// The state before the insert.
	ref, node := ctxt.Build(Empty{})

	// The state after the insert with the proper length.
	after, _ := ctxt.Build(&Account{dirty: true, address: addr, info: info, pathLength: 33, dirtyHash: true})

	// The operation is creating one account node.
	account, _ := ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	path = path[7:] // pretend the node is nested somewhere.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("empty node should never change")
	}
	if newRoot != account {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", account, newRoot)
	}

	ctxt.ExpectEqualTries(t, after, account)
}

func TestEmptyNode_SetAccount_ToEmptyInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{1}
	info := AccountInfo{}

	// The state before the insert.
	ref, node := ctxt.Build(Empty{})

	// The state after the insert should remain unchanged.
	after, _ := ref, node

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("adding empty account information should have not changed the trie")
	}
	if newRoot != ref {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", ref, newRoot)
	}

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestEmptyNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}

	if handle.Get().IsDirty() {
		t.Errorf("expected node to be clean after release")
	}
}

func TestEmptyNode_StoresAConstantZeroHashThatIsConsideredDirty(t *testing.T) {
	node := EmptyNode{}
	_, dirty := node.GetHash()
	if !dirty {
		t.Errorf("an empty node should have a dirty hash")
	}
}

func TestEmptyNode_SetHashHasNoEffect(t *testing.T) {
	node := EmptyNode{}
	_, dirty := node.GetHash()
	if !dirty {
		t.Errorf("an empty node should always have a dirty hash")
	}
	node.SetHash(common.Hash{})
	if !dirty {
		t.Errorf("an empty node should always have a dirty hash")
	}
}

func TestEmptyNode_IsAlwaysFrozen(t *testing.T) {
	node := EmptyNode{}
	if !node.IsFrozen() {
		t.Errorf("empty node should be alway frozen")
	}
}

func TestEmptyNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	_, node := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Freeze(ctxt, handle); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
}

func TestEmptyNode_Visit(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(Empty{})
	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)
	depth4 := 4
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth4}).Return(VisitResponseAbort)
	depth6 := 6
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth6}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, &ref, 4, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, &ref, 6, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestEmptyNode_ChecksDoNotProduceErrors(t *testing.T) {
	empty := EmptyNode{}
	if err := empty.Check(nil, nil, nil); err != nil {
		t.Errorf("unexpected error from empty node check: %v", err)
	}
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

func TestBranchNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	nodeRef, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, nodeRef)

	// Case 1: the trie does not contain the requested account.
	trg := common.Address{}
	path := addressToNibbles(trg)
	handle := node.GetReadHandle()
	defer handle.Release()
	if info, exists, err := handle.Get().GetAccount(ctxt, trg, path[:]); !info.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return empty info, got %v, exists %v, err %v", info, exists, err)
	}

	// Case 2: the trie contains the requested account.
	trg = common.Address{0x81}
	path = addressToNibbles(trg)
	if res, exists, err := handle.Get().GetAccount(ctxt, trg, path[:]); res != info || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", info, res, exists, err)
	}
}

func TestBranchNode_SetAccount_WithExistingAccount_NoChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	after, _ := ctxt.Clone(ref)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, ref, after)
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_NoChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)

	ctxt.Freeze(ref)
	after, _ := ctxt.Clone(ref)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, ref, after)
}

func TestBranchNode_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info1},
		}},
	)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info2, dirty: true, dirtyHash: true},
		}, dirty: true, dirtyChildHashes: []int{8}, dirtyHash: true},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// The account node that is targeted should marked to be updated.
	readHandle := node.GetReadHandle()
	branch := readHandle.Get().(*BranchNode)
	account, _ := ctxt.getWriteAccess(&branch.children[8])
	account.Release()
	readHandle.Release()

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, ref)
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info1},
		}},
	)
	ctxt.Check(t, before)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1, frozen: true},
			8: &Account{address: common.Address{0x81}, info: info2, dirty: true, dirtyHash: true},
		}, dirty: true, dirtyChildHashes: []int{8}, frozenChildren: []int{4}, dirtyHash: true},
	)
	ctxt.Check(t, after)

	// Create and freeze the target node.
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	// This operation should create a new account and branch node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("setting account failed: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen node should never change")
	}
	if ref == newRoot {
		t.Errorf("modification did not create a new root")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x21}, info: info, dirty: true, dirtyHash: true},
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: true, dirtyChildHashes: []int{2}, dirtyHash: true},
	)
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	handle := node.GetWriteHandle()

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x21}, info: info, dirty: true, dirtyHash: true},
			4: &Account{address: common.Address{0x40}, info: info, frozen: true},
			8: &Account{address: common.Address{0x81}, info: info, frozen: true},
		}, dirty: true, dirtyChildHashes: []int{2}, frozenChildren: []int{4, 8}, dirtyHash: true},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and a new branch.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				0: &Account{address: common.Address{0x40}, info: info},
				1: &Account{address: common.Address{0x41}, info: info, dirty: true, dirtyHash: true},
			}, dirty: true, dirtyChildHashes: []int{1}, dirtyHash: true},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{4}},
	)
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	handle := node.GetWriteHandle()

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				0: &Account{address: common.Address{0x40}, info: info, frozen: true},
				1: &Account{address: common.Address{0x41}, info: info, dirty: true, dirtyHash: true},
			}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{1}, frozenChildren: []int{0}},
			8: &Account{address: common.Address{0x81}, info: info, frozen: true},
		}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{4}, frozenChildren: []int{8}},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and 2 new branches.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			4: &Tag{"A", &Account{address: common.Address{0x41}, info: info}},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{}},
	)
	ctxt.Check(t, after)

	accountRef, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountRef).Return(nil)

	handle := node.GetWriteHandle()

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, ref)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info, frozen: true},
			8: &Account{address: common.Address{0x82}, info: info, frozen: true},
		}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{}, frozenChildren: []int{2, 8}},
	)
	ctxt.Check(t, after)

	// This situation should create a new branch node to be used as a result.
	ctxt.ExpectCreateBranch()

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info}},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info})
	ctxt.Check(t, after)

	account, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&account).Return(nil)
	ctxt.EXPECT().release(&ref).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info, frozen: true})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	ctxt.ExpectCreateTemporaryBranch()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Tag{"R", &Account{address: common.Address{0x41}, info: info, pathLength: 39}},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info, pathLength: 39}},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info, pathLength: 40, dirty: true, dirtyHash: true})
	ctxt.Check(t, after)

	accountRef, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountRef).Return(nil)
	ctxt.EXPECT().release(&ref).Return(nil)

	// The remaining account is updated because its length has changed.
	_, account := ctxt.Get("R")
	accountHandle := account.GetWriteHandle()
	accountHandle.Release()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info, pathLength: 39},
			8: &Account{address: common.Address{0x82}, info: info, pathLength: 39},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info, pathLength: 40, dirty: true, dirtyHash: true})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	ctxt.ExpectCreateTemporaryBranch()

	// It also creates a new account node with a modified length.
	ctxt.ExpectCreateAccount()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Tag{"E", &Extension{
				path: []Nibble{1, 2, 3},
				next: &Branch{children: Children{
					1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info},
					2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info},
				}},
			}},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info}},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4, 1, 2, 3},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info},
			2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info},
		}},
		dirty:     true,
		hashDirty: true,
	})
	ctxt.Check(t, after)

	accountRef, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountRef).Return(nil)
	ctxt.EXPECT().release(&ref).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Extension{
				path: []Nibble{1, 2, 3},
				next: &Branch{children: Children{
					1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info},
					2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info},
				}},
			},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4, 1, 2, 3},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info, frozen: true},
			2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info, frozen: true},
		}, frozen: true, frozenChildren: []int{1, 2}},
		dirty:     true,
		hashDirty: true,
	})
	ctxt.Check(t, after)

	// This update creates a temporary branch that is released again.
	ctxt.ExpectCreateTemporaryBranch()

	// Also, a new extension node (the result) is expected to be created.
	ctxt.ExpectCreateExtension()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				1: &Account{address: common.Address{0x41, 0x20}, info: info},
				2: &Account{address: common.Address{0x42, 0x84}, info: info},
			}},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info}},
		}},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x20}, info: info},
			2: &Account{address: common.Address{0x42, 0x84}, info: info},
		}},
		dirty:     true,
		hashDirty: true,
	})
	ctxt.Check(t, after)

	extensionId, _ := ctxt.ExpectCreateExtension()

	accountRef, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountRef).Return(nil)
	ctxt.EXPECT().release(&ref).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := extensionId
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				1: &Account{address: common.Address{0x41, 0x20}, info: info},
				2: &Account{address: common.Address{0x42, 0x84}, info: info},
			}},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x20}, info: info, frozen: true},
			2: &Account{address: common.Address{0x42, 0x84}, info: info, frozen: true},
		}, frozen: true, frozenChildren: []int{1, 2}},
		dirty:     true,
		hashDirty: true,
	})
	ctxt.Check(t, after)

	// This update creates a temporary branch that is released again.
	ctxt.ExpectCreateTemporaryBranch()

	// Also, a new extension node (the result) is expected to be created.
	ctxt.ExpectCreateExtension()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(&Branch{children: Children{
		1: &Tag{"A", &Account{}},
		4: &Tag{"B", &Account{}},
		8: &Tag{"C", &Account{}},
	}})

	ctxt.EXPECT().release(&ref).Return(nil)
	accountRefA, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountRefA).Return(nil)
	accountRefB, _ := ctxt.Get("B")
	ctxt.EXPECT().release(&accountRefB).Return(nil)
	accountRefC, _ := ctxt.Get("C")
	ctxt.EXPECT().release(&accountRefC).Return(nil)

	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}

	if handle.Get().IsDirty() {
		t.Errorf("released nodes should be clean")
	}

	handle.Release()
}

func TestBranchNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)
	node3 := NewMockNode(ctrl)

	_, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		4: &Mock{node: node2},
		8: &Mock{node: node3},
	}})

	node1.EXPECT().Freeze(gomock.Any(), gomock.Any()).Return(nil)
	node2.EXPECT().Freeze(gomock.Any(), gomock.Any()).Return(nil)
	node3.EXPECT().Freeze(gomock.Any(), gomock.Any()).Return(nil)

	handle := node.GetWriteHandle()
	if handle.Get().(*BranchNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := handle.Get().Freeze(ctxt, handle); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !handle.Get().(*BranchNode).frozen {
		t.Errorf("node not marked as frozen after call")
	}
	handle.Release()
}

func TestBranchNode_VisitContinue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	node1.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)
	node2.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitPruned(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)

	ref, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)

	ref, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	node1.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)
	node2.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil) // = aborted

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_CheckDetectsIssues(t *testing.T) {
	emptyValueHash := common.Hash{
		202, 105, 210, 237, 104, 137, 30, 166, 212, 211, 73, 237, 177, 164, 110, 101,
		102, 128, 108, 18, 151, 209, 112, 197, 142, 218, 9, 115, 49, 58, 100, 135,
	}
	unknownHashStatus := hashStatusUnknown
	tests := map[string]struct {
		setup NodeDesc
		ok    bool
	}{
		"no children":    {&Branch{}, false},
		"only one child": {&Branch{children: Children{1: &Value{}}}, false},
		"two children":   {&Branch{children: Children{1: &Value{}, 2: &Value{}}}, true},
		"invalid hash": {&Branch{
			children:    Children{1: &Value{}, 2: &Value{}},
			childHashes: ChildHashes{1: common.Hash{1}, 2: emptyValueHash},
		}, false},
		"dirty hashes are ignored": {&Branch{
			children:         Children{1: &Value{}, 2: &Value{}},
			childHashes:      ChildHashes{1: common.Hash{1}, 2: emptyValueHash},
			dirty:            true,
			dirtyHash:        true,
			dirtyChildHashes: []int{1},
		}, true},
		"full_frozen": {&Branch{
			children:       Children{1: &Value{frozen: true}, 2: &Value{frozen: true}},
			frozenChildren: []int{1, 2},
			frozen:         true,
		}, true},
		"partial_frozen": {&Branch{
			children:       Children{1: &Value{frozen: true}, 2: &Value{frozen: true}},
			frozenChildren: []int{1, 2},
			frozen:         false,
		}, true},
		"partial_frozen_with_missing_hint": {&Branch{
			children:       Children{1: &Value{frozen: true}, 2: &Value{frozen: true}},
			frozenChildren: []int{1},
			frozen:         false,
		}, true},
		"inconsistent_freeze_flags": {&Branch{
			children:       Children{1: &Value{}, 2: &Value{}},
			frozenChildren: []int{2},
		}, false},
		"frozen_branch_with_non_frozen_children": {&Branch{
			children:       Children{1: &Value{}, 2: &Value{frozen: true}},
			frozenChildren: []int{2},
			frozen:         true,
		}, false},
		"clean_with_dirty_hash": {&Branch{
			children:  Children{1: &Value{}, 2: &Value{}},
			dirty:     false,
			dirtyHash: true,
		}, false},
		"clean_hash_with_dirty_child_hash": {&Branch{
			children:         Children{1: &Value{}, 2: &Value{}},
			dirty:            true,
			dirtyHash:        false,
			dirtyChildHashes: []int{1},
		}, false},
		"dirts_node_with_unknown_hash_status": {&Branch{
			children:   Children{1: &Value{}, 2: &Value{}},
			dirty:      true,
			hashStatus: &unknownHashStatus,
		}, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)
			ref, node := ctxt.Build(test.setup)
			handle := node.GetViewHandle()
			defer handle.Release()

			err := handle.Get().Check(ctxt, &ref, nil)
			if test.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !test.ok && err == nil {
				t.Errorf("expected an error but check passed")
			}
		})
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

func TestExtensionNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, ref)

	// Case 1: try to locate a non-existing address
	trg := common.Address{}
	path := addressToNibbles(trg)
	handle := node.GetReadHandle()
	defer handle.Release()
	if res, exists, err := handle.Get().GetAccount(ctxt, trg, path[:]); !res.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", AccountInfo{}, res, exists, err)
	}

	// Case 2: locate an existing address
	trg = common.Address{0x12, 0x35}
	path = addressToNibbles(trg)
	if res, exists, err := handle.Get().GetAccount(ctxt, trg, path[:]); res != info || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", info, res, exists, err)
	}

	// Case 3: locate an address with a partial extension path overlap only
	trg = common.Address{0x12, 0x4F}
	path = addressToNibbles(trg)
	if res, exists, err := handle.Get().GetAccount(ctxt, trg, path[:]); !res.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", AccountInfo{}, res, exists, err)
	}
}

func TestExtensionNode_SetAccount_ExistingLeaf_UnchangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, ref)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, trg, path[:], info); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, ref)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_UnchangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)
	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Clone(ref)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, trg, path[:], info); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, ref)
	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Tag{"B", &Branch{children: Children{
				5: &Tag{"A", &Account{address: common.Address{0x12, 0x35}, info: info1}},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}}},
		},
	)
	ctxt.Check(t, ref)

	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info2, dirty: true, dirtyHash: true},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{5}},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)
	ctxt.Check(t, after)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, trg, path[:], info2); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info1},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}},
		},
	)
	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info2, dirty: true, dirtyHash: true},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2, frozen: true},
			}, dirtyChildHashes: []int{5}, frozenChildren: []int{8}, dirty: true, dirtyHash: true},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)
	ctxt.Check(t, after)

	// The following update creates a new account, branch, and extension.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, trg, path[:], info2)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)

	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2},
			next: &Branch{children: Children{
				3: &Extension{
					path: []Nibble{4},
					next: &Branch{children: Children{
						0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
						0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
					}},
					dirty:     true,
					hashDirty: true,
				},
				4: &Account{address: common.Address{0x12, 0x40}, info: info, dirty: true, dirtyHash: true},
			}, dirtyChildHashes: []int{3, 4}, dirty: true, dirtyHash: true},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch, extension and account is created.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateAccount()
	extension, _ := ctxt.ExpectCreateExtension()

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != extension || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", extension, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, extension)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2},
			next: &Branch{children: Children{
				3: &Extension{
					path: []Nibble{4},
					next: &Branch{children: Children{
						0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info, frozen: true},
						0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info, frozen: true},
					}, frozen: true, frozenChildren: []int{0xA, 0xE}},
					dirty:     true,
					hashDirty: true,
				},
				4: &Account{address: common.Address{0x12, 0x40}, info: info, dirty: true, dirtyHash: true},
			}, dirtyChildHashes: []int{3, 4}, dirty: true, dirtyHash: true},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch, two extensions, and account is created.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateAccount()

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			1: &Extension{
				path: []Nibble{2, 3, 4},
				next: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
					0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
				}},
				dirty:     true,
				hashDirty: true,
			},
			4: &Account{address: common.Address{0x40}, info: info, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{1, 4}, dirty: true, dirtyHash: true},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	ctxt.ExpectCreateAccount()
	branchId, _ := ctxt.ExpectCreateBranch()

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, branchId)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			1: &Extension{
				path: []Nibble{2, 3, 4},
				next: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info, frozen: true},
					0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info, frozen: true},
				}, frozen: true, frozenChildren: []int{0xA, 0xE}},
				dirty:     true,
				hashDirty: true,
			},
			4: &Account{address: common.Address{0x40}, info: info, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{1, 4}, dirty: true, dirtyHash: true},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)

	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				4: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
					0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
				}},
				8: &Account{address: common.Address{0x12, 0x38}, info: info, dirty: true, dirtyHash: true},
			}, dirtyChildHashes: []int{8}, dirty: true, dirtyHash: true},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{
				children: Children{
					4: &Branch{children: Children{
						0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info, frozen: true},
						0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info, frozen: true},
					}, frozen: true, frozenChildren: []int{0xA, 0xE}},
					8: &Account{address: common.Address{0x12, 0x38}, info: info, dirty: true, dirtyHash: true},
				},
				dirty:            true,
				dirtyHash:        true,
				dirtyChildHashes: []int{8},
				frozenChildren:   []int{4},
			},
			dirty:         true,
			hashDirty:     true,
			nextHashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x1A}, info: info},
				0xE: &Account{address: common.Address{0x1E}, info: info},
			}},
		},
	)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			1: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x1A}, info: info},
				0xE: &Account{address: common.Address{0x1E}, info: info},
			}},
			2: &Account{address: common.Address{0x20}, info: info, dirty: true, dirtyHash: true},
		}, dirty: true, dirtyHash: true, dirtyChildHashes: []int{2}},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// In this case a branch and account is created and an extension released.
	ctxt.ExpectCreateAccount()
	branchId, _ := ctxt.ExpectCreateBranch()

	ctxt.EXPECT().release(&ref).Return(nil)

	addr := common.Address{0x20}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != branchId || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, branchId)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x1A}, info: info},
				0xE: &Account{address: common.Address{0x1E}, info: info},
			}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Branch{
			children: Children{
				1: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x1A}, info: info, frozen: true},
					0xE: &Account{address: common.Address{0x1E}, info: info, frozen: true},
				}, frozen: true, frozenChildren: []int{0xA, 0xE}},
				2: &Account{address: common.Address{0x20}, info: info, dirty: true, dirtyHash: true},
			},
			dirty:            true,
			dirtyHash:        true,
			dirtyChildHashes: []int{2},
			frozenChildren:   []int{1},
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, before)
	ctxt.Check(t, after)

	// The following update creates and discards a temporary extension.
	ctxt.ExpectCreateTemporaryExtension()

	// Also, the creation of a new account.
	ctxt.ExpectCreateAccount()

	// And the creation of a new branch.
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x20}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Branch{children: Children{
					1: &Account{address: common.Address{0x11, 0x10}, info: info},
					2: &Account{address: common.Address{0x11, 0x20}, info: info},
				}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)

	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 1},
			next: &Branch{children: Children{
				1: &Account{address: common.Address{0x11, 0x10}, info: info},
				2: &Account{address: common.Address{0x11, 0x20}, info: info},
			}},
			dirty:     true,
			hashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	account, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&account)

	branch, _ := ctxt.Get("B")
	ctxt.EXPECT().release(&branch)

	extension, _ := ctxt.ExpectCreateExtension()
	ctxt.EXPECT().release(&extension).Return(nil)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Branch{children: Children{
					1: &Account{address: common.Address{0x11, 0x10}, info: info},
					2: &Account{address: common.Address{0x11, 0x20}, info: info},
				}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 1},
			next: &Branch{children: Children{
				1: &Account{address: common.Address{0x11, 0x10}, info: info, frozen: true},
				2: &Account{address: common.Address{0x11, 0x20}, info: info, frozen: true},
			}, frozen: true, frozenChildren: []int{1, 2}},
			dirty:     true,
			hashDirty: true,
		},
	)

	ctxt.Check(t, ref)
	ctxt.Check(t, before)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It also requires a temporary extension.
	extension, _ := ctxt.ExpectCreateExtension()
	ctxt.EXPECT().release(&extension)

	// And it also creates a new extension that constitutes the result.
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)

	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info})

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	account, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&account)

	branch, _ := ctxt.Get("B")
	ctxt.EXPECT().release(&branch)

	ctxt.EXPECT().release(&ref).Return(nil)

	resultId, _ := ctxt.Get("R")

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != resultId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, resultId)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info, frozen: true})

	ctxt.Check(t, before)
	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It creates and discards an extension.
	ctxt.ExpectCreateTemporaryExtension()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	rId, _ := ctxt.Get("R")
	if newRoot != rId {
		t.Errorf("operation should return pre-existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 38}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info, pathLength: 38}},
			}}},
		},
	)

	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 40, dirty: true, dirtyHash: true})

	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	account, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&account)

	branch, _ := ctxt.Get("B")
	ctxt.EXPECT().release(&branch)

	ctxt.EXPECT().release(&ref).Return(nil)

	resultRef, _ := ctxt.Get("R")

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty); newRoot != resultRef || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultRef, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, resultRef)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 38},
				2: &Account{address: common.Address{0x12}, info: info, pathLength: 38},
			}}},
		},
	)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)
	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 40, dirty: true, dirtyHash: true})

	ctxt.Check(t, before)
	ctxt.Check(t, ref)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It creates and discards an extension.
	ctxt.ExpectCreateTemporaryExtension()

	// It also creates a new account with an altered length.
	ctxt.ExpectCreateAccount()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetSlot_NonExistingAccount_PartialPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				1: &Account{address: common.Address{0x12, 0x31}, info: info},
				2: &Account{address: common.Address{0x12, 0x32}, info: info},
			}},
		},
	)

	after, _ := ctxt.Clone(ref)

	ctxt.Check(t, ref)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	key := common.Key{}
	value := common.Value{1}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_Frozen_SetSlot_NonExistingAccount_PartialPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				1: &Account{address: common.Address{0x12, 0x31}, info: info},
				2: &Account{address: common.Address{0x12, 0x32}, info: info},
			}},
		},
	)

	ctxt.Freeze(ref)
	after, _ := ctxt.Clone(ref)

	ctxt.Check(t, ref)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	key := common.Key{}
	value := common.Value{1}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestExtensionNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Tag{"C", &Branch{children: Children{
				1: &Tag{"A", &Account{}},
				4: &Tag{"B", &Account{}},
			}}},
		})

	ctxt.EXPECT().release(&ref).Return(nil)
	accountA, _ := ctxt.Get("A")
	ctxt.EXPECT().release(&accountA).Return(nil)
	accountB, _ := ctxt.Get("B")
	ctxt.EXPECT().release(&accountB).Return(nil)
	branch, _ := ctxt.Get("C")
	ctxt.EXPECT().release(&branch).Return(nil)

	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
	handle.Release()
}

func TestExtensionNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)

	_, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	next.EXPECT().Freeze(gomock.Any(), gomock.Any()).Return(nil)

	handle := node.GetWriteHandle()
	defer handle.Release()
	if handle.Get().(*ExtensionNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := handle.Get().Freeze(ctxt, handle); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !handle.Get().(*ExtensionNode).frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

func TestExtensionNode_VisitContinue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	next.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitPrune(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)

	ref, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)

	ref, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	next.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil) // = abort

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_CheckDetectsIssues(t *testing.T) {
	unknownHashStatus := hashStatusUnknown
	tests := map[string]struct {
		setup NodeDesc
		ok    bool
	}{
		"ok":                                  {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}}, true},
		"empty path":                          {&Extension{next: &Branch{}}, false},
		"next not a branch":                   {&Extension{path: []Nibble{1, 2, 3}, next: &Value{}}, false},
		"invalid hash":                        {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}, nextHash: &common.Hash{1}}, false},
		"dirty hash is ignored":               {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}, nextHash: &common.Hash{1}, dirty: true, hashDirty: true, nextHashDirty: true}, true},
		"full_frozen":                         {&Extension{frozen: true, path: []Nibble{1, 2, 3}, next: &Branch{frozen: true}}, true},
		"partial_frozen":                      {&Extension{frozen: false, path: []Nibble{1, 2, 3}, next: &Branch{frozen: true}}, true},
		"inconsistent_frozen":                 {&Extension{frozen: true, path: []Nibble{1, 2, 3}, next: &Branch{frozen: false}}, false},
		"clean_with_dirty_hash":               {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}, dirty: false, hashDirty: true}, false},
		"clean_hash_with_dirty_next_hash":     {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}, dirty: false, hashDirty: false, nextHashDirty: true}, false},
		"dirts_node_with_unknown_hash_status": {&Extension{path: []Nibble{1, 2, 3}, next: &Branch{}, dirty: true, hashStatus: &unknownHashStatus}, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)
			ref, node := ctxt.Build(test.setup)
			handle := node.GetViewHandle()
			defer handle.Release()

			err := handle.Get().Check(ctxt, &ref, nil)
			if test.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !test.ok && err == nil {
				t.Errorf("expected an error but check passed")
			}
		})
	}
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

func TestAccountNode_AddressIsAccessible(t *testing.T) {
	node := AccountNode{address: common.Address{1, 2, 3}}
	if want, got := node.address, node.Address(); want != got {
		t.Errorf("invalid address produced, wanted %v, got %v", want, got)
	}
}

func TestAccountNode_InfoIsAccessible(t *testing.T) {
	node := AccountNode{info: AccountInfo{
		Balance:  common.Balance{1, 2, 3},
		Nonce:    common.Nonce{4, 5},
		CodeHash: common.Hash{6, 7, 8},
	}}
	if want, got := node.info, node.Info(); want != got {
		t.Errorf("invalid info produced, wanted %v, got %v", want, got)
	}
}
func TestAccountNode_SetPathLength(t *testing.T) {
	tests := []struct {
		before, after byte
	}{
		{0, 0},
		{10, 10},
		{0, 1},
		{1, 0},
		{10, 11},
		{10, 9},
	}

	for _, test := range tests {
		node := AccountNode{
			pathLength: test.before,
		}
		ref := NodeReference{}
		handle := shared.WriteHandle[Node]{}
		res, changed, err := node.setPathLength(nil, &ref, handle, test.after)
		if err != nil {
			t.Fatalf("failed to change path length: %v", err)
		}
		if want, got := (test.before != test.after), changed; want != got {
			t.Errorf("invalid changed flag, wanted %t, got %t", want, got)
		}
		if want, got := ref.Id(), res.Id(); want != got {
			t.Errorf("invalid result node reference, wanted %v, got %v", want, got)
		}
		if want, got := test.after, node.pathLength; want != got {
			t.Errorf("invalid modified path length, wanted %d, got %d", want, got)
		}
	}
}

func TestAccountNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	addr := common.Address{1}
	node := &AccountNode{info: info}

	// Case 1: the node does not contain the requested info.
	path := addressToNibbles(addr)
	if res, exists, err := node.GetAccount(mgr, addr, path[:]); !res.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", AccountInfo{}, res, exists, err)
	}

	// Case 2: the node contains the requested info.
	node.address = addr
	if res, exists, err := node.GetAccount(mgr, addr, path[:]); info != res || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", info, res, exists, err)
	}
}

func TestAccountNode_SetAccount_WithMatchingAccount_SameInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(&Account{address: addr, info: info})
	backupId, _ := ctxt.Clone(ref)

	// Update the account information with the same information.
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backupId, ref)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_SameInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	before, _ := ctxt.Build(&Account{address: addr, info: info})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Account{address: addr, info: info, frozen: true})

	// Update the account information with the same information.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != ref {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(&Account{address: addr, info: info1})
	after, _ := ctxt.Build(&Account{address: addr, info: info2, dirty: true, dirtyHash: true})

	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(&Account{address: addr, info: info1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Account{address: addr, info: info2, dirty: true, dirtyHash: true})

	ctxt.ExpectCreateAccount()

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	ref, node := ctxt.Build(&Account{
		address: addr,
		info:    info1,
		storage: &Tag{"S", &Value{}},
	})
	after, _ := ctxt.Build(Empty{})

	id, _ := ctxt.Get("S")
	ctxt.EXPECT().release(&ref).Return(nil)
	ctxt.EXPECT().releaseTrieAsynchronous(RefTo(id.Id()))

	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2); !newRoot.Id().IsEmpty() || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), false, newRoot.Id(), changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, NewNodeReference(EmptyId()))
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr, info: info1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(&Account{address: addr1, info: info1})

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Account{address: addr1, info: info1},
		3: &Account{address: addr2, info: info2, dirty: true, dirtyHash: true},
	}, dirtyChildHashes: []int{3}, dirty: true, dirtyHash: true})

	// This operation creates one new account node and a branch.
	ctxt.ExpectCreateAccount()
	res, _ := ctxt.ExpectCreateBranch()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, res)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_NoCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Account{address: addr1, info: info1, frozen: true},
		3: &Account{address: addr2, info: info2, dirty: true, dirtyHash: true},
	}, dirtyChildHashes: []int{3}, dirty: true, dirtyHash: true, frozenChildren: []int{2}})

	// This operation creates one new account node and a branch.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(&Account{address: addr1, info: info1})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1},
			0xB: &Account{address: addr2, info: info2, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xB}, dirty: true, dirtyHash: true},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, res)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1, frozen: true},
			0xB: &Account{address: addr2, info: info2, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xB}, dirty: true, dirtyHash: true, frozenChildren: []int{0xA}},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	ref, node := ctxt.Build(&Account{address: addr1, info: info1, pathLength: 40})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1, pathLength: 36, dirty: true, dirtyHash: true},
			0xB: &Account{address: addr2, info: info2, pathLength: 36, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xA, 0xB}, dirty: true, dirtyHash: true},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2); newRoot != res || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, res)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1, pathLength: 40})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1, pathLength: 36, dirty: true, dirtyHash: true},
			0xB: &Account{address: addr2, info: info2, pathLength: 36, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xA, 0xB}, dirty: true, dirtyHash: true},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates two new accounts, one branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	ref, node := ctxt.Build(&Account{address: addr1, info: info1})
	after, _ := ctxt.Clone(ref)

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Account{address: addr1, info: info1, frozen: true})

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != ref {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	ref, node := ctxt.Build(&Account{address: addr1, info: info1})
	after, _ := ctxt.Clone(ref)

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Account{address: addr1, info: info1, frozen: true})

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, addr2, path[:], info2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != ref {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_GetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node := &AccountNode{}

	key := common.Key{}
	path := keyToNibbles(key)
	if _, _, err := node.GetValue(ctxt, key, path[:]); err == nil {
		t.Fatalf("GetValue call should always return an error")
	}
}

func TestAccountNode_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	ref := NewNodeReference(AccountId(12))
	node := &AccountNode{}

	if _, _, err := node.SetValue(ctxt, &ref, shared.WriteHandle[Node]{}, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
}

func TestAccountNode_Frozen_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	ref, node := ctxt.Build(&Account{})
	ctxt.Freeze(ref)

	handle := node.GetWriteHandle()
	if _, _, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
	handle.Release()
}

func TestAccountNode_SetSlot_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x12, 0x3A}
	info := AccountInfo{Nonce: common.Nonce{1}}
	key := common.Key{0x21}
	value := common.Value{1}

	ref, node := ctxt.Build(&Account{address: addr, info: info})

	after, _ := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:    key,
			value:  value,
			length: 64,
			dirty:  true, dirtyHash: true,
		},
		dirty:            true,
		dirtyHash:        true,
		storageHashDirty: true,
	})

	// This operation creates one new value node.
	ctxt.ExpectCreateValue()

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_SetSlot_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr := common.Address{0x12, 0x3A}
	info := AccountInfo{Nonce: common.Nonce{1}}
	key := common.Key{0x21}
	value := common.Value{1}

	before, _ := ctxt.Build(&Account{address: addr, info: info})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:    key,
			value:  value,
			length: 64,
			dirty:  true, dirtyHash: true,
		},
		dirty:            true,
		dirtyHash:        true,
		storageHashDirty: true,
	})

	// This operation creates a new account and a value.
	ctxt.ExpectCreateValue()
	ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetSlot_UpdateOfExistingValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x12, 0x3A}
	info := AccountInfo{Nonce: common.Nonce{1}}
	key := common.Key{0x21}
	value1 := common.Value{1}
	value2 := common.Value{2}

	ref, node := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:   key,
			value: value1,
		},
	})

	after, _ := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:   key,
			value: value2,
			dirty: true, dirtyHash: true,
		},
		dirty:            true,
		dirtyHash:        true,
		storageHashDirty: true,
	})

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value2); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_SetSlot_UpdateOfExistingValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr := common.Address{0x12, 0x3A}
	info := AccountInfo{Nonce: common.Nonce{1}}
	key := common.Key{0x21}
	value1 := common.Value{1}
	value2 := common.Value{2}

	before, _ := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:   key,
			value: value1,
		},
	})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Account{
		address: addr,
		info:    info,
		storage: &Value{
			key:   key,
			value: value2,
			dirty: true, dirtyHash: true,
		},
		dirty:            true,
		dirtyHash:        true,
		storageHashDirty: true,
	})

	// This operation creates a new account and a value.
	ctxt.ExpectCreateValue()
	ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(&Account{})

	ctxt.EXPECT().release(&ref).Return(nil)
	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestAccountNode_ReleaseStateTrie(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	ref, node := ctxt.Build(&Account{
		storage: &Tag{"A", &Mock{storage}},
	})

	storageRef, storageNode := ctxt.Get("A")
	ctxt.EXPECT().release(&ref).Return(nil)

	write := storageNode.GetWriteHandle()
	storage.EXPECT().Release(ctxt, RefTo(storageRef.Id()), write)
	write.Release()

	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestAccountNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storageRoot := NewMockNode(ctrl)

	storage, _ := ctxt.Build(&Mock{storageRoot})
	_, node := ctxt.Build(&Account{})

	handle := node.GetWriteHandle()
	defer handle.Release()
	handle.Get().(*AccountNode).storage = storage

	storageRoot.EXPECT().Freeze(gomock.Any(), gomock.Any()).Return(nil)

	if handle.Get().(*AccountNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := handle.Get().Freeze(ctxt, handle); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !handle.Get().(*AccountNode).frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

func TestAccountNode_Frozen_SetSlot_WithExistingSlotValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0xAA}
	key := common.Key{0x12, 0x3A}
	value := common.Value{1}

	newValue := common.Value{2}

	ref, node := ctxt.Build(&Account{
		address: addr,
		info:    AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		storage: &Value{key: key, value: value},
	})
	ctxt.Freeze(ref)

	// A new account and value is expected to be created.
	newId, _ := ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateValue()

	handle := node.GetWriteHandle()
	defer handle.Release()

	path := keyToNibbles(key)
	newRoot, changed, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, newValue)
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}

	// check value exists for the original node
	if _, exists, _ := handle.Get().GetSlot(ctxt, addr, path[:], key); !exists {
		t.Errorf("value for key %v should exist", key)
	}

	// check value is gone for the new root
	newHandle, _ := ctxt.getReadAccess(&newRoot)
	defer newHandle.Release()
	if val, exists, _ := newHandle.Get().GetSlot(ctxt, addr, path[:], key); val != newValue || !exists {
		t.Errorf("value for key %v should not exist", key)
	}
}

func TestAccountNode_Frozen_Split_InSetPrefixLength(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, MptConfig{
		Name:                          "S4",
		UseHashedPaths:                false,
		TrackSuffixLengthsInLeafNodes: true,
		Hashing:                       DirectHashing,
	})

	addr1 := common.Address{0xA0}
	addr2 := common.Address{0xB0}

	key := common.Key{0x12}
	value := common.Value{1}

	ref, node := ctxt.Build(
		&Branch{
			children: Children{
				0xA: &Account{address: addr1, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
					storage:    &Value{key: key, value: value, length: 64},
					pathLength: 39},
				0xB: &Account{address: addr2, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAB}},
					pathLength: 39},
			},
		})

	ctxt.Check(t, ref)
	ctxt.Freeze(ref)

	before, _ := ctxt.Clone(ref)

	newInfo := AccountInfo{common.Nonce{1}, common.Balance{100}, common.Hash{0xAA}}
	newAddr := common.Address{0xAA, 0xB}

	after, _ := ctxt.Build(&Branch{
		children: Children{
			0xA: &Branch{
				children: Children{
					0: &Account{address: addr1, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
						storage:    &Value{key: key, value: value, length: 64, frozen: true},
						pathLength: 38, dirty: true, dirtyHash: true},
					0xA: &Account{address: newAddr, info: newInfo,
						pathLength: 38, dirty: true, dirtyHash: true},
				},
				dirty:            true,
				dirtyHash:        true,
				dirtyChildHashes: []int{0, 0xA},
			},
			0xB: &Account{address: addr2, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAB}},
				pathLength: 39, frozen: true},
		},
		dirty:            true,
		dirtyHash:        true,
		dirtyChildHashes: []int{0xA},
		frozenChildren:   []int{0xB},
	})
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateAccount()
	newId, _ := ctxt.ExpectCreateBranch()

	handle := node.GetWriteHandle()
	path := addressToNibbles(newAddr)
	// This creates a new account on the path of a frozen account
	newRoot, changed, err := handle.Get().SetAccount(ctxt, &ref, handle, newAddr, path, newInfo)
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_ClearStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0xAA}
	key := common.Key{0x12, 0x3A}
	value := common.Value{1}

	ref, node := ctxt.Build(&Account{
		address: addr,
		info:    AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		storage: &Tag{"A", &Value{key: key, value: value}},
	})

	after, _ := ctxt.Build(&Account{
		address:          addr,
		info:             AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		dirty:            true,
		dirtyHash:        true,
		storageHashDirty: true,
	})

	storage, _ := ctxt.Get("A")
	ctxt.EXPECT().releaseTrieAsynchronous(RefTo(storage.Id()))

	handle := node.GetWriteHandle()
	path := keyToNibbles(key)
	newRoot, changed, err := handle.Get().ClearStorage(ctxt, &ref, handle, addr, path[:])
	if newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestAccountNode_Frozen_ClearStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0xAA}
	key := common.Key{0x12, 0x3A}
	value := common.Value{1}

	ref, node := ctxt.Build(&Account{
		address: addr,
		info:    AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		storage: &Value{key: key, value: value},
	})
	ctxt.Freeze(ref)

	newId, _ := ctxt.ExpectCreateAccount() // new account will be created

	handle := node.GetWriteHandle()
	defer handle.Release()

	path := keyToNibbles(key)
	newRoot, changed, err := handle.Get().ClearStorage(ctxt, &ref, handle, addr, path[:])
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}

	// check value exists for the original node
	if _, exists, _ := handle.Get().GetSlot(ctxt, addr, path[:], key); !exists {
		t.Errorf("value for key: %s should exist: ", key)
	}

	// check value is gone for the new root
	newHandle, _ := ctxt.getReadAccess(&newRoot)
	defer newHandle.Release()
	if _, exists, _ := newHandle.Get().GetSlot(ctxt, addr, path[:], key); exists {
		t.Errorf("value for key %v should not exist", key)
	}
}

func TestAccountNode_VisitContinue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	storage.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitPrune(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	storage.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_CheckDetectsIssues(t *testing.T) {
	unknownHashStatus := hashStatusUnknown
	tests := map[string]struct {
		path  []Nibble
		setup NodeDesc
		ok    bool
	}{
		"ok": {[]Nibble{1, 2, 3}, &Account{
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
		}, true},
		"wrong branch": {[]Nibble{1, 2, 3}, &Account{
			address:    common.Address{0x32, 0x10},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
		}, false},
		"empty info": {[]Nibble{1, 2, 3}, &Account{
			address:    common.Address{0x12, 0x34},
			pathLength: 37,
		}, false},
		"wrong path length": {[]Nibble{1, 2, 3}, &Account{
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 36,
		}, false},
		"full_frozen": {[]Nibble{1, 2, 3}, &Account{
			frozen:     true,
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
			storage:    &Value{frozen: true},
		}, true},
		"partial_frozen": {[]Nibble{1, 2, 3}, &Account{
			frozen:     false,
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
			storage:    &Value{frozen: true},
		}, true},
		"inconsistent_frozen": {[]Nibble{1, 2, 3}, &Account{
			frozen:     true,
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
			storage:    &Value{frozen: false},
		}, false},
		"clean_with_dirty_hash": {[]Nibble{1, 2, 3}, &Account{
			frozen:     true,
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
			storage:    &Value{frozen: false},
			dirty:      false,
			dirtyHash:  true,
		}, false},
		"clean_hash_with_dirty_storage_hash": {[]Nibble{1, 2, 3}, &Account{
			frozen:           true,
			address:          common.Address{0x12, 0x34},
			info:             AccountInfo{Nonce: common.Nonce{1}},
			pathLength:       37,
			storage:          &Value{frozen: false},
			dirtyHash:        false,
			storageHashDirty: true,
		}, false},
		"dirts_node_with_unknown_hash_status": {[]Nibble{1, 2, 3}, &Account{
			address:    common.Address{0x12, 0x34},
			info:       AccountInfo{Nonce: common.Nonce{1}},
			pathLength: 37,
			dirty:      true,
			hashStatus: &unknownHashStatus,
		}, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			config := MptConfig{
				Hashing:                       EthereumLikeHashing,
				TrackSuffixLengthsInLeafNodes: true,
			}
			ctxt := newNodeContextWithConfig(t, ctrl, config)
			ref, node := ctxt.Build(test.setup)
			handle := node.GetViewHandle()
			defer handle.Release()

			err := handle.Get().Check(ctxt, &ref, test.path)
			if test.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !test.ok && err == nil {
				t.Errorf("expected an error but check passed")
			}
		})
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

func TestValueNode_KeyIsAccessible(t *testing.T) {
	node := ValueNode{key: common.Key{1, 2, 3}}
	if want, got := node.key, node.Key(); want != got {
		t.Errorf("invalid key produced, wanted %v, got %v", want, got)
	}
}

func TestValueNode_ValueIsAccessible(t *testing.T) {
	node := ValueNode{value: common.Value{1, 2, 3}}
	if want, got := node.value, node.Value(); want != got {
		t.Errorf("invalid value produced, wanted %v, got %v", want, got)
	}
}

func TestValueNode_SetPathLength(t *testing.T) {
	tests := []struct {
		before, after byte
	}{
		{0, 0},
		{10, 10},
		{0, 1},
		{1, 0},
		{10, 11},
		{10, 9},
	}

	for _, test := range tests {
		node := ValueNode{
			pathLength: test.before,
		}
		ref := NodeReference{}
		handle := shared.WriteHandle[Node]{}
		res, changed, err := node.setPathLength(nil, &ref, handle, test.after)
		if err != nil {
			t.Fatalf("failed to change path length: %v", err)
		}
		if want, got := (test.before != test.after), changed; want != got {
			t.Errorf("invalid changed flag, wanted %t, got %t", want, got)
		}
		if want, got := ref.Id(), res.Id(); want != got {
			t.Errorf("invalid result node reference, wanted %v, got %v", want, got)
		}
		if want, got := test.after, node.pathLength; want != got {
			t.Errorf("invalid modified path length, wanted %d, got %d", want, got)
		}
	}
}

func TestValueNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node := &ValueNode{}

	addr := common.Address{}
	path := addressToNibbles(addr)
	if _, _, err := node.GetAccount(ctxt, addr, path[:]); err == nil {
		t.Fatalf("GetAccount call should always return an error")
	}
}

func TestValueNode_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref := NewNodeReference(ValueId(12))
	node := &ValueNode{}

	if _, _, err := node.SetAccount(ctxt, &ref, shared.WriteHandle[Node]{}, addr, path[:], info); err == nil {
		t.Fatalf("SetAccount call should always return an error")
	}
}

func TestValueNode_Frozen_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	ref, node := ctxt.Build(&Value{})
	ctxt.Freeze(ref)

	handle := node.GetWriteHandle()
	defer handle.Release()
	if _, _, err := handle.Get().SetAccount(ctxt, &ref, handle, addr, path[:], info); err == nil {
		t.Fatalf("SetAccount call should always return an error")
	}
}

func TestValueNode_GetSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	key := common.Key{}

	node := &ValueNode{}
	if _, _, err := node.GetSlot(ctxt, addr, path[:], key); err == nil {
		t.Fatalf("GetSlot call should always return an error")
	}
}

func TestValueNode_SetSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	key := common.Key{}
	value := common.Value{}

	ref := NewNodeReference(ValueId(12))
	node := &ValueNode{}

	if _, _, err := node.SetSlot(ctxt, &ref, shared.WriteHandle[Node]{}, addr, path[:], key, value); err == nil {
		t.Fatalf("SetSlot call should always return an error")
	}
}

func TestValueNode_Frozen_SetSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	key := common.Key{}
	value := common.Value{}

	ref, node := ctxt.Build(&Value{})
	ctxt.Freeze(ref)

	handle := node.GetWriteHandle()
	defer handle.Release()
	if _, _, err := handle.Get().SetSlot(ctxt, &ref, handle, addr, path[:], key, value); err == nil {
		t.Fatalf("SetSlot call should always return an error")
	}
}

func TestValueNode_GetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)
	value := common.Value{1}

	key := common.Key{1}
	node := &ValueNode{value: value}

	// Case 1: the node does not contain the requested info.
	path := keyToNibbles(key)
	if res, exists, err := node.GetValue(mgr, key, path[:]); res != (common.Value{}) || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", common.Value{}, res, exists, err)
	}

	// Case 2: the node contains the requested info.
	node.key = key
	if res, exists, err := node.GetValue(mgr, key, path[:]); value != res || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", value, res, exists, err)
	}
}

func TestValueNode_SetValue_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	ref, node := ctxt.Build(&Value{key: key, value: value})
	backup, _ := ctxt.Clone(ref)

	// Update the value with the same value.
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backup, ref)
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	ref, node := ctxt.Build(&Value{key: key, value: value})
	ctxt.Freeze(ref)
	backup, _ := ctxt.Clone(ref)

	// Update the value with the same value.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value)
	if newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backup, ref)
	ctxt.ExpectEqualTries(t, backup, newRoot)
}

func TestValueNode_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	ref, node := ctxt.Build(&Value{key: key, value: value1})
	after, _ := ctxt.Build(&Value{key: key, value: value2, dirty: true, dirtyHash: true})

	handle := node.GetWriteHandle()

	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value2); newRoot != ref || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	before, _ := ctxt.Build(&Value{key: key, value: value1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Value{key: key, value: value2, dirty: true, dirtyHash: true})

	ctxt.ExpectCreateValue()

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	ref, node := ctxt.Build(&Value{key: key, value: value1})
	after, _ := ctxt.Build(Empty{})

	ctxt.EXPECT().release(&ref).Return(nil)

	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value2); !newRoot.Id().IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, NewNodeReference(EmptyId()))
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key: key, value: value1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{2}

	ref, node := ctxt.Build(&Value{key: key1, value: value1})

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Value{key: key1, value: value1},
		3: &Value{key: key2, value: value2, dirty: true, dirtyHash: true},
	}, dirtyChildHashes: []int{3}, dirty: true, dirtyHash: true})

	// This operation creates one new value node and a branch.
	res, _ := ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, res)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_NoCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{2}

	before, _ := ctxt.Build(&Value{key: key1, value: value1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Value{key: key1, value: value1, frozen: true},
		3: &Value{key: key2, value: value2, dirty: true, dirtyHash: true},
	}, dirtyChildHashes: []int{3}, dirty: true, dirtyHash: true, frozenChildren: []int{2}})

	// This operation creates one new value node and a branch.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{2}

	ref, node := ctxt.Build(&Value{key: key1, value: value1})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Value{key: key1, value: value1},
			0xB: &Value{key: key2, value: value2, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xB}, dirty: true, dirtyHash: true},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates one new value, branch, and extension node.
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, res)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_WithCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{2}

	before, _ := ctxt.Build(&Value{key: key1, value: value1})

	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Value{key: key1, value: value1, frozen: true},
			0xB: &Value{key: key2, value: value2, dirty: true, dirtyHash: true},
		}, dirtyChildHashes: []int{0xB}, dirty: true, dirtyHash: true, frozenChildren: []int{0xA}},
		dirty:         true,
		hashDirty:     true,
		nextHashDirty: true,
	})

	// This operation creates one new value, branch, and extension node.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	ref, node := ctxt.Build(&Value{key: key1, value: value1})
	after, _ := ctxt.Clone(ref)

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key: key1, value: value1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Value{key: key1, value: value1, frozen: true})

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != ref {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	ref, node := ctxt.Build(&Value{key: key1, value: value1})
	after, _ := ctxt.Clone(ref)

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2); newRoot != ref || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", ref, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, ref)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key: key1, value: value1})
	ctxt.Freeze(before)
	ref, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Value{key: key1, value: value1, frozen: true})

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, &ref, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != ref {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, ref)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_ClearStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)

	ref := NewNodeReference(ValueId(12))
	node := &ValueNode{}

	if _, _, err := node.ClearStorage(ctxt, &ref, shared.WriteHandle[Node]{}, addr, path[:]); err == nil {
		t.Fatalf("ClearStorage call should always return an error")
	}
}

func TestValueNode_Frozen_ClearStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)

	ref, node := ctxt.Build(&Value{})
	ctxt.Freeze(ref)

	handle := node.GetWriteHandle()
	defer handle.Release()
	if _, _, err := handle.Get().ClearStorage(ctxt, &ref, handle, addr, path[:]); err == nil {
		t.Fatalf("ClearStorage call should always return an error")
	}
}

func TestValueNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(&Value{})

	ctxt.EXPECT().release(&ref).Return(nil)
	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
	handle.Release()
}

func TestValueNode_Frozen_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref, node := ctxt.Build(&Value{})
	ctxt.Freeze(ref)

	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
	handle.Release()
}

func TestValueNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node := &ValueNode{}

	if node.frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := node.Freeze(ctxt, shared.WriteHandle[Node]{}); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !node.frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

func TestValueNode_Visit(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	visitor := NewMockNodeVisitor(ctrl)

	ref, node := ctxt.Build(&Value{})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth2}).Return(VisitResponseContinue)
	depth4 := 4
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth4}).Return(VisitResponseAbort)
	depth6 := 6
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id(), Depth: &depth6}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, &ref, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got (%v, %v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, &ref, 4, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got (%v, %v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, &ref, 6, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got (%v, %v)", abort, err)
	}
}

func TestValueNode_CheckDetectsIssues(t *testing.T) {
	unknownHashStatus := hashStatusUnknown
	tests := map[string]struct {
		path  []Nibble
		setup NodeDesc
		ok    bool
	}{
		"ok": {[]Nibble{1, 2, 3}, &Value{
			key:    common.Key{0x12, 0x34},
			value:  common.Value{1},
			length: 61,
		}, true},
		"wrong location": {[]Nibble{1, 2, 3}, &Value{
			key:    common.Key{0x43, 0x21},
			value:  common.Value{1},
			length: 61,
		}, false},
		"zero value": {[]Nibble{1, 2, 3}, &Value{
			key:    common.Key{0x12, 0x34},
			value:  common.Value{},
			length: 61,
		}, false},
		"wrong path length": {[]Nibble{1, 2, 3}, &Value{
			key:    common.Key{0x12, 0x34},
			value:  common.Value{1},
			length: 37,
		}, false},
		"clean with dirty hash": {[]Nibble{1, 2, 3}, &Value{
			key:       common.Key{0x12, 0x34},
			value:     common.Value{1},
			length:    37,
			dirty:     false,
			dirtyHash: true,
		}, false},
		"dirts_node_with_unknown_hash_status": {[]Nibble{1, 2, 3}, &Value{
			key:        common.Key{0x12, 0x34},
			value:      common.Value{1},
			length:     61,
			dirty:      true,
			hashStatus: &unknownHashStatus,
		}, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			config := MptConfig{
				Hashing:                       EthereumLikeHashing,
				TrackSuffixLengthsInLeafNodes: true,
			}
			ctxt := newNodeContextWithConfig(t, ctrl, config)
			ref, node := ctxt.Build(test.setup)
			handle := node.GetViewHandle()
			defer handle.Release()

			err := handle.Get().Check(ctxt, &ref, test.path)
			if test.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !test.ok && err == nil {
				t.Errorf("expected an error but check passed")
			}
		})
	}
}

// ----------------------------------------------------------------------------
//                             CheckForest
// ----------------------------------------------------------------------------

func TestCheckForest_DetectsIssuesInTrees(t *testing.T) {
	tests := map[string]struct {
		tree NodeDesc
		ok   bool
	}{
		"ok empty": {&Empty{}, true},
		"ok nested": {&Branch{children: Children{
			1: &Account{address: common.Address{0x12}, info: AccountInfo{Nonce: common.Nonce{1}}},
			4: &Account{address: common.Address{0x45}, info: AccountInfo{Nonce: common.Nonce{1}}},
		}}, true},
		"top level issue": {&Branch{children: Children{
			// not enough children
			1: &Account{address: common.Address{0x12}, info: AccountInfo{Nonce: common.Nonce{1}}},
		}}, false},
		"nested issue": {&Branch{children: Children{
			1: &Account{address: common.Address{0x12}, info: AccountInfo{Nonce: common.Nonce{1}}},
			4: &Account{address: common.Address{0x45}, info: AccountInfo{}}, // empty info
		}}, false},
		"value node reachable without account": {&Branch{children: Children{
			1: &Value{key: common.Key{0x12}, value: common.Value{1}},
			4: &Value{key: common.Key{0x45}, value: common.Value{2}},
		}}, false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)
			ref, _ := ctxt.Build(test.tree)

			err := CheckForest(ctxt, []*NodeReference{&ref})
			if test.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !test.ok && err == nil {
				t.Errorf("expected an error but check passed")
			}
		})
	}
}

func TestCheckForest_AcceptsValidReUse(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref1, node1 := ctxt.Build(&Branch{children: Children{
		1: &Account{address: common.Address{0x12}, info: AccountInfo{Nonce: common.Nonce{1}}},
	}})

	ref2, node2 := ctxt.Build(&Branch{children: Children{
		8: &Account{address: common.Address{0x82}, info: AccountInfo{Nonce: common.Nonce{1}}},
	}})

	// integrate shared mock node into both trees
	node := NewMockNode(ctrl)
	node.EXPECT().IsFrozen().AnyTimes().Return(false)
	node.EXPECT().Check(gomock.Any(), gomock.Any(), gomock.Any()) // shared node is only checked once
	refMock, _ := ctxt.Build(&Mock{node})

	handle := node1.GetWriteHandle()
	handle.Get().(*BranchNode).children[4] = refMock
	handle.Release()

	handle = node2.GetWriteHandle()
	handle.Get().(*BranchNode).children[4] = refMock
	handle.Release()

	if err := CheckForest(ctxt, []*NodeReference{&ref1, &ref2}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckForest_DetectsInvalidReUse(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	ref1, _ := ctxt.Build(&Branch{children: Children{
		1: &Account{address: common.Address{0x12}, info: AccountInfo{Nonce: common.Nonce{1}}},
		4: &Tag{"A", &Account{address: common.Address{0x45}, info: AccountInfo{Nonce: common.Nonce{1}}}},
	}})

	ref2, _ := ctxt.Get("A")

	err := CheckForest(ctxt, []*NodeReference{&ref1, &ref2})
	if err == nil || !strings.Contains(err.Error(), "invalid reuse") {
		t.Errorf("expected an invalid reuse error but got: %v", err)
	}
}

// ----------------------------------------------------------------------------
//                              HashStatus
// ----------------------------------------------------------------------------

func TestHashStatus_Print(t *testing.T) {
	tests := []struct {
		status hashStatus
		print  string
	}{
		{hashStatusClean, "clean"},
		{hashStatusDirty, "dirty"},
		{hashStatusUnknown, "unknown"},
		{hashStatus(12), "unknown"},
	}

	for _, test := range tests {
		if want, got := test.print, test.status.String(); want != got {
			t.Errorf("unexpected print, wanted %v, got %v", want, got)
		}
	}
}

// ----------------------------------------------------------------------------
//                               Encoders
// ----------------------------------------------------------------------------

func TestAccountNodeEncoderWithNodeHash(t *testing.T) {
	node := AccountNode{
		nodeBase: nodeBase{
			hash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			hashStatus: hashStatusClean,
		},
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage: NewNodeReference(NodeId(12)),
	}
	encoder := AccountNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	node.storageHashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestAccountNodeEncoderWithChildHash(t *testing.T) {
	node := AccountNode{
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage:     NewNodeReference(NodeId(12)),
		storageHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}
	encoder := AccountNodeEncoderWithChildHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	node.hashStatus = hashStatusUnknown
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}
func TestAccountNodeWithPathLengthEncoderWithNodeHash(t *testing.T) {
	node := AccountNode{
		nodeBase: nodeBase{
			hash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			hashStatus: hashStatusClean,
		},
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage:    NewNodeReference(NodeId(12)),
		pathLength: 14,
	}
	encoder := AccountNodeWithPathLengthEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	node.storageHashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestBranchNodeEncoderWithChildHashes(t *testing.T) {
	node := BranchNode{
		children: [16]NodeReference{
			NewNodeReference(1),
			NewNodeReference(2),
			NewNodeReference(3),
			NewNodeReference(4),
			NewNodeReference(5),
			NewNodeReference(6),
			NewNodeReference(7),
			NewNodeReference(8),
			NewNodeReference(9),
			NewNodeReference(10),
			NewNodeReference(11),
			NewNodeReference(12),
			NewNodeReference(13),
			NewNodeReference(14),
			NewNodeReference(15),
			NewNodeReference(16),
		},
		hashes:           [16]common.Hash{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}},
		embeddedChildren: 12,
	}
	encoder := BranchNodeEncoderWithChildHashes{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := BranchNode{}
	encoder.Load(buffer, &recovered)
	node.hashStatus = hashStatusUnknown
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestBranchNodeEncoderWithNodeHash(t *testing.T) {
	node := BranchNode{
		nodeBase: nodeBase{
			hash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			hashStatus: hashStatusClean,
		},
		children: [16]NodeReference{
			NewNodeReference(1),
			NewNodeReference(2),
			NewNodeReference(3),
			NewNodeReference(4),
			NewNodeReference(5),
			NewNodeReference(6),
			NewNodeReference(7),
			NewNodeReference(8),
			NewNodeReference(9),
			NewNodeReference(10),
			NewNodeReference(11),
			NewNodeReference(12),
			NewNodeReference(13),
			NewNodeReference(14),
			NewNodeReference(15),
			NewNodeReference(16),
		},
		embeddedChildren: 12,
	}
	encoder := BranchNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := BranchNode{}
	encoder.Load(buffer, &recovered)
	node.dirtyHashes = ^uint16(0)
	node.embeddedChildren = 0
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestExtensionNodeEncoderWithChildHash(t *testing.T) {
	node := ExtensionNode{
		path: Path{
			path:   [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			length: 7,
		},
		next:           NewNodeReference(NodeId(12)),
		nextHash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		nextIsEmbedded: true,
	}
	encoder := ExtensionNodeEncoderWithChildHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ExtensionNode{}
	encoder.Load(buffer, &recovered)
	node.hashStatus = hashStatusUnknown
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}
func TestExtensionNodeEncoderWithNodeHash(t *testing.T) {
	node := ExtensionNode{
		nodeBase: nodeBase{
			hash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			hashStatus: hashStatusClean,
		},
		path: Path{
			path:   [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			length: 7,
		},
		next:           NewNodeReference(NodeId(12)),
		nextIsEmbedded: true,
	}
	encoder := ExtensionNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ExtensionNode{}
	encoder.Load(buffer, &recovered)
	node.nextHashDirty = true
	node.nextIsEmbedded = false
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeEncoderWithoutNodeHash(t *testing.T) {
	node := ValueNode{
		key:   common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value: common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}
	encoder := ValueNodeEncoderWithoutNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	node.hashStatus = hashStatusUnknown
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeEncoderWithNodeHash(t *testing.T) {
	node := ValueNode{
		nodeBase: nodeBase{
			hash:       common.Hash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			hashStatus: hashStatusClean,
		},
		key:   common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value: common.Value{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33},
	}
	encoder := ValueNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeWithPathLengthEncoderWithoutNodeHash(t *testing.T) {
	node := ValueNode{
		key:        common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value:      common.Value{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33},
		pathLength: 12,
	}
	encoder := ValueNodeWithPathLengthEncoderWithoutNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	node.hashStatus = hashStatusUnknown
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeWithPathLengthEncoderWithNodeHash(t *testing.T) {
	node := ValueNode{
		nodeBase: nodeBase{
			hash:       common.Hash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			hashStatus: hashStatusClean,
		},
		key:        common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value:      common.Value{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33},
		pathLength: 12,
	}
	encoder := ValueNodeWithPathLengthEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

// ----------------------------------------------------------------------------
//                               Transitions
// ----------------------------------------------------------------------------

type nodeType int

const (
	ntAccount nodeType = iota
	ntBranch
	ntEmpty
	ntExtension
	ntValue
)

func (t nodeType) String() string {
	switch t {
	case ntAccount:
		return "Account"
	case ntBranch:
		return "Branch"
	case ntEmpty:
		return "Empty"
	case ntExtension:
		return "Extension"
	case ntValue:
		return "Value"
	}
	return "unknown"
}

type operationType int

const (
	otSetAccount operationType = iota
	otSetValue
	otClearStorage
)

func (t operationType) String() string {
	switch t {
	case otSetAccount:
		return "SetAccount"
	case otSetValue:
		return "SetValue"
	case otClearStorage:
		return "ClearStorage"
	}
	return "unknown"
}

type transition struct {
	node        nodeType
	operation   operationType
	description string
	before      NodeDesc
	change      func(*trie) (NodeReference, bool, error)
	after       NodeDesc
}

func (t *transition) getLabel() string {
	return fmt.Sprintf("%s/%s/%s", t.node, t.operation, t.description)
}

func (t *transition) apply(mgr NodeManager, root NodeReference) (NodeReference, bool, error) {
	return t.change(&trie{mgr, root})
}

type trie struct {
	manager NodeManager
	root    NodeReference
}

func (t *trie) GetAccount(addr common.Address) (AccountInfo, bool, error) {
	handle, err := t.manager.getReadAccess(&t.root)
	if err != nil {
		return AccountInfo{}, false, err
	}
	defer handle.Release()
	path := addressToNibbles(addr)
	return handle.Get().GetAccount(t.manager, addr, path)
}

func (t *trie) SetAccount(addr common.Address, info AccountInfo) (NodeReference, bool, error) {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	path := addressToNibbles(addr)
	return handle.Get().SetAccount(t.manager, &t.root, handle, addr, path, info)
}

func (t *trie) GetValue(addr common.Address, key common.Key) (common.Value, bool, error) {
	handle, err := t.manager.getReadAccess(&t.root)
	if err != nil {
		return common.Value{}, false, err
	}
	defer handle.Release()
	path := addressToNibbles(addr)
	return handle.Get().GetSlot(t.manager, addr, path, key)
}

func (t *trie) SetValue(addr common.Address, key common.Key, value common.Value) (NodeReference, bool, error) {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	path := addressToNibbles(addr)
	return handle.Get().SetSlot(t.manager, &t.root, handle, addr, path, key, value)
}

func (t *trie) ClearStorage(addr common.Address) (NodeReference, bool, error) {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	path := addressToNibbles(addr)
	return handle.Get().ClearStorage(t.manager, &t.root, handle, addr, path)
}

func (t *trie) Check() error {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Check(t.manager, &t.root, []Nibble{})
}

func (t *trie) CheckForest() error {
	return CheckForest(t.manager, []*NodeReference{&t.root})
}

func (t *trie) Dump() error {
	handle, err := t.manager.getViewAccess(&t.root)
	if err != nil {
		return err
	}
	defer handle.Release()
	// The output of the dump is a side effect ignored in the context of this
	// test operation on tries. The relevant part is that (a) it does not cause
	// a panic and (b) that errors encountered while dumping the node are
	// reported correctly.
	out := &bytes.Buffer{}
	return handle.Get().Dump(out, t.manager, &t.root, "")
}

func (t *trie) Visit() error {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return err
	}
	defer handle.Release()
	_, err = handle.Get().Visit(t.manager, &t.root, 0,
		MakeVisitor(func(Node, NodeInfo) VisitResponse {
			return VisitResponseContinue
		}),
	)
	return err
}

func (t *trie) Release() error {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Release(t.manager, &t.root, handle)
}

func (t *trie) Freeze() error {
	handle, err := t.manager.getWriteAccess(&t.root)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Freeze(t.manager, handle)
}

func getTestTransitions() []transition {
	res := []transition{}

	// --- Accounts ---

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "no_change",
		before: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{5}, AccountInfo{Nonce: common.Nonce{1, 2, 3}})
		},
		after: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "update",
		before: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{5}, AccountInfo{Nonce: common.Nonce{3, 2, 1}})
		},
		after: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{3, 2, 1}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "delete",
		before: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{5}, AccountInfo{})
		},
		after: &Empty{},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "new_empty_account",
		before: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{2}, AccountInfo{})
		},
		after: &Account{
			address: common.Address{5},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "create_sibling_no_common_prefix",

		before: &Account{
			address: common.Address{0x12, 0x34},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x43, 0x21}, AccountInfo{Nonce: common.Nonce{3, 2, 1}})
		},
		after: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12, 0x34},
				info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			},
			4: &Account{
				address: common.Address{0x43, 0x21},
				info:    AccountInfo{Nonce: common.Nonce{3, 2, 1}},
			},
		}},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "create_sibling_with_common_prefix_length_1",
		before: &Account{
			address: common.Address{0x12, 0x34},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x14, 0x21}, AccountInfo{Nonce: common.Nonce{3, 2, 1}})
		},
		after: &Extension{
			path: []Nibble{1},
			next: &Branch{children: Children{
				2: &Account{
					address: common.Address{0x12, 0x34},
					info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
				},
				4: &Account{
					address: common.Address{0x14, 0x21},
					info:    AccountInfo{Nonce: common.Nonce{3, 2, 1}},
				},
			}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetAccount,
		description: "create_sibling_with_common_prefix_length_2",
		before: &Account{
			address: common.Address{0x12, 0x34},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x43}, AccountInfo{Nonce: common.Nonce{3, 2, 1}})
		},
		after: &Extension{
			path: []Nibble{1, 2},
			next: &Branch{children: Children{
				3: &Account{
					address: common.Address{0x12, 0x34},
					info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
				},
				4: &Account{
					address: common.Address{0x12, 0x43},
					info:    AccountInfo{Nonce: common.Nonce{3, 2, 1}},
				},
			}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "no_change",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{2}, common.Key{2}, common.Value{3})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "creating_storage",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{2}, common.Value{3})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "updating_storage",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{2}, common.Value{4})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{4},
			},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "extending_storage",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{4}, common.Value{5})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Extension{path: []Nibble{0}, next: &Branch{children: Children{
				2: &Value{key: common.Key{2}, value: common.Value{3}},
				4: &Value{key: common.Key{4}, value: common.Value{5}},
			}}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "shrink_storage",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Extension{path: []Nibble{0}, next: &Branch{children: Children{
				2: &Value{key: common.Key{2}, value: common.Value{3}},
				4: &Value{key: common.Key{4}, value: common.Value{5}},
			}}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{4}, common.Value{0})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otSetValue,
		description: "deleting_storage",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{2}, common.Value{0})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otClearStorage,
		description: "clear_storage_hit",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{1})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
		},
	})

	res = append(res, transition{
		node:        ntAccount,
		operation:   otClearStorage,
		description: "clear_storage_miss",
		before: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{2})
		},
		after: &Account{
			address: common.Address{1},
			info:    AccountInfo{Nonce: common.Nonce{1, 2, 3}},
			storage: &Value{
				key:   common.Key{2},
				value: common.Value{3},
			},
		},
	})

	// --- Branches ---

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "no_change",
		before: &Branch{children: Children{
			4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{Nonce: common.Nonce{1, 2}})
		},
		after: &Branch{children: Children{
			4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "update",
		before: &Branch{children: Children{
			4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{Nonce: common.Nonce{2, 3}})
		},
		after: &Branch{children: Children{
			4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{2, 3}}},
			7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "new_child",
		before: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0xA3}, AccountInfo{Nonce: common.Nonce{5, 6}})
		},
		after: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
			0xA: &Account{address: common.Address{0xA3}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "split_child",
		before: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x46}, AccountInfo{Nonce: common.Nonce{5, 6}})
		},
		after: &Branch{children: Children{
			0x4: &Branch{
				children: Children{
					0x2: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
					0x6: &Account{address: common.Address{0x46}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
				},
			},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_child_from_more_than_two_children",
		before: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
			0xB: &Account{address: common.Address{0xB4}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{})
		},
		after: &Branch{children: Children{
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
			0xB: &Account{address: common.Address{0xB4}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_causing_replacement_by_leaf",
		before: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{})
		},
		after: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_causing_replacement_by_extension",
		before: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2}}},
			0x7: &Branch{children: Children{
				3: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
				5: &Account{address: common.Address{0x75}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
			}},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{7}, next: &Branch{children: Children{
			3: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{3, 4}}},
			5: &Account{address: common.Address{0x75}, info: AccountInfo{Nonce: common.Nonce{5, 6}}},
		}}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetValue,
		description: "no_change",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x42}, common.Value{1, 2})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetValue,
		description: "update",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x42}, common.Value{2, 3})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{2, 3}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "new_child",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0xA3}, common.Value{5, 6})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				0x7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
				0xA: &Value{key: common.Key{0xA3}, value: common.Value{5, 6}},
			}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "split_child",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x46}, common.Value{5, 6})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x4: &Branch{
					children: Children{
						0x2: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
						0x6: &Value{key: common.Key{0x46}, value: common.Value{5, 6}},
					},
				},
				0x7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_child_from_more_than_two_children",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				0x7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
				0xB: &Value{key: common.Key{0xB4}, value: common.Value{5, 6}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x42}, common.Value{})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
				0xB: &Value{key: common.Key{0xB4}, value: common.Value{5, 6}},
			}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_causing_replacement_by_leave",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				0x7: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x42}, common.Value{})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otSetAccount,
		description: "delete_causing_replacement_by_extension",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Branch{children: Children{
				0x4: &Value{key: common.Key{0x42}, value: common.Value{1, 2}},
				0x7: &Branch{children: Children{
					3: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
					5: &Value{key: common.Key{0x75}, value: common.Value{5, 6}},
				}},
			}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x42}, common.Value{})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Extension{path: []Nibble{7}, next: &Branch{children: Children{
				3: &Value{key: common.Key{0x73}, value: common.Value{3, 4}},
				5: &Value{key: common.Key{0x75}, value: common.Value{5, 6}},
			}}},
		},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otClearStorage,
		description: "clear_storage_missing_account",
		before: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{0x12})
		},
		after: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}},
	})

	res = append(res, transition{
		node:        ntBranch,
		operation:   otClearStorage,
		description: "clear_storage_existing_account",
		before: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{0x73})
		},
		after: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
			},
		}},
	})

	// --- Empty ---

	res = append(res, transition{
		node:        ntEmpty,
		operation:   otSetAccount,
		description: "no_change",
		before:      &Empty{},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{})
		},
		after: &Empty{},
	})

	res = append(res, transition{
		node:        ntEmpty,
		operation:   otSetAccount,
		description: "create_account",
		before:      &Empty{},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x42}, AccountInfo{Nonce: common.Nonce{1, 2, 3}})
		},
		after: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1, 2, 3}}},
	})

	res = append(res, transition{
		node:        ntEmpty,
		operation:   otSetValue,
		description: "no_change",
		before:      &Empty{},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1}, common.Key{2}, common.Value{3})
		},
		after: &Empty{},
	})

	res = append(res, transition{
		node:        ntEmpty,
		operation:   otClearStorage,
		description: "no_change",
		before:      &Empty{},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{1})
		},
		after: &Empty{},
	})

	// --- Extension ---

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "no_change",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x34, 0x56}, AccountInfo{Nonce: common.Nonce{1}})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_zero_common_prefix_length_0",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x23}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_zero_common_prefix_length_1",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x13}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_zero_common_prefix_length_2",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x40}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_zero_common_prefix_length_3",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x30}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_non_zero_common_prefix_length_0",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x78}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Branch{children: Children{
			1: &Extension{path: []Nibble{2, 3}, next: &Branch{children: Children{
				4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
				7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
			}}},
			7: &Account{address: common.Address{0x78}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_non_zero_common_prefix_length_1",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x18}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			2: &Extension{path: []Nibble{3}, next: &Branch{children: Children{
				4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
				7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
			}}},
			8: &Account{address: common.Address{0x18}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_non_zero_common_prefix_length_2",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x84}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			3: &Branch{children: Children{
				4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
				7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
			}},
			8: &Account{address: common.Address{0x12, 0x84}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_non_zero_common_prefix_length_3",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x39, 0xAB}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
			9: &Account{address: common.Address{0x12, 0x39, 0xAB}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_into_short_extension_non_zero_common_prefix_length_0",
		before: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x14}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x17}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x78}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Branch{children: Children{
			1: &Branch{children: Children{
				4: &Account{address: common.Address{0x14}, info: AccountInfo{Nonce: common.Nonce{1}}},
				7: &Account{address: common.Address{0x17}, info: AccountInfo{Nonce: common.Nonce{2}}},
			}},
			7: &Account{address: common.Address{0x78}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "insert_into_short_extension_non_zero_common_prefix_length_1",
		before: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x14}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x17}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x18}, AccountInfo{Nonce: common.Nonce{3}})
		},
		after: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x14}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x17}, info: AccountInfo{Nonce: common.Nonce{2}}},
			8: &Account{address: common.Address{0x18}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "delete_causing_collapse",
		before: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
			4: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			7: &Account{address: common.Address{0x12, 0x37, 0x89}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x37, 0x89}, AccountInfo{})
		},
		after: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetAccount,
		description: "delete_causing_merge",
		before: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			3: &Extension{path: []Nibble{4, 5}, next: &Branch{children: Children{
				6: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
				8: &Account{address: common.Address{0x12, 0x34, 0x58}, info: AccountInfo{Nonce: common.Nonce{2}}},
			}}},
			7: &Account{address: common.Address{0x12, 0x70}, info: AccountInfo{Nonce: common.Nonce{3}}},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetAccount(common.Address{0x12, 0x70}, AccountInfo{})
		},
		after: &Extension{path: []Nibble{1, 2, 3, 4, 5}, next: &Branch{children: Children{
			6: &Account{address: common.Address{0x12, 0x34, 0x56}, info: AccountInfo{Nonce: common.Nonce{1}}},
			8: &Account{address: common.Address{0x12, 0x34, 0x58}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otSetValue,
		description: "insert_value_common_prefix_length_2",
		before: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Extension{path: []Nibble{1, 2, 3}, next: &Branch{children: Children{
				4: &Value{key: common.Key{0x12, 0x34, 0x56}, value: common.Value{1}},
				7: &Value{key: common.Key{0x12, 0x37, 0x89}, value: common.Value{2}},
			}}},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{1, 2}, common.Key{0x12, 0x40}, common.Value{3})
		},
		after: &Account{
			address: common.Address{1, 2},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
				0x3: &Branch{children: Children{
					4: &Value{key: common.Key{0x12, 0x34, 0x56}, value: common.Value{1}},
					7: &Value{key: common.Key{0x12, 0x37, 0x89}, value: common.Value{2}},
				}},
				0x4: &Value{key: common.Key{0x12, 0x40}, value: common.Value{3}},
			}}},
		},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otClearStorage,
		description: "clear_storage_missing_account",
		before: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x12, 0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{0x12, 0x34})
		},
		after: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x12, 0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}}},
	})

	res = append(res, transition{
		node:        ntExtension,
		operation:   otClearStorage,
		description: "clear_storage_existing_account",
		before: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Value{key: common.Key{0x12}, value: common.Value{1}},
			},
			0x7: &Account{
				address: common.Address{0x12, 0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}}},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.ClearStorage(common.Address{0x12, 0x42})
		},
		after: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
			},
			0x7: &Account{
				address: common.Address{0x12, 0x73},
				info:    AccountInfo{Nonce: common.Nonce{2}},
				storage: &Value{key: common.Key{0x34}, value: common.Value{2}},
			},
		}}},
	})

	// --- Values ---

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "no_change",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{5}, common.Value{1, 2, 3})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "update",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{5}, common.Value{3, 2, 1})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{3, 2, 1},
			},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "delete",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{5}, common.Value{})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "new_zero_value",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{8}, common.Value{})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{5},
				value: common.Value{1, 2, 3},
			},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "new_sibling_with_common_prefix_length_0",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{0x12, 0x34},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{0x43, 0x21}, common.Value{3, 2, 1})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Branch{children: Children{
				1: &Value{
					key:   common.Key{0x12, 0x34},
					value: common.Value{1, 2, 3},
				},
				4: &Value{
					key:   common.Key{0x43, 0x21},
					value: common.Value{3, 2, 1},
				},
			}},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "new_sibling_with_common_prefix_length_1",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{0x12, 0x34},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{0x14, 0x21}, common.Value{3, 2, 1})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Extension{
				path: []Nibble{1},
				next: &Branch{children: Children{
					2: &Value{
						key:   common.Key{0x12, 0x34},
						value: common.Value{1, 2, 3},
					},
					4: &Value{
						key:   common.Key{0x14, 0x21},
						value: common.Value{3, 2, 1},
					},
				}},
			},
		},
	})

	res = append(res, transition{
		node:        ntValue,
		operation:   otSetValue,
		description: "new_sibling_with_common_prefix_length_2",
		before: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Value{
				key:   common.Key{0x12, 0x34},
				value: common.Value{1, 2, 3},
			},
		},
		change: func(trg *trie) (NodeReference, bool, error) {
			return trg.SetValue(common.Address{}, common.Key{0x12, 0x43}, common.Value{3, 2, 1})
		},
		after: &Account{
			info: AccountInfo{Balance: common.Balance{1}},
			storage: &Extension{
				path: []Nibble{1, 2},
				next: &Branch{children: Children{
					3: &Value{
						key:   common.Key{0x12, 0x34},
						value: common.Value{1, 2, 3},
					},
					4: &Value{
						key:   common.Key{0x12, 0x43},
						value: common.Value{3, 2, 1},
					},
				}},
			},
		},
	})

	return res
}

func getTestStates() []NodeDesc {
	res := []NodeDesc{}
	for _, cur := range getTestTransitions() {
		res = append(res, cur.before)
		res = append(res, cur.after)
	}
	for _, cur := range getTestActions() {
		res = append(res, cur.input)
	}
	return res
}

func TestTransitions_TestForMissingTransitions(t *testing.T) {
	allNodeTypes := []nodeType{
		ntAccount,
		ntBranch,
		ntEmpty,
		ntExtension,
		ntValue,
	}

	allOperationTypes := []operationType{
		otSetAccount,
		otSetValue,
		otClearStorage,
	}

	transitions := getTestTransitions()
	for _, nodeType := range allNodeTypes {
		for _, opType := range allOperationTypes {
			if nodeType == ntValue {
				// Setting an account or clearing storage is not defined for this node type.
				if opType == otSetAccount || opType == otClearStorage {
					continue
				}
			}
			found := false
			for _, cur := range transitions {
				if cur.node == nodeType && cur.operation == opType {
					found = true
				}
			}
			if !found {
				t.Errorf("Missing transition for node type %v and operation %v", nodeType, opType)
			}
		}
	}
}

func TestTransitions_TestStatesAreValid(t *testing.T) {
	for _, transition := range getTestTransitions() {
		transition := transition
		t.Run(transition.getLabel(), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, _ := ctxt.Build(transition.before)
			ctxt.Check(t, ref)

			ref, _ = ctxt.Build(transition.after)
			ctxt.Check(t, ref)
		})
	}
}

func TestTransitions_BeforeAndAfterStatesCanBeFrozen(t *testing.T) {
	for _, transition := range getTestTransitions() {
		transition := transition
		t.Run(transition.getLabel(), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, _ := ctxt.Build(transition.before)
			ctxt.Check(t, ref)
			ctxt.Freeze(ref)
			ctxt.Check(t, ref)

			ref, _ = ctxt.Build(transition.after)
			ctxt.Check(t, ref)
			ctxt.Freeze(ref)
			ctxt.Check(t, ref)
		})
	}
}

func TestTransitions_StatesAreReleasable(t *testing.T) {
	for _, state := range getTestStates() {
		ctrl := gomock.NewController(t)
		ctxt := newNiceNodeContext(t, ctrl)

		ref, node := ctxt.Build(state)
		handle := node.GetWriteHandle()
		if err := handle.Get().Release(ctxt, &ref, handle); err != nil {
			t.Errorf("error during release: %v", err)
		}
		handle.Release()
	}
}

func TestTransitions_StatesAreDumpable(t *testing.T) {
	for _, state := range getTestStates() {
		ctrl := gomock.NewController(t)
		ctxt := newNodeContext(t, ctrl)

		ref, node := ctxt.Build(state)
		handle := node.GetViewHandle()
		buffer := &bytes.Buffer{}
		if err := handle.Get().Dump(buffer, ctxt, &ref, ""); err != nil {
			t.Errorf("failed to dump node: %v", err)
		}
		handle.Release()

		str := buffer.String()
		if len(str) == 0 {
			t.Errorf("dump for node should not be empty")
		}
	}
}

func TestTransitions_MutableTransitionHaveExpectedEffect(t *testing.T) {
	testTransitions_MutableTransitionHaveExpectedEffect(t, S4LiveConfig)
}

func TestTransitions_MutableTransitionHaveExpectedEffectWithTrackedPrefixLength(t *testing.T) {
	config := S4LiveConfig
	config.TrackSuffixLengthsInLeafNodes = true
	testTransitions_MutableTransitionHaveExpectedEffect(t, config)
}

func TestTransitions_MutableTransitionHaveExpectedEffectWithEthereumHashing(t *testing.T) {
	config := S4LiveConfig
	config.TrackSuffixLengthsInLeafNodes = true
	config.Hashing = EthereumLikeHashing
	testTransitions_MutableTransitionHaveExpectedEffect(t, config)
}

func testTransitions_MutableTransitionHaveExpectedEffect(t *testing.T, config MptConfig) {
	for _, transition := range getTestTransitions() {
		transition := transition
		t.Run(transition.getLabel(), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContextWithConfig(t, ctrl, config)

			if config.TrackSuffixLengthsInLeafNodes {
				fixPrefixLength(transition.before, 0)
				fixPrefixLength(transition.after, 0)
			}

			before, _ := ctxt.Build(transition.before)
			original, _ := ctxt.Clone(before)
			after, changed, err := transition.apply(ctxt, before)
			if err != nil {
				t.Fatalf("failed to apply transition: %v", err)
			}
			ctxt.Check(t, after)

			if want, got := !ctxt.equalTries(original, before), changed; want != got {
				t.Errorf("unexpected 'changed' result, wanted %t, got %t", want, got)
			}

			want, _ := ctxt.Build(transition.after)
			markModifiedAsDirty(t, ctxt, original, want)
			ctxt.Check(t, want)

			ctxt.ExpectEqualTries(t, want, after)
		})
	}
}

func fixPrefixLength(desc NodeDesc, depth byte) {
	if desc == nil {
		return
	}
	switch n := desc.(type) {
	case *Empty: /* nothing */
	case *Branch:
		for _, child := range n.children {
			fixPrefixLength(child, depth+1)
		}
	case *Extension:
		fixPrefixLength(n.next, depth+byte(len(n.path)))
	case *Account:
		n.pathLength = 40 - depth
		fixPrefixLength(n.storage, 0)
	case *Value:
		n.length = 64 - depth
	default:
		panic(fmt.Sprintf("unsupported node description type: %v", reflect.TypeOf(desc)))
	}
}

func markModifiedAsDirty(t *testing.T, ctxt *nodeContext, before, after NodeReference) {
	t.Helper()

	// Collect all nodes that have been there before.
	allPreexistingNodes, err := getAllReachableNodes(ctxt, before)
	if err != nil {
		t.Fatalf("failed to collect preexisting nodes: %v", err)
	}
	isReused := func(n Node) bool {
		equalConfig := equalityConfig{
			ignoreDirtyFlag: true,
			ignoreDirtyHash: true,
			ignoreFreeze:    true,
		}
		for _, cur := range allPreexistingNodes {
			if ctxt.equalWithConfig(cur, n, equalConfig) {
				return true
			}
		}
		return false
	}

	handle, _ := ctxt.getViewAccess(&after)
	handle.Get().Visit(ctxt, &after, 0, MakeVisitor(func(n Node, i NodeInfo) VisitResponse {
		// If the current node is not equivalent to a node that was present before,
		// then it is a new node and should have a dirty hash.
		if !isReused(n) {
			switch n := n.(type) {
			case (*AccountNode):
				n.markDirty()
			case (*BranchNode):
				n.markDirty()
			case (*ExtensionNode):
				n.markDirty()
			case (*ValueNode):
				n.markDirty()
			}
		}
		// Also update the dirty child-hash markers in the nodes.
		if branch, ok := n.(*BranchNode); ok {
			branch.dirtyHashes = 0
			for i := byte(0); i < 16; i++ {
				if branch.children[i].Id().IsEmpty() {
					continue
				}
				read, _ := ctxt.getReadAccess(&branch.children[i])
				if !isReused(read.Get()) {
					branch.markChildHashDirty(i)
				}
				read.Release()
			}
		}
		if extension, ok := n.(*ExtensionNode); ok {
			read, _ := ctxt.getReadAccess(&extension.next)
			extension.nextHashDirty = !isReused(read.Get())
			read.Release()
		}
		if account, ok := n.(*AccountNode); ok {
			// Look in pre-existing nodes for this account.
			found := false
			for _, node := range allPreexistingNodes {
				cur, ok := node.(*AccountNode)
				if !ok {
					continue
				}
				if cur.address == account.address {
					found = true
					account.storageHashDirty = !ctxt.equalTriesWithConfig(account.storage, cur.storage, equalityConfig{ignoreDirtyHash: true, ignoreFreeze: true})
					break
				}
			}
			if !found {
				account.storageHashDirty = !account.storage.Id().IsEmpty()
			}
		}
		return VisitResponseContinue
	}))
	handle.Release()
}

func TestVisitPathToAccount_CanReachTerminalNodes(t *testing.T) {
	address := common.Address{0x12, 0x34, 0x56, 0x78}

	tests := map[string]struct {
		trie NodeDesc // < the structure of the trie
		path []string // < the path to follow to reach the test account
	}{
		"empty": {
			trie: &Tag{"A", &Empty{}},
			path: []string{},
		},
		"wrong account": {
			trie: &Tag{"A", &Account{}},
			path: []string{"A"},
		},
		"correct account": {
			trie: &Tag{"A", &Account{address: address}},
			path: []string{"A"},
		},
		"branch without account": {
			trie: &Tag{"A", &Branch{children: Children{
				0: &Tag{"B", &Empty{}},
				1: &Tag{"C", &Empty{}},
				2: &Tag{"D", &Empty{}},
			}}},
			path: []string{"A"},
		},
		"branch with wrong account": {
			trie: &Tag{"A", &Branch{children: Children{
				0: &Tag{"B", &Empty{}},
				1: &Tag{"C", &Account{}},
				2: &Tag{"D", &Empty{}},
			}}},
			path: []string{"A", "C"},
		},
		"branch with correct account": {
			trie: &Tag{"A", &Branch{children: Children{
				0: &Tag{"B", &Empty{}},
				1: &Tag{"C", &Account{address: address}},
				2: &Tag{"D", &Empty{}},
			}}},
			path: []string{"A", "C"},
		},
		"extension with common prefix": {
			trie: &Tag{"A", &Extension{
				path: []Nibble{1, 2, 3},
				next: &Tag{"B", &Branch{children: Children{
					3: &Tag{"C", &Empty{}},
					4: &Tag{"D", &Empty{}},
				}},
				}}},
			path: []string{"A", "B"},
		},
		"extension without common prefix": {
			trie: &Tag{"A", &Extension{path: []Nibble{2, 3}}},
			path: []string{"A"},
		},
		"branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: addressToNibbles(address), // extension node will exhaust the path
				next: &Tag{"B", &Branch{}},
			},
			},
			path: []string{"A"},
		},
		"nested branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: addressToNibbles(address)[0:39], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					0: &Tag{"C", &Branch{children: Children{
						0: &Tag{"D", &Account{}},
					}}}}},
				}}},
			path: []string{"A", "B", "C"},
		},
		"account node too deep": {
			trie: &Tag{"A", &Extension{
				path: addressToNibbles(address)[0:39], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					0: &Tag{"C", &Account{address: address}}},
				}}},
			},
			path: []string{"A", "B", "C"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContext(t, ctrl)

			root, shared := ctxt.Build(test.trie)
			accountPresent := false
			handle := shared.GetViewHandle()
			if _, err := handle.Get().Visit(ctxt, &root, 0, MakeVisitor(func(n Node, i NodeInfo) VisitResponse {
				if node, ok := n.(*AccountNode); ok && node.address == address {
					accountPresent = true
				}
				return VisitResponseContinue
			})); err != nil {
				t.Fatalf("unexpected error during visit: %v", err)
			}
			handle.Release()

			visitor := NewMockNodeVisitor(ctrl)
			var last *gomock.Call
			for _, label := range test.path {
				ref, shared := ctxt.Get(label)
				handle := shared.GetViewHandle()
				cur := visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id()})
				handle.Release()
				if last != nil {
					cur.After(last)
				}
				last = cur
			}

			found, err := VisitPathToAccount(ctxt, &root, address, visitor)
			if err != nil {
				t.Fatalf("unexpected error during path iteration: %v", err)
			}

			if found != accountPresent {
				t.Errorf("unexpected found result, wanted %t, got %t", accountPresent, found)
			}
		})
	}
}

func TestVisitPathToAccount_SourceError(t *testing.T) {
	injectedErr := errors.New("injected error")

	ctrl := gomock.NewController(t)

	source := NewMockNodeSource(ctrl)
	source.EXPECT().getConfig().Return(S4LiveConfig).AnyTimes()
	source.EXPECT().getViewAccess(gomock.Any()).Return(shared.ViewHandle[Node]{}, injectedErr)

	nodeVisitor := NewMockNodeVisitor(ctrl)

	var address common.Address
	rootId := NewNodeReference(EmptyId())
	if found, err := VisitPathToAccount(source, &rootId, address, nodeVisitor); found || !errors.Is(err, injectedErr) {
		t.Fatalf("expected iteration to fail")
	}
}

func TestVisitPathToAccount_VisitorAborted(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNiceNodeContext(t, ctrl)

	var address common.Address
	root, _ := ctxt.Build(&Extension{path: []Nibble{0}})
	nodeVisitor := NewMockNodeVisitor(ctrl)
	nodeVisitor.EXPECT().Visit(gomock.Any(), gomock.Any()).Return(VisitResponseAbort)

	if found, _ := VisitPathToAccount(ctxt, &root, address, nodeVisitor); found {
		t.Fatalf("expected iteration to fail")
	}
}

func TestVisitPathToStorage_CanReachTerminalNodes(t *testing.T) {
	key := common.Key{0x12, 0x34, 0x56, 0x78}

	tests := map[string]struct {
		trie NodeDesc // < the structure of the trie
		path []string // < the path to follow to reach the test account
	}{
		"correct storage": {
			trie: &Tag{"A", &Value{key: key}},
			path: []string{"A"},
		},
		"branch with correct storage": {
			trie: &Tag{"A", &Branch{children: Children{
				0: &Tag{"B", &Empty{}},
				1: &Tag{"C", &Value{key: key}},
				2: &Tag{"D", &Empty{}},
			}}},
			path: []string{"A", "C"},
		},
		"branch with incorrect storage": {
			trie: &Tag{"A", &Branch{children: Children{
				0: &Tag{"B", &Empty{}},
				1: &Tag{"C", &Value{}},
				2: &Tag{"D", &Empty{}},
			}}},
			path: []string{"A", "C"},
		},
		"branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: keyToNibbles(key), // extension node will exhaust the path
				next: &Tag{"B", &Branch{}},
			}},
			path: []string{"A"},
		},
		"nested branch node too deep": {
			trie: &Tag{"A", &Extension{
				path: keyToNibbles(key)[0:63], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					0: &Tag{"C", &Branch{children: Children{
						0: &Tag{"D", &Value{}},
					}}}}},
				}}},
			path: []string{"A", "B", "C"},
		},
		"value node too deep": {
			trie: &Tag{"A", &Extension{
				path: keyToNibbles(key)[0:63], // branch node will exhaust the path
				next: &Tag{"B", &Branch{children: Children{
					0: &Tag{"C", &Value{key: key}},
				}}},
			}},
			path: []string{"A", "B", "C"},
		},
		"wrong extension": {
			trie: &Tag{"A", &Extension{
				path: keyToNibbles(common.Key{}),
				next: &Tag{"B", &Branch{}},
			}},
			path: []string{"A"},
		},
		"empty node ": {
			trie: &Tag{"A", Empty{}},
			path: []string{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContext(t, ctrl)

			root, shared := ctxt.Build(test.trie)
			storagePresent := false
			handle := shared.GetViewHandle()
			if _, err := handle.Get().Visit(ctxt, &root, 0, MakeVisitor(func(n Node, i NodeInfo) VisitResponse {
				if node, ok := n.(*ValueNode); ok && node.key == key {
					storagePresent = true
				}
				return VisitResponseContinue
			})); err != nil {
				t.Fatalf("unexpected error during visit: %v", err)
			}
			handle.Release()

			visitor := NewMockNodeVisitor(ctrl)
			var last *gomock.Call
			for _, label := range test.path {
				ref, shared := ctxt.Get(label)
				handle := shared.GetViewHandle()
				cur := visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: ref.Id()})
				handle.Release()
				if last != nil {
					cur.After(last)
				}
				last = cur
			}

			found, err := VisitPathToStorage(ctxt, &root, key, visitor)
			if err != nil {
				t.Fatalf("unexpected error during path iteration: %v", err)
			}

			if found != storagePresent {
				t.Errorf("unexpected found result, wanted %t, got %t", storagePresent, found)
			}
		})
	}
}

func TestTransitions_ImmutableTransitionHaveExpectedEffect(t *testing.T) {
	testTransitions_ImmutableTransitionHaveExpectedEffect(t, S4LiveConfig)
}

func TestTransitions_ImmutableTransitionHaveExpectedEffectWithTrackedPrefixLength(t *testing.T) {
	config := S4LiveConfig
	config.TrackSuffixLengthsInLeafNodes = true
	testTransitions_ImmutableTransitionHaveExpectedEffect(t, config)
}

func TestTransitions_ImmutableTransitionHaveExpectedEffectWithEthereumHashing(t *testing.T) {
	config := S4LiveConfig
	config.TrackSuffixLengthsInLeafNodes = true
	config.Hashing = EthereumLikeHashing
	testTransitions_ImmutableTransitionHaveExpectedEffect(t, config)
}

func testTransitions_ImmutableTransitionHaveExpectedEffect(t *testing.T, config MptConfig) {
	for _, transition := range getTestTransitions() {
		transition := transition
		t.Run(transition.getLabel(), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContextWithConfig(t, ctrl, config)

			if config.TrackSuffixLengthsInLeafNodes {
				fixPrefixLength(transition.before, 0)
				fixPrefixLength(transition.after, 0)
			}

			before, _ := ctxt.Build(transition.before)
			ctxt.Freeze(before)
			original, _ := ctxt.Clone(before)

			after, changed, err := transition.apply(ctxt, before)
			if err != nil {
				t.Fatalf("failed to apply transition: %v", err)
			}
			ctxt.Check(t, before)
			ctxt.Check(t, after)

			if changed {
				t.Errorf("frozen nodes should never be changed")
			}

			// Make sure the result is what is expected.
			want, _ := ctxt.Build(transition.after)
			markModifiedAsDirty(t, ctxt, before, want)
			markReusedAsFrozen(t, ctxt, before, want)
			ctxt.Check(t, want)

			// This modified after state, partially referring to old nodes,
			// should be equal to the modified trie.
			ctxt.ExpectEqualTries(t, want, after)

			// Also, make sure the original structure is preserved.
			ctxt.ExpectEqualTries(t, original, before)
		})
	}
}

func markReusedAsFrozen(t *testing.T, ctxt *nodeContext, before, after NodeReference) {
	t.Helper()
	// All nodes that have been there before should be reused and frozen.
	allPreexistingNodes, err := getAllReachableNodes(ctxt, before)
	if err != nil {
		t.Fatalf("failed to collect preexisting nodes: %v", err)
	}
	isReused := func(n Node) bool {
		for _, cur := range allPreexistingNodes {
			if ctxt.equalWithConfig(cur, n, equalityConfig{ignoreFreeze: true}) {
				return true
			}
		}
		return false
	}

	handle, _ := ctxt.getViewAccess(&after)
	handle.Get().Visit(ctxt, &after, 0, MakeVisitor(func(n Node, i NodeInfo) VisitResponse {
		// Update the wanted node expected to be frozen.
		if isReused(n) {
			n.MarkFrozen()
		}
		// Also update the frozen hints in branch nodes.
		if branch, ok := n.(*BranchNode); ok {
			branch.frozenChildren = 0
			for i := byte(0); i < 16; i++ {
				if branch.children[i].Id().IsEmpty() {
					continue
				}
				read, _ := ctxt.getReadAccess(&branch.children[i])
				if isReused(read.Get()) {
					branch.setChildFrozen(i, true)
				}
				read.Release()
			}
		}
		return VisitResponseContinue
	}))
	handle.Release()
}

func getAllReachableNodes(source NodeSource, root NodeReference) ([]Node, error) {
	res := []Node{}

	handle, err := source.getViewAccess(&root)
	if err != nil {
		return nil, err
	}
	defer handle.Release()

	_, err = handle.Get().Visit(source, &root, 0, MakeVisitor(func(n Node, _ NodeInfo) VisitResponse {
		res = append(res, n)
		return VisitResponseContinue
	}))
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ----------------------------------------------------------------------------
//                                 Actions
// ----------------------------------------------------------------------------

type action struct {
	description string
	input       NodeDesc
	action      func(*testing.T, *trie) error
}

func (a *action) apply(t *testing.T, mgr NodeManager, root NodeReference) error {
	return a.action(t, &trie{mgr, root})
}

func getTestActions() []action {
	res := []action{}

	// Make all transitions actions.
	for _, transition := range getTestTransitions() {
		transition := transition
		res = append(res, action{
			description: fmt.Sprintf("transition/%s", transition.getLabel()),
			input:       transition.before,
			action: func(t *testing.T, trie *trie) error {
				_, _, err := transition.change(trie)
				return err
			},
		})
	}

	// Add additional actions.

	// -- Account Nodes --

	res = append(res, action{
		description: "Account/GetAccount/missing",
		input:       &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested account should not exist")
			}
			if want, got := (AccountInfo{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Account/GetAccount/present",
		input:       &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x73})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested account should exist")
			}
			if want, got := (AccountInfo{Nonce: common.Nonce{2}}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Account/GetValue/missing_account",
		input: &Account{
			address: common.Address{0x42},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Value{key: common.Key{0x52}, value: common.Value{1}},
		},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x24}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Account/GetValue/missing_slot",
		input: &Account{
			address: common.Address{0x42},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Value{key: common.Key{0x52}, value: common.Value{1}},
		},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Account/GetValue/present",
		input: &Account{
			address: common.Address{0x42},
			info:    AccountInfo{Nonce: common.Nonce{1}},
			storage: &Value{key: common.Key{0x52}, value: common.Value{1}},
		},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x52})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested value should exist")
			}
			if want, got := (common.Value{1}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	// -- Branch Nodes --

	res = append(res, action{
		description: "Branch/GetAccount/missing",
		input: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested account should not exist")
			}
			if want, got := (AccountInfo{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Branch/GetAccount/present",
		input: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x42}, info: AccountInfo{Nonce: common.Nonce{1}}},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x73})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested account should exist")
			}
			if want, got := (AccountInfo{Nonce: common.Nonce{2}}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Branch/GetValue/missing",
		input: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Branch{children: Children{
					0x5: &Value{key: common.Key{0x52}, value: common.Value{1}},
					0xB: &Value{key: common.Key{0xBC}, value: common.Value{2}},
				}},
			},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Branch/GetValue/present",
		input: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Branch{children: Children{
					0x5: &Value{key: common.Key{0x52}, value: common.Value{1}},
					0xB: &Value{key: common.Key{0xBC}, value: common.Value{2}},
				}},
			},
			0x7: &Account{address: common.Address{0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x52})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested value should exist")
			}
			if want, got := (common.Value{1}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	// -- Empty Nodes --

	res = append(res, action{
		description: "Empty/GetAccount",
		input:       &Empty{},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested account should not exist")
			}
			if want, got := (AccountInfo{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Empty/GetValue",
		input:       &Empty{},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	// -- Extension Node --

	res = append(res, action{
		description: "Extension/GetAccount/missing",
		input: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x12, 0x42}, info: AccountInfo{Nonce: common.Nonce{1}}},
			0x7: &Account{address: common.Address{0x12, 0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested account should not exist")
			}
			if want, got := (AccountInfo{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Extension/GetAccount/present",
		input: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{address: common.Address{0x12, 0x42}, info: AccountInfo{Nonce: common.Nonce{1}}},
			0x7: &Account{address: common.Address{0x12, 0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetAccount(common.Address{0x12, 0x73})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested account should exist")
			}
			if want, got := (AccountInfo{Nonce: common.Nonce{2}}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Extension/GetValue/missing_account",
		input: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
					0x5: &Value{key: common.Key{0x12, 0x52}, value: common.Value{1}},
					0xB: &Value{key: common.Key{0x12, 0xBC}, value: common.Value{2}},
				}}},
			},
			0x7: &Account{address: common.Address{0x12, 0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x42}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Extension/GetValue/missing_slot",
		input: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
					0x5: &Value{key: common.Key{0x12, 0x52}, value: common.Value{1}},
					0xB: &Value{key: common.Key{0x12, 0xBC}, value: common.Value{2}},
				}}},
			},
			0x7: &Account{address: common.Address{0x12, 0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x12, 0x42}, common.Key{0x12})
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("requested value should not exist")
			}
			if want, got := (common.Value{}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	res = append(res, action{
		description: "Extension/GetValue/present",
		input: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
			0x4: &Account{
				address: common.Address{0x12, 0x42},
				info:    AccountInfo{Nonce: common.Nonce{1}},
				storage: &Extension{path: []Nibble{1, 2}, next: &Branch{children: Children{
					0x5: &Value{key: common.Key{0x12, 0x52}, value: common.Value{1}},
					0xB: &Value{key: common.Key{0x12, 0xBC}, value: common.Value{2}},
				}}},
			},
			0x7: &Account{address: common.Address{0x12, 0x73}, info: AccountInfo{Nonce: common.Nonce{2}}},
		}}},
		action: func(t *testing.T, trie *trie) error {
			info, exists, err := trie.GetValue(common.Address{0x12, 0x42}, common.Key{0x12, 0x52})
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("requested value should exist")
			}
			if want, got := (common.Value{1}), info; want != got {
				t.Errorf("unexpected result, wanted %v, got %v", want, got)
			}
			return nil
		},
	})

	// General operations on all states defined so far.
	snapshot := slices.Clone(res)

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/check",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.Check()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/check_forest",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.CheckForest()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/dump",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.Dump()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/visit",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.Visit()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/release",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.Release()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				return trie.Freeze()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze_and_check",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				if err := trie.Freeze(); err != nil {
					return err
				}
				return trie.Check()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze_and_check_forest",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				if err := trie.Freeze(); err != nil {
					return err
				}
				return trie.CheckForest()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze_and_dump",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				if err := trie.Freeze(); err != nil {
					return err
				}
				return trie.Dump()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze_and_visit",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				if err := trie.Freeze(); err != nil {
					return err
				}
				return trie.Visit()
			},
		})
	}

	for _, cur := range snapshot {
		res = append(res, action{
			description: cur.description + "/freeze_and_release",
			input:       cur.input,
			action: func(t *testing.T, trie *trie) error {
				if err := trie.Freeze(); err != nil {
					return err
				}
				return trie.Release()
			},
		})
	}

	return res
}

func TestActions_InputsAreValid(t *testing.T) {
	for _, state := range getTestStates() {
		ctrl := gomock.NewController(t)
		ctxt := newNiceNodeContext(t, ctrl)
		input, _ := ctxt.Build(state)
		ctxt.Check(t, input)
	}
}

func TestActions_PassOnMutableNode(t *testing.T) {
	testActions_Pass(t, false)
}

func TestActions_PassOnFrozenNode(t *testing.T) {
	testActions_Pass(t, true)
}

func testActions_Pass(t *testing.T, frozen bool) {
	for _, action := range getTestActions() {
		action := action
		t.Run(action.description, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContext(t, ctrl)
			input, _ := ctxt.Build(action.input)
			if frozen {
				ctxt.Freeze(input)
			}
			if err := action.apply(t, ctxt, input); err != nil {
				t.Fatalf("action failed with error: %v", err)
			}
		})
	}
}

func TestActions_AllErrorsAreForwardedByMutableNodes(t *testing.T) {
	testActions_AllErrorsAreForwarded(t, false)
}

func TestActions_AllErrorsAreForwardedByFrozenNodes(t *testing.T) {
	testActions_AllErrorsAreForwarded(t, true)
}

func testActions_AllErrorsAreForwarded(t *testing.T, frozen bool) {
	t.Run("without_path_length", func(t *testing.T) {
		testActions_AllErrorsAreForwardedInternal(t, frozen, S4LiveConfig)
	})
	t.Run("with_path_length", func(t *testing.T) {
		config := S4LiveConfig
		config.TrackSuffixLengthsInLeafNodes = true
		testActions_AllErrorsAreForwardedInternal(t, frozen, config)
	})
}

func testActions_AllErrorsAreForwardedInternal(t *testing.T, frozen bool, config MptConfig) {
	for _, action := range getTestActions() {

		if config.TrackSuffixLengthsInLeafNodes {
			fixPrefixLength(action.input, 0)
		}

		action := action
		t.Run(action.description, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctxt := newNiceNodeContextWithConfig(t, ctrl, config)

			input, _ := ctxt.Build(action.input)
			if frozen {
				ctxt.Freeze(input)
			}

			// Count the number of NodeManager calls.
			countingManager := &errorInjectingNodeManager{NodeManager: ctxt, errorPosition: -1}
			if err := action.apply(t, countingManager, input); err != nil {
				t.Fatalf("failed to process transition: %v", err)
			}

			// Inject a test error for each encountered manager call.
			for i := 0; i < countingManager.counter; i++ {
				injectedError := fmt.Errorf("Introduced-Test-Error")
				t.Run(fmt.Sprintf("failing_call_%d", i), func(t *testing.T) {
					ctrl := gomock.NewController(t)
					ctxt := newNiceNodeContextWithConfig(t, ctrl, config)
					before, _ := ctxt.Build(action.input)
					if frozen {
						ctxt.Freeze(before)
					}

					manager := &errorInjectingNodeManager{
						NodeManager:   ctxt,
						errorPosition: i,
						err:           injectedError,
					}
					err := action.apply(t, manager, before)

					if manager.counter < i {
						t.Fatalf("invalid number of manager calls, expected %d, got %d", i, manager.counter)
					}

					if err == nil || !errors.Is(err, injectedError) {
						t.Fatalf("missing expected error, wanted %v, got %v", injectedError, err)
					}

				})
			}
		})
	}
}

// errorInjectingNodeManager is a wrapper around a node manager capable of injecting errors
// for selected manager interface calls.
type errorInjectingNodeManager struct {
	NodeManager
	errorPosition int   // the call index at which an error is injected
	err           error // the error to be injected
	counter       int
}

func (m *errorInjectingNodeManager) getReadAccess(r *NodeReference) (shared.ReadHandle[Node], error) {
	if m.counter == m.errorPosition {
		return shared.ReadHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.getReadAccess(r)
}

func (m *errorInjectingNodeManager) getViewAccess(r *NodeReference) (shared.ViewHandle[Node], error) {
	if m.counter == m.errorPosition {
		return shared.ViewHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.getViewAccess(r)
}

func (m *errorInjectingNodeManager) getHashAccess(r *NodeReference) (shared.HashHandle[Node], error) {
	if m.counter == m.errorPosition {
		return shared.HashHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.getHashAccess(r)
}

func (m *errorInjectingNodeManager) getWriteAccess(r *NodeReference) (shared.WriteHandle[Node], error) {
	if m.counter == m.errorPosition {
		return shared.WriteHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.getWriteAccess(r)
}

func (m *errorInjectingNodeManager) getHashFor(r *NodeReference) (common.Hash, error) {
	if m.counter == m.errorPosition {
		return common.Hash{}, m.err
	}
	m.counter++
	return m.NodeManager.getHashFor(r)
}

func (m *errorInjectingNodeManager) createAccount() (NodeReference, shared.WriteHandle[Node], error) {
	if m.counter == m.errorPosition {
		return NodeReference{}, shared.WriteHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.createAccount()
}

func (m *errorInjectingNodeManager) createBranch() (NodeReference, shared.WriteHandle[Node], error) {
	if m.counter == m.errorPosition {
		return NodeReference{}, shared.WriteHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.createBranch()
}

func (m *errorInjectingNodeManager) createExtension() (NodeReference, shared.WriteHandle[Node], error) {
	if m.counter == m.errorPosition {
		return NodeReference{}, shared.WriteHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.createExtension()
}

func (m *errorInjectingNodeManager) createValue() (NodeReference, shared.WriteHandle[Node], error) {
	if m.counter == m.errorPosition {
		return NodeReference{}, shared.WriteHandle[Node]{}, m.err
	}
	m.counter++
	return m.NodeManager.createValue()
}

func (m *errorInjectingNodeManager) release(id *NodeReference) error {
	if m.counter == m.errorPosition {
		return m.err
	}
	m.counter++
	return m.NodeManager.release(id)
}

// ----------------------------------------------------------------------------
//                               Utilities
// ----------------------------------------------------------------------------

// NodeDesc is used to describe the structure of a MPT node for unit tests. It
// is intended to be used to build convenient, readable test-structures of nodes
// on which operations are to be exercised.
type NodeDesc interface {
	Build(*nodeContext) (NodeReference, *shared.Shared[Node])
}

type Empty struct{}

func (Empty) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	return NewNodeReference(EmptyId()), shared.MakeShared[Node](EmptyNode{})
}

type Mock struct {
	node Node
}

func (m *Mock) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	return NewNodeReference(ValueId(ctx.nextIndex())), shared.MakeShared[Node](m.node)
}

type Account struct {
	dirty            bool
	address          common.Address
	info             AccountInfo
	frozen           bool
	pathLength       byte
	storage          NodeDesc
	storageHashDirty bool
	dirtyHash        bool
	hashStatus       *hashStatus // overrides dirtyHash flag if set
}

func (a *Account) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	storage := NewNodeReference(EmptyId())
	var storageHash common.Hash
	if a.storage != nil {
		id, _ := ctx.Build(a.storage)
		storage = id
		storageHash, _ = ctx.getHashFor(&storage)
	} else {
		storageHash = EmptyNodeEthereumHash
	}
	hashStatus := hashStatusClean
	if a.dirtyHash {
		hashStatus = hashStatusDirty
	}
	if a.hashStatus != nil {
		hashStatus = *a.hashStatus
	}
	res := &AccountNode{
		nodeBase: nodeBase{
			frozen:     a.frozen,
			hashStatus: hashStatus,
		},
		address:          a.address,
		info:             a.info,
		pathLength:       a.pathLength,
		storage:          storage,
		storageHashDirty: a.storageHashDirty,
		storageHash:      storageHash,
	}
	res.nodeBase.clean.Store(!a.dirty)
	return NewNodeReference(AccountId(ctx.nextIndex())), shared.MakeShared[Node](res)
}

type Children map[Nibble]NodeDesc
type ChildHashes map[Nibble]common.Hash

type Branch struct {
	dirty            bool
	children         Children
	childHashes      ChildHashes
	embeddedChildren []bool
	dirtyChildHashes []int
	frozen           bool
	frozenChildren   []int
	dirtyHash        bool
	hashStatus       *hashStatus // overrides dirtyHash flag if set
}

func (b *Branch) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	ref := NewNodeReference(BranchId(ctx.nextIndex()))
	res := &BranchNode{}
	res.nodeBase.clean.Store(!b.dirty)
	res.frozen = b.frozen
	for i, desc := range b.children {
		ref, _ := ctx.Build(desc)
		res.children[i] = ref
		res.hashes[i], _ = ctx.getHashFor(&ref)
	}
	for i, hash := range b.childHashes {
		res.hashes[i] = hash
	}
	for _, i := range b.dirtyChildHashes {
		res.markChildHashDirty(byte(i))
	}
	for _, i := range b.frozenChildren {
		res.setChildFrozen(byte(i), true)
	}
	for i, embedded := range b.embeddedChildren {
		res.setEmbedded(byte(i), embedded)
	}
	res.hashStatus = hashStatusClean
	if b.dirtyHash {
		res.hashStatus = hashStatusDirty
	}
	if b.hashStatus != nil {
		res.hashStatus = *b.hashStatus
	}
	return ref, shared.MakeShared[Node](res)
}

type Extension struct {
	dirty         bool
	frozen        bool
	path          []Nibble
	next          NodeDesc
	hashDirty     bool
	nextHash      *common.Hash
	nextHashDirty bool
	hashStatus    *hashStatus // overrides dirtyHash flag if set
	nextEmbedded  bool
}

func (e *Extension) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	ref := NewNodeReference(ExtensionId(ctx.nextIndex()))
	res := &ExtensionNode{}
	res.nodeBase.clean.Store(!e.dirty)
	res.frozen = e.frozen
	res.path = CreatePathFromNibbles(e.path)
	res.next, _ = ctx.Build(e.next)
	res.hashStatus = hashStatusClean
	res.nextIsEmbedded = e.nextEmbedded
	if e.hashDirty {
		res.hashStatus = hashStatusDirty
	}
	if e.hashStatus != nil {
		res.hashStatus = *e.hashStatus
	}
	res.nextHashDirty = e.nextHashDirty
	if e.nextHash != nil {
		res.nextHash = *e.nextHash
	} else if !res.nextHashDirty {
		res.nextHash, _ = ctx.getHashFor(&res.next)
	}
	return ref, shared.MakeShared[Node](res)
}

type Tag struct {
	label  string
	nested NodeDesc
}

func (t *Tag) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	ref, res := ctx.Build(t.nested)
	ctx.tags[t.label] = entry{ref, res}
	return ref, res
}

type Value struct {
	dirty      bool
	key        common.Key
	value      common.Value
	length     byte
	dirtyHash  bool
	frozen     bool
	hashStatus *hashStatus // overrides dirtyHash flag if set
}

func (v *Value) Build(ctx *nodeContext) (NodeReference, *shared.Shared[Node]) {
	hashStatus := hashStatusClean
	if v.dirtyHash {
		hashStatus = hashStatusDirty
	}
	if v.hashStatus != nil {
		hashStatus = *v.hashStatus
	}
	res := &ValueNode{
		nodeBase: nodeBase{
			frozen:     v.frozen,
			hashStatus: hashStatus,
		},
		key:        v.key,
		value:      v.value,
		pathLength: v.length,
	}
	res.nodeBase.clean.Store(!v.dirty)
	return NewNodeReference(ValueId(ctx.nextIndex())), shared.MakeShared[Node](res)
}

type entry struct {
	ref  NodeReference
	node *shared.Shared[Node]
}
type nodeContext struct {
	*MockNodeManager
	index     map[NodeId]entry
	cache     map[NodeDesc]entry
	tags      map[string]entry
	lastIndex uint64
	config    MptConfig
	released  []NodeId
}

func newNodeContext(t *testing.T, ctrl *gomock.Controller) *nodeContext {
	return newNodeContextWithConfig(t, ctrl, S4LiveConfig)
}

func newNodeContextWithConfig(t *testing.T, ctrl *gomock.Controller, config MptConfig) *nodeContext {
	res := &nodeContext{
		MockNodeManager: NewMockNodeManager(ctrl),
		index:           map[NodeId]entry{},
		cache:           map[NodeDesc]entry{},
		tags:            map[string]entry{},
		config:          config,
	}
	res.EXPECT().getConfig().AnyTimes().Return(config)
	res.EXPECT().hashAddress(gomock.Any()).AnyTimes().DoAndReturn(common.Keccak256ForAddress)
	res.EXPECT().hashKey(gomock.Any()).AnyTimes().DoAndReturn(common.Keccak256ForKey)
	res.EXPECT().getHashFor(gomock.Any()).AnyTimes().DoAndReturn(func(ref *NodeReference) (common.Hash, error) {
		// Mock nodes have a constant hash of zero.
		handle, err := res.getViewAccess(ref)
		if err != nil {
			return common.Hash{}, err
		}
		defer handle.Release()
		if _, isMock := handle.Get().(*MockNode); isMock {
			return common.Hash{}, nil
		}
		// All others are hashed according to the configuration.
		hasher := config.Hashing.createHasher()
		return hasher.getHash(ref, res)
	})

	// The empty node is always present.
	res.Build(Empty{})

	// Make sure that in the end all node handles have been released.
	t.Cleanup(func() {
		for _, entry := range res.index {
			handle, ok := entry.node.TryGetWriteHandle()
			if !ok {
				t.Errorf("failed to acquire exclusive access to node %v at end of test -- looks like not all handle have been released", entry.ref)
			} else {
				handle.Release()
			}
		}
	})

	// Make sure that all released nodes are marked as clean.
	t.Cleanup(func() {
		for _, id := range res.released {
			ref := NewNodeReference(id)
			handle, err := res.getViewAccess(&ref)
			if err != nil {
				t.Errorf("failed to acquire read access to node %v: %v", id, err)
				continue
			}
			if handle.Get().IsDirty() {
				t.Errorf("released node %v is dirty at end of test", id)
			}
			handle.Release()
		}
	})

	return res
}

// newNiceNodeContext is a node context that expects (and accepts) an arbitrary
// number of calls to its functions without the need to explicitly define
// those expectations.
func newNiceNodeContext(t *testing.T, ctrl *gomock.Controller) *nodeContext {
	return newNiceNodeContextWithConfig(t, ctrl, S4LiveConfig)
}

func newNiceNodeContextWithConfig(t *testing.T, ctrl *gomock.Controller, config MptConfig) *nodeContext {
	ctxt := newNodeContextWithConfig(t, ctrl, config)

	ctxt.EXPECT().createAccount().AnyTimes().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		ref, shared := ctxt.Build(&Account{dirty: true})
		return ref, shared.GetWriteHandle(), nil
	})

	ctxt.EXPECT().createBranch().AnyTimes().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		ref, shared := ctxt.Build(&Branch{dirty: true})
		return ref, shared.GetWriteHandle(), nil
	})

	ctxt.EXPECT().createExtension().AnyTimes().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		ref, shared := ctxt.Build(&Extension{dirty: true})
		return ref, shared.GetWriteHandle(), nil
	})

	ctxt.EXPECT().createValue().AnyTimes().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		ref, shared := ctxt.Build(&Value{dirty: true})
		return ref, shared.GetWriteHandle(), nil
	})

	ctxt.EXPECT().release(gomock.Any()).AnyTimes()
	ctxt.EXPECT().releaseTrieAsynchronous(gomock.Any()).AnyTimes()

	return ctxt
}

func (c *nodeContext) Build(desc NodeDesc) (NodeReference, *shared.Shared[Node]) {
	if desc == nil {
		return NewNodeReference(EmptyId()), nil
	}
	e, exists := c.cache[desc]
	if exists {
		return e.ref, e.node
	}

	ref, node := desc.Build(c)
	c.EXPECT().getReadAccess(RefTo(ref.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ReadHandle[Node], error) {
		return node.GetReadHandle(), nil
	})
	c.EXPECT().getViewAccess(RefTo(ref.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ViewHandle[Node], error) {
		return node.GetViewHandle(), nil
	})
	c.EXPECT().getHashAccess(RefTo(ref.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.HashHandle[Node], error) {
		return node.GetHashHandle(), nil
	})
	c.EXPECT().getWriteAccess(RefTo(ref.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.WriteHandle[Node], error) {
		return node.GetWriteHandle(), nil
	})
	c.index[ref.Id()] = entry{ref, node}
	c.cache[desc] = entry{ref, node}

	view := node.GetViewHandle()
	wantsHashDirty := true
	if _, isMock := view.Get().(*MockNode); !isMock {
		_, wantsHashDirty = view.Get().GetHash()
	}
	view.Release()

	if !wantsHashDirty {
		hash, _ := c.getHashFor(&ref)
		write := node.GetWriteHandle()
		write.Get().SetHash(hash)
		write.Release()
	}

	return ref, node
}

func (c *nodeContext) ExpectCreateAccount() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Account{dirty: true})
	c.EXPECT().createAccount().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	handle.Release()
	return ref, instance
}

func (c *nodeContext) ExpectCreateBranch() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Branch{dirty: true})
	c.EXPECT().createBranch().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	handle.Release()
	return ref, instance
}

func (c *nodeContext) ExpectCreateTemporaryBranch() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Branch{dirty: true})
	c.EXPECT().createBranch().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	c.EXPECT().release(&ref).Return(nil)
	return ref, instance
}

func (c *nodeContext) ExpectCreateExtension() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Extension{dirty: true})
	c.EXPECT().createExtension().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	handle.Release()
	return ref, instance
}

func (c *nodeContext) ExpectCreateTemporaryExtension() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Extension{dirty: true})
	c.EXPECT().createExtension().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	c.EXPECT().release(&ref).Return(nil)
	return ref, instance
}

func (c *nodeContext) ExpectCreateValue() (NodeReference, *shared.Shared[Node]) {
	ref, instance := c.Build(&Value{dirty: true})
	c.EXPECT().createValue().DoAndReturn(func() (NodeReference, shared.WriteHandle[Node], error) {
		return ref, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	handle.Release()
	return ref, instance
}

func (c *nodeContext) Get(label string) (NodeReference, *shared.Shared[Node]) {
	e, exists := c.tags[label]
	if !exists {
		panic("requested non-existing element")
	}
	return e.ref, e.node
}

func (c *nodeContext) nextIndex() uint64 {
	c.lastIndex++
	return c.lastIndex
}

func (c *nodeContext) Check(t *testing.T, ref NodeReference) {
	t.Helper()
	if err := CheckForest(c, []*NodeReference{&ref}); err != nil {
		handle := c.tryGetNode(t, ref.Id())
		defer handle.Release()
		out := &bytes.Buffer{}
		handle.Get().Dump(out, c, &ref, "")
		t.Fatalf("inconsistent node structure encountered:\n%v\n%s", err, out.String())
	}
}

func (c *nodeContext) Print(ref NodeReference) string {
	out := &bytes.Buffer{}
	handle, _ := c.getReadAccess(&ref)
	handle.Get().Dump(out, c, &ref, "")
	handle.Release()
	return out.String()
}

func (c *nodeContext) Freeze(ref NodeReference) {
	handle, _ := c.getWriteAccess(&ref)
	defer handle.Release()
	handle.Get().Freeze(c, handle)
}

func (c *nodeContext) tryGetNode(t *testing.T, id NodeId) shared.ReadHandle[Node] {
	entry, found := c.index[id]
	if !found {
		t.Fatalf("unknown node: %v", id)
	}
	handle, success := entry.node.TryGetReadHandle()
	if !success {
		t.Fatalf("failed to gain read access to node %v -- forgot to release some handles?", id)
	}
	return handle
}

func (c *nodeContext) ExpectEqualTries(t *testing.T, want, got NodeReference) {
	t.Helper()
	wantHandle := c.tryGetNode(t, want.Id())
	defer wantHandle.Release()
	gotHandle := c.tryGetNode(t, got.Id())
	defer gotHandle.Release()
	equal := c.equal(wantHandle.Get(), gotHandle.Get())
	diffs := c.diff("root", wantHandle.Get(), gotHandle.Get())
	if equal != (len(diffs) == 0) {
		panic("FATAL: equal and diff not consistent")
	}
	if len(diffs) > 0 {
		print := &bytes.Buffer{}
		wantHandle.Get().Dump(print, c, &want, "")
		t.Errorf("Want:\n%s", print.String())
		have := &bytes.Buffer{}
		gotHandle.Get().Dump(have, c, &got, "")
		t.Errorf("Have:\n%s", have.String())
		t.Errorf("unexpected resulting node structure")
		t.Errorf("differences:\n")
		for _, diff := range diffs {
			t.Errorf("\t" + diff)
		}
	}
}

func (c *nodeContext) Clone(ref NodeReference) (NodeReference, *shared.Shared[Node]) {
	if ref.Id().IsEmpty() {
		return NewNodeReference(EmptyId()), c.index[ref.Id()].node
	}

	handle, _ := c.getReadAccess(&ref)
	defer handle.Release()
	res, node := c.cloneInternal(handle.Get())
	c.EXPECT().getReadAccess(RefTo(res.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ReadHandle[Node], error) {
		return node.GetReadHandle(), nil
	})
	c.EXPECT().getViewAccess(RefTo(res.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ViewHandle[Node], error) {
		return node.GetViewHandle(), nil
	})
	c.EXPECT().getHashAccess(RefTo(res.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.HashHandle[Node], error) {
		return node.GetHashHandle(), nil
	})
	c.EXPECT().getWriteAccess(RefTo(res.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.WriteHandle[Node], error) {
		return node.GetWriteHandle(), nil
	})
	c.index[res.Id()] = entry{res, node}
	return res, node
}

func (c *nodeContext) cloneInternal(node Node) (NodeReference, *shared.Shared[Node]) {
	clone := func(ref NodeReference) NodeReference {
		ref, _ = c.Clone(ref)
		return ref
	}

	if a, ok := node.(*AccountNode); ok {
		res := &AccountNode{}
		*res = *a
		res.storage = clone(a.storage)
		return NewNodeReference(AccountId(c.nextIndex())), shared.MakeShared[Node](res)
	}

	if e, ok := node.(*ExtensionNode); ok {
		res := &ExtensionNode{}
		*res = *e
		res.next = clone(e.next)
		return NewNodeReference(ExtensionId(c.nextIndex())), shared.MakeShared[Node](res)
	}

	if b, ok := node.(*BranchNode); ok {
		ref := NewNodeReference(BranchId(c.nextIndex()))
		res := &BranchNode{}
		*res = *b
		for i, next := range b.children {
			res.children[i] = clone(next)
		}
		return ref, shared.MakeShared[Node](res)
	}

	if v, ok := node.(*ValueNode); ok {
		res := &ValueNode{}
		*res = *v
		return NewNodeReference(ValueId(c.nextIndex())), shared.MakeShared[Node](res)
	}

	panic(fmt.Sprintf("encountered unsupported node type: %v", reflect.TypeOf(node)))
}

func (c *nodeContext) diff(prefix string, nodeA, nodeB Node) []string {
	diffs := []string{}

	if dirtyA, dirtyB := nodeA.IsDirty(), nodeB.IsDirty(); dirtyA != dirtyB {
		diffs = append(diffs, fmt.Sprintf("%s: different dirty state, got %t and %t", prefix, dirtyA, dirtyB))
	}

	if _, ok := nodeA.(EmptyNode); ok {
		if _, ok := nodeB.(EmptyNode); !ok {
			diffs = append(diffs, fmt.Sprintf("%s: different node types, got %v and %v", prefix, reflect.TypeOf(nodeA), reflect.TypeOf(nodeB)))
		}
	}

	if a, ok := nodeA.(*AccountNode); ok {
		if b, ok := nodeB.(*AccountNode); ok {
			if a.address != b.address {
				diffs = append(diffs, fmt.Sprintf("%s: different address, got %x and %x", prefix, a.address, b.address))
			}
			if a.frozen != b.frozen {
				diffs = append(diffs, fmt.Sprintf("%s: different frozen state, got %t and %t", prefix, a.frozen, b.frozen))
			}
			if a.info != b.info {
				diffs = append(diffs, fmt.Sprintf("%s: different info, got %v and %v", prefix, a.info, b.info))
			}
			if a.hashStatus != b.hashStatus {
				diffs = append(diffs, fmt.Sprintf("%s: different hash-dirty flag, got %v and %v", prefix, a.hashStatus, b.hashStatus))
			}
			if a.storageHashDirty != b.storageHashDirty {
				diffs = append(diffs, fmt.Sprintf("%s: different storage hash-dirty flag, got %t and %t", prefix, a.storageHashDirty, b.storageHashDirty))
			}

			if c.config.TrackSuffixLengthsInLeafNodes {
				if a.pathLength != b.pathLength {
					diffs = append(diffs, fmt.Sprintf("%s: different path-length, got %d and %d", prefix, a.pathLength, b.pathLength))
				}
			}

			diffs = append(diffs, c.diffTries(prefix+"/storage", a.storage, b.storage)...)
		} else {
			diffs = append(diffs, fmt.Sprintf("%s: different node types, got %v and %v", prefix, reflect.TypeOf(a), reflect.TypeOf(nodeB)))
		}
	}

	if a, ok := nodeA.(*ExtensionNode); ok {
		if b, ok := nodeB.(*ExtensionNode); ok {
			if a.frozen != b.frozen {
				diffs = append(diffs, fmt.Sprintf("%s: different frozen state, got %t and %t", prefix, a.frozen, b.frozen))
			}
			if a.path != b.path {
				diffs = append(diffs, fmt.Sprintf("%s: different extension path, got %v and %v", prefix, a.path, b.path))
			}
			if a.hashStatus != b.hashStatus {
				diffs = append(diffs, fmt.Sprintf("%s: different hash-dirty flag, got %v and %v", prefix, a.hashStatus, b.hashStatus))
			}
			if a.nextHashDirty != b.nextHashDirty {
				diffs = append(diffs, fmt.Sprintf("%s: different next-hash-dirty flag, got %t and %t", prefix, a.nextHashDirty, b.nextHashDirty))
			}
			diffs = append(diffs, c.diffTries(prefix+"/next", a.next, b.next)...)
		} else {
			diffs = append(diffs, fmt.Sprintf("%s: different node types, got %v and %v", prefix, reflect.TypeOf(a), reflect.TypeOf(nodeB)))
		}
	}

	if a, ok := nodeA.(*BranchNode); ok {
		if b, ok := nodeB.(*BranchNode); ok {
			if a.frozen != b.frozen {
				diffs = append(diffs, fmt.Sprintf("%s: different frozen state, got %t and %t", prefix, a.frozen, b.frozen))
			}
			if a.frozenChildren != b.frozenChildren {
				diffs = append(diffs, fmt.Sprintf("%s: different frozen children flags, got %016b and %016b", prefix, a.frozenChildren, b.frozenChildren))
			}
			if a.hashStatus != b.hashStatus {
				diffs = append(diffs, fmt.Sprintf("%s: different hash-dirty flag, got %v and %v", prefix, a.hashStatus, b.hashStatus))
			}
			if a.dirtyHashes != b.dirtyHashes {
				diffs = append(diffs, fmt.Sprintf("%s: different dirty-child-hashes flags, got %016b and %016b", prefix, a.dirtyHashes, b.dirtyHashes))
			}

			for i, next := range a.children {
				diffs = append(diffs, c.diffTries(fmt.Sprintf("%s/0x%X", prefix, i), next, b.children[i])...)
			}
		} else {
			diffs = append(diffs, fmt.Sprintf("%s: different node types, got %v and %v", prefix, reflect.TypeOf(a), reflect.TypeOf(nodeB)))
		}
	}

	if a, ok := nodeA.(*ValueNode); ok {
		if b, ok := nodeB.(*ValueNode); ok {
			if a.frozen != b.frozen {
				diffs = append(diffs, fmt.Sprintf("%s: different frozen state, got %t and %t", prefix, a.frozen, b.frozen))
			}
			if a.hashStatus != b.hashStatus {
				diffs = append(diffs, fmt.Sprintf("%s: different hash-dirty flag, got %v and %v", prefix, a.hashStatus, b.hashStatus))
			}
			if a.key != b.key {
				diffs = append(diffs, fmt.Sprintf("%s: different key, got %x and %x", prefix, a.key, b.key))
			}
			if a.key != b.key {
				diffs = append(diffs, fmt.Sprintf("%s: different value, got %x and %x", prefix, a.value, b.value))
			}
			if c.config.TrackSuffixLengthsInLeafNodes {
				if a.pathLength != b.pathLength {
					diffs = append(diffs, fmt.Sprintf("%s: different path-length, got %d and %d", prefix, a.pathLength, b.pathLength))
				}
			}
		} else {
			diffs = append(diffs, fmt.Sprintf("%s: different node types, got %v and %v", prefix, reflect.TypeOf(a), reflect.TypeOf(b)))
		}
	}

	return diffs
}

func (c *nodeContext) diffTries(prefix string, a, b NodeReference) []string {
	nodeA, _ := c.getReadAccess(&a)
	nodeB, _ := c.getReadAccess(&b)
	defer nodeA.Release()
	defer nodeB.Release()
	return c.diff(prefix, nodeA.Get(), nodeB.Get())
}

func (c *nodeContext) equal(a, b Node) bool {
	return c.equalWithConfig(a, b, equalityConfig{})
}

type equalityConfig struct {
	ignoreDirtyFlag bool
	ignoreDirtyHash bool
	ignoreFreeze    bool
}

func (c *nodeContext) equalWithConfig(a, b Node, config equalityConfig) bool {
	if !config.ignoreDirtyFlag {
		if dirtyA, dirtyB := a.IsDirty(), b.IsDirty(); dirtyA != dirtyB {
			return false
		}
	}

	if _, ok := a.(EmptyNode); ok {
		_, ok := b.(EmptyNode)
		return ok
	}

	if a, ok := a.(*AccountNode); ok {
		if b, ok := b.(*AccountNode); ok {
			eq := a.address == b.address
			eq = eq && a.info == b.info
			eq = eq && (config.ignoreDirtyHash || a.hashStatus == b.hashStatus)
			eq = eq && (config.ignoreDirtyHash || a.storageHashDirty == b.storageHashDirty)
			eq = eq && (config.ignoreFreeze || a.frozen == b.frozen)
			eq = eq && c.equalTriesWithConfig(a.storage, b.storage, config)
			if !eq {
				return false
			}
			if c.config.TrackSuffixLengthsInLeafNodes {
				if a.pathLength != b.pathLength {
					return false
				}
			}
			return true
		}
		return false
	}

	if a, ok := a.(*ExtensionNode); ok {
		if b, ok := b.(*ExtensionNode); ok {
			eq := a.path == b.path
			eq = eq && (config.ignoreDirtyHash || a.hashStatus == b.hashStatus)
			eq = eq && (config.ignoreDirtyHash || a.nextHashDirty == b.nextHashDirty)
			eq = eq && (config.ignoreFreeze || a.frozen == b.frozen)
			eq = eq && c.equalTriesWithConfig(a.next, b.next, config)
			return eq
		}
		return false
	}

	if a, ok := a.(*BranchNode); ok {
		if b, ok := b.(*BranchNode); ok {
			if !config.ignoreFreeze && a.frozen != b.frozen {
				return false
			}
			if !config.ignoreDirtyHash && a.hashStatus != b.hashStatus {
				return false
			}
			if !config.ignoreDirtyHash && a.dirtyHashes != b.dirtyHashes {
				return false
			}
			if !config.ignoreFreeze && a.frozenChildren != b.frozenChildren {
				return false
			}
			for i, next := range a.children {
				if !c.equalTriesWithConfig(next, b.children[i], config) {
					return false
				}
			}
			return true
		}
		return false
	}

	if a, ok := a.(*ValueNode); ok {
		if b, ok := b.(*ValueNode); ok {
			eq := a.key == b.key
			eq = eq && a.value == b.value
			eq = eq && (config.ignoreDirtyHash || a.hashStatus == b.hashStatus)
			eq = eq && (config.ignoreFreeze || a.frozen == b.frozen)
			if !eq {
				return false
			}
			if c.config.TrackSuffixLengthsInLeafNodes {
				if a.pathLength != b.pathLength {
					return false
				}
			}
			return true
		}
		return false
	}

	return false

}

func (c *nodeContext) equalTries(a, b NodeReference) bool {
	return c.equalTriesWithConfig(a, b, equalityConfig{})
}

func (c *nodeContext) equalTriesWithConfig(a, b NodeReference, config equalityConfig) bool {
	nodeA, _ := c.getReadAccess(&a)
	nodeB, _ := c.getReadAccess(&b)
	defer nodeA.Release()
	defer nodeB.Release()
	return c.equalWithConfig(nodeA.Get(), nodeB.Get(), config)
}

func (c *nodeContext) release(id *NodeReference) error {
	c.released = append(c.released, id.Id())
	return c.MockNodeManager.release(id)
}

func addressToNibbles(addr common.Address) []Nibble {
	return AddressToNibblePath(addr, nil)
}

func keyToNibbles(key common.Key) []Nibble {
	return KeyToNibblePath(key, nil)
}

// A matcher for references to a given node ID.
func RefTo(id NodeId) gomock.Matcher {
	return refTo{id}
}

type refTo struct {
	id NodeId
}

func (m refTo) Matches(value any) bool {
	val, ok := value.(NodeReference)
	if ok {
		return val.Id() == m.id
	}
	ref, ok := value.(*NodeReference)
	return ok && ref.Id() == m.id
}

func (m refTo) String() string {
	return fmt.Sprintf("reference to %v", m.id)
}
