package mpt

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
	gomock "go.uber.org/mock/gomock"
)

var PathLengthTracking = MptConfig{
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
	id, node := ctxt.Build(Empty{})

	// The state after the insert.
	afterId, _ := ctxt.Build(&Account{address: addr, info: info})

	// The operation is creating one account node.
	accountId, _ := ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if !changed {
		t.Errorf("added account information not indicated as a change")
	}
	if newRoot != accountId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", accountId, newRoot)
	}

	ctxt.ExpectEqualTries(t, afterId, accountId)
}

func TestEmptyNode_SetAccount_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr := common.Address{1}
	info := AccountInfo{Nonce: common.Nonce{1}}

	// The state before the insert.
	id, node := ctxt.Build(Empty{})

	// The state after the insert with the proper length.
	afterId, _ := ctxt.Build(&Account{address: addr, info: info, pathLength: 33})

	// The operation is creating one account node.
	accountId, _ := ctxt.ExpectCreateAccount()

	path := addressToNibbles(addr)
	path = path[7:] // pretend the node is nested somewhere.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if !changed {
		t.Errorf("added account information not indicated as a change")
	}
	if newRoot != accountId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", accountId, newRoot)
	}

	ctxt.ExpectEqualTries(t, afterId, accountId)
}

func TestEmptyNode_SetAccount_ToEmptyInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{1}
	info := AccountInfo{}

	// The state before the insert.
	id, node := ctxt.Build(Empty{})

	// The state after the insert should remain unchanged.
	afterId, _ := id, node

	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("adding empty account information should have not changed the trie")
	}
	if newRoot != id {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", id, newRoot)
	}

	ctxt.ExpectEqualTries(t, afterId, id)
}

func TestEmptyNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	id, node := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Release(ctxt, id, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
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

	id, node := ctxt.Build(Empty{})
	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)
	depth4 := 4
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth4}).Return(VisitResponseAbort)
	depth6 := 6
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth6}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, EmptyId(), 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, EmptyId(), 4, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, EmptyId(), 6, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

func TestBranchNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	nodeId, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, nodeId)

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

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)
	after, _ := ctxt.Clone(id)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, id, after)
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_NoChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)

	ctxt.Freeze(id)
	after, _ := ctxt.Clone(id)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, id, after)
}

func TestBranchNode_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info1},
		}},
	)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info2},
		}, dirty: []int{8}},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// The account node that is targeted should marked to be updated.
	readHandle := node.GetReadHandle()
	branch := readHandle.Get().(*BranchNode)
	account, _ := ctxt.getMutableNode(branch.children[8])
	ctxt.EXPECT().update(branch.children[8], account)
	account.Release()
	readHandle.Release()

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, id, after)
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	beforeId, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info1},
		}},
	)
	ctxt.Check(t, beforeId)

	afterId, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info1},
			8: &Account{address: common.Address{0x81}, info: info2},
		}, dirty: []int{8}, frozen: []int{4}},
	)
	ctxt.Check(t, afterId)

	// Create and freeze the target node.
	ctxt.Freeze(beforeId)
	id, node := ctxt.Clone(beforeId)

	// This operation should create a new account and branch node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("setting account failed: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen node should never change")
	}
	if id == newRoot {
		t.Errorf("modification did not create a new root")
	}

	ctxt.ExpectEqualTries(t, beforeId, id)
	ctxt.ExpectEqualTries(t, afterId, newRoot)
}

func TestBranchNode_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x21}, info: info},
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: []int{2}},
	)
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x21}, info: info},
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: []int{2}, frozen: []int{4, 8}},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and a new branch.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				0: &Account{address: common.Address{0x40}, info: info},
				1: &Account{address: common.Address{0x41}, info: info},
			}, dirty: []int{0, 1}},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: []int{4}},
	)
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x40}, info: info},
			8: &Account{address: common.Address{0x81}, info: info},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				0: &Account{address: common.Address{0x40}, info: info},
				1: &Account{address: common.Address{0x41}, info: info},
			}, dirty: []int{0, 1}},
			8: &Account{address: common.Address{0x81}, info: info},
		}, dirty: []int{4}, frozen: []int{8}},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and 2 new branches.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateBranch()

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			4: &Tag{"A", &Account{address: common.Address{0x41}, info: info}},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}, dirty: []int{4}},
	)
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, id)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			2: &Account{address: common.Address{0x20}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}, dirty: []int{4}, frozen: []int{2, 8}},
	)
	ctxt.Check(t, after)

	// This situation should create a new branch node to be used as a result.
	ctxt.ExpectCreateBranch()

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info}},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info})
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	ctxt.ExpectCreateTemporaryBranch()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Tag{"R", &Account{address: common.Address{0x41}, info: info, pathLength: 39}},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info, pathLength: 39}},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info, pathLength: 40})
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	// The remaining account is updated because its length has changed.
	accountId, account := ctxt.Get("R")
	accountHandle := account.GetWriteHandle()
	ctxt.EXPECT().update(accountId, accountHandle).Return(nil)
	accountHandle.Release()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Account{address: common.Address{0x41}, info: info, pathLength: 39},
			8: &Account{address: common.Address{0x82}, info: info, pathLength: 39},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Account{address: common.Address{0x41}, info: info, pathLength: 40})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	ctxt.ExpectCreateTemporaryBranch()

	// It also creates a new account node with a modified length.
	ctxt.ExpectCreateAccount()

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
	ctxt.Check(t, id)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4, 1, 2, 3},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info},
			2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info},
		}},
	})
	ctxt.Check(t, after)

	extensionId, extension := ctxt.Get("E")
	extensionHandle := extension.GetWriteHandle()
	ctxt.EXPECT().update(extensionId, extensionHandle).Return(nil)
	extensionHandle.Release()

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	wantId := handle.Get().(*BranchNode).children[4]
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4, 1, 2, 3},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x23, 0x10}, info: info},
			2: &Account{address: common.Address{0x41, 0x23, 0x20}, info: info},
		}, frozen: []int{1, 2}},
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
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				1: &Account{address: common.Address{0x41, 0x20}, info: info},
				2: &Account{address: common.Address{0x42, 0x84}, info: info},
			}},
			8: &Tag{"A", &Account{address: common.Address{0x82}, info: info}},
		}},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x20}, info: info},
			2: &Account{address: common.Address{0x42, 0x84}, info: info},
		}},
		dirtyHash: true,
	})
	ctxt.Check(t, after)

	extensionId, _ := ctxt.ExpectCreateExtension()

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := extensionId
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, wantId)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{children: Children{
			4: &Branch{children: Children{
				1: &Account{address: common.Address{0x41, 0x20}, info: info},
				2: &Account{address: common.Address{0x42, 0x84}, info: info},
			}},
			8: &Account{address: common.Address{0x82}, info: info},
		}},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Extension{
		path: []Nibble{4},
		next: &Branch{children: Children{
			1: &Account{address: common.Address{0x41, 0x20}, info: info},
			2: &Account{address: common.Address{0x42, 0x84}, info: info},
		}, frozen: []int{1, 2}},
		dirtyHash: true,
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
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestBranchNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	id, node := ctxt.Build(&Branch{children: Children{
		1: &Tag{"A", &Account{}},
		4: &Tag{"B", &Account{}},
		8: &Tag{"C", &Account{}},
	}})

	ctxt.EXPECT().release(id).Return(nil)
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("B")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("C")
	ctxt.EXPECT().release(accountId).Return(nil)

	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, id, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
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

	id, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	node1.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)
	node2.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitPruned(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)

	id, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)

	id, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestBranchNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	id, node := ctxt.Build(&Branch{children: Children{
		1: &Mock{node: node1},
		8: &Mock{node: node2},
	}})

	handle := node.GetWriteHandle()
	defer handle.Release()

	node1.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)
	node2.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil) // = aborted

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

func TestExtensionNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, id)

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

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, id)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, trg, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, id)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_UnchangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}},
		},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)
	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Clone(id)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, trg, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, id)
	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, id)
}

func TestExtensionNode_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Tag{"B", &Branch{children: Children{
				5: &Tag{"A", &Account{address: common.Address{0x12, 0x35}, info: info1}},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}}},
		},
	)
	ctxt.Check(t, id)

	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info2},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}, dirty: []int{5}},
			dirtyHash: true,
		},
	)
	ctxt.Check(t, after)

	accountId, account := ctxt.Get("A")
	accountHandle := account.GetWriteHandle()
	ctxt.EXPECT().update(accountId, accountHandle).Return(nil)
	accountHandle.Release()

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, trg, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info1},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}},
		},
	)
	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				5: &Account{address: common.Address{0x12, 0x35}, info: info2},
				8: &Account{address: common.Address{0x12, 0x38}, info: info2},
			}, dirty: []int{5}, frozen: []int{8}},
			dirtyHash: true,
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
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, trg, path[:], info2)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
					dirtyHash: true,
				},
				4: &Account{address: common.Address{0x12, 0x40}, info: info},
			}, dirty: []int{3, 4}},
			dirtyHash: true,
		},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case, one new branch, extension and account is created.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateAccount()
	extensionId, _ := ctxt.ExpectCreateExtension()

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != extensionId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", extensionId, true, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, after, extensionId)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2},
			next: &Branch{children: Children{
				3: &Extension{
					path: []Nibble{4},
					next: &Branch{children: Children{
						0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
						0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
					}, frozen: []int{0xA, 0xE}},
					dirtyHash: true,
				},
				4: &Account{address: common.Address{0x12, 0x40}, info: info},
			}, dirty: []int{3, 4}},
			dirtyHash: true,
		},
	)

	ctxt.Check(t, id)
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
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
				dirtyHash: true, // < if the extension node is reused, this would not be needed; but there is no guarantee for that
			},
			4: &Account{address: common.Address{0x40}, info: info},
		}, dirty: []int{1, 4}},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	ctxt.ExpectCreateAccount()
	branchId, _ := ctxt.ExpectCreateBranch()

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, branchId)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			1: &Extension{
				path: []Nibble{2, 3, 4},
				next: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
					0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
				}, frozen: []int{0xA, 0xE}},
				dirtyHash: true,
			},
			4: &Account{address: common.Address{0x40}, info: info},
		}, dirty: []int{1, 4}},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}, dirty: []int{4, 8}},
			dirtyHash: true,
		},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3, 4},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
				0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
			}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Branch{children: Children{
				4: &Branch{children: Children{
					0xA: &Account{address: common.Address{0x12, 0x34, 0xAB}, info: info},
					0xE: &Account{address: common.Address{0x12, 0x34, 0xEF}, info: info},
				}, frozen: []int{0xA, 0xE}},
				8: &Account{address: common.Address{0x12, 0x38}, info: info},
			}, dirty: []int{4, 8}}, // < TODO: it should be possible to restrict this to {8}
			dirtyHash: true,
		},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
			2: &Account{address: common.Address{0x20}, info: info},
		}, dirty: []int{1, 2}}, // < TODO: could be {2}
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// In this case a branch and account is created and an extension released.
	ctxt.ExpectCreateAccount()
	branchId, _ := ctxt.ExpectCreateBranch()

	ctxt.EXPECT().release(id).Return(nil)

	addr := common.Address{0x20}
	path := addressToNibbles(addr)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, branchId)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x1A}, info: info},
				0xE: &Account{address: common.Address{0x1E}, info: info},
			}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Branch{children: Children{
			1: &Branch{children: Children{
				0xA: &Account{address: common.Address{0x1A}, info: info},
				0xE: &Account{address: common.Address{0x1E}, info: info},
			}, frozen: []int{0xA, 0xE}},
			2: &Account{address: common.Address{0x20}, info: info},
		}, dirty: []int{1, 2}},
	)

	ctxt.Check(t, id)
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
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
			dirtyHash: true,
		},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	extensionId, _ := ctxt.ExpectCreateExtension()
	ctxt.EXPECT().release(extensionId).Return(nil)

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
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
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(
		&Extension{
			path: []Nibble{1, 1},
			next: &Branch{children: Children{
				1: &Account{address: common.Address{0x11, 0x10}, info: info},
				2: &Account{address: common.Address{0x11, 0x20}, info: info},
			}, frozen: []int{1, 2}},
			dirtyHash: true, // < could be optimized away ..
		},
	)

	ctxt.Check(t, id)
	ctxt.Check(t, before)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It also requires a temporary extension.
	extensionId, _ := ctxt.ExpectCreateExtension()
	ctxt.EXPECT().release(extensionId)

	// And it also creates a new extension that constitutes the result.
	ctxt.ExpectCreateExtension()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)

	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info})

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	ctxt.EXPECT().release(id).Return(nil)

	resultId, _ := ctxt.Get("R")

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != resultId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, resultId)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info}},
			}}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info})

	ctxt.Check(t, before)
	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It creates and discards an extension.
	ctxt.ExpectCreateTemporaryExtension()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
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

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Tag{"R", &Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 38}},
				2: &Tag{"A", &Account{address: common.Address{0x12}, info: info, pathLength: 38}},
			}}},
		},
	)

	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 40})

	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// This case eliminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	ctxt.EXPECT().release(id).Return(nil)

	resultId, result := ctxt.Get("R")

	// The result's path length changes, so an update needs to be called.
	// The first time when removing the branch, the second time when removing the extension.
	resultHandle := result.GetWriteHandle()
	ctxt.EXPECT().update(resultId, resultHandle).Times(2)
	resultHandle.Release()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty); newRoot != resultId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultId, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, resultId)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1},
			next: &Tag{"B", &Branch{children: Children{
				1: &Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 38},
				2: &Account{address: common.Address{0x12}, info: info, pathLength: 38},
			}}},
		},
	)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)
	after, _ := ctxt.Build(&Account{address: common.Address{0x11, 0x10}, info: info, pathLength: 40})

	ctxt.Check(t, before)
	ctxt.Check(t, id)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	ctxt.ExpectCreateTemporaryBranch()

	// It creates and discards an extension.
	ctxt.ExpectCreateTemporaryExtension()

	// It also creates a new account with an altered length.
	accountId, account := ctxt.ExpectCreateAccount()

	// There is an extra update call to the account since, 1x by branch, 1x by extension.
	accountHandle := account.GetWriteHandle()
	ctxt.EXPECT().update(accountId, accountHandle)
	accountHandle.Release()

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestExtensionNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	id, node := ctxt.Build(
		&Extension{
			path: []Nibble{1, 2, 3},
			next: &Tag{"C", &Branch{children: Children{
				1: &Tag{"A", &Account{}},
				4: &Tag{"B", &Account{}},
			}}},
		})

	ctxt.EXPECT().release(id).Return(nil)
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("B")
	ctxt.EXPECT().release(accountId).Return(nil)
	branchId, _ := ctxt.Get("C")
	ctxt.EXPECT().release(branchId).Return(nil)

	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, id, handle); err != nil {
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

	id, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	next.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitPrune(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)

	id, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)

	id, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	visitor := NewMockNodeVisitor(ctrl)
	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestExtensionNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	next := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	id, node := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Mock{next},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	next.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil) // = abort

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

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

	id, node := ctxt.Build(&Account{address: addr, info: info})
	backupId, _ := ctxt.Clone(id)

	// Update the account information with the same information.
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backupId, id)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_SameInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	before, _ := ctxt.Build(&Account{address: addr, info: info})
	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)
	after, _ := ctxt.Build(&Account{address: addr, info: info})

	// Update the account information with the same information.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{address: addr, info: info1})
	after, _ := ctxt.Build(&Account{address: addr, info: info2})

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	before, _ := ctxt.Build(&Account{address: addr, info: info1})
	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)
	after, _ := ctxt.Build(&Account{address: addr, info: info2})

	ctxt.ExpectCreateAccount()

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{address: addr, info: info1})
	after, _ := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, EmptyId())
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr, info: info1})
	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)
	after, _ := ctxt.Build(Empty{})

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{address: addr1, info: info1})

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Account{address: addr1, info: info1},
		3: &Account{address: addr2, info: info2},
	}, dirty: []int{2, 3}})

	// This operation creates one new account node and a branch.
	ctxt.ExpectCreateAccount()
	res, _ := ctxt.ExpectCreateBranch()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2); newRoot != res || changed || err != nil {
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

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Account{address: addr1, info: info1},
		3: &Account{address: addr2, info: info2},
	}, dirty: []int{2, 3}})

	// This operation creates one new account node and a branch.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{address: addr1, info: info1})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1},
			0xB: &Account{address: addr2, info: info2},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2); newRoot != res || changed || err != nil {
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

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1},
			0xB: &Account{address: addr2, info: info2},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, PathLengthTracking)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{address: addr1, info: info1, pathLength: 40})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1, pathLength: 36},
			0xB: &Account{address: addr2, info: info2, pathLength: 36},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates one new account, branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()

	// Also the old node is to be updated, since its length changed.
	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	path := addressToNibbles(addr2)
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
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

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Account{address: addr1, info: info1, pathLength: 36},
			0xB: &Account{address: addr2, info: info2, pathLength: 36},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates two new accounts, one branch, and extension node.
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{address: addr1, info: info1})
	after, _ := ctxt.Clone(id)

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Account{address: addr1, info: info1})

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	handle.Release()
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{address: addr1, info: info1})
	after, _ := ctxt.Clone(id)

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	before, _ := ctxt.Build(&Account{address: addr1, info: info1})

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Account{address: addr1, info: info1})

	path := addressToNibbles(addr2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, addr2, path[:], info2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, id)
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

	id := AccountId(12)
	node := &AccountNode{}

	if _, _, err := node.SetValue(ctxt, id, shared.WriteHandle[Node]{}, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
}

func TestAccountNode_Frozen_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id, node := ctxt.Build(&Account{})
	ctxt.Freeze(id)

	handle := node.GetWriteHandle()
	if _, _, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
	handle.Release()
}

func TestAccountNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	id, node := ctxt.Build(&Account{})

	ctxt.EXPECT().release(id).Return(nil)
	handle := node.GetWriteHandle()
	defer handle.Release()
	if err := handle.Get().Release(ctxt, id, handle); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestAccountNode_ReleaseStateTrie(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage, _ := ctxt.Build(&Value{})
	id, node := ctxt.Build(&Account{})

	handle := node.GetWriteHandle()
	defer handle.Release()
	handle.Get().(*AccountNode).storage = storage

	ctxt.EXPECT().release(id).Return(nil)
	ctxt.EXPECT().release(storage).Return(nil)

	if err := handle.Get().Release(ctxt, id, handle); err != nil {
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

	id, node := ctxt.Build(&Account{
		address: addr,
		info:    AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		storage: &Value{key: key, value: value},
	})
	ctxt.Freeze(id)

	// A new account and value is expected to be created.
	newId, _ := ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateValue()

	handle := node.GetWriteHandle()
	defer handle.Release()

	path := keyToNibbles(key)
	newRoot, changed, err := handle.Get().SetSlot(ctxt, id, handle, addr, path[:], key, newValue)
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}

	// check value exists for the original node
	if _, exists, _ := handle.Get().GetSlot(ctxt, addr, path[:], key); !exists {
		t.Errorf("value for key %v should exist", key)
	}

	// check value is gone for the new root
	newHandle, _ := ctxt.getNode(newRoot)
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

	id, node := ctxt.Build(
		&Branch{
			children: Children{
				0xA: &Account{address: addr1, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
					storage:    &ValueWithLength{key: key, value: value, length: 64},
					pathLength: 39},
				0xB: &Account{address: addr2, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAB}},
					pathLength: 39},
			},
		})

	ctxt.Check(t, id)
	ctxt.Freeze(id)

	before, _ := ctxt.Clone(id)

	newInfo := AccountInfo{common.Nonce{1}, common.Balance{100}, common.Hash{0xAA}}
	newAddr := common.Address{0xAA, 0xB}

	after, _ := ctxt.Build(&Branch{
		children: Children{
			0xA: &Branch{
				children: Children{
					0: &Account{address: addr1, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
						storage:    &ValueWithLength{key: key, value: value, length: 64},
						pathLength: 38},
					0xA: &Account{address: newAddr, info: newInfo,
						pathLength: 38},
				},
				dirty: []int{0, 0xA},
			},
			0xB: &Account{address: addr2, info: AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAB}},
				pathLength: 39},
		},
		dirty:  []int{0xA},
		frozen: []int{0xB},
	})
	ctxt.Check(t, after)

	ctxt.ExpectCreateAccount()
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateAccount()
	newId, _ := ctxt.ExpectCreateBranch()

	handle := node.GetWriteHandle()
	path := addressToNibbles(newAddr)
	// This creates a new account on the path of a frozen account
	newRoot, changed, err := handle.Get().SetAccount(ctxt, id, handle, newAddr, path, newInfo)
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestAccountNode_Frozen_ClearStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0xAA}
	key := common.Key{0x12, 0x3A}
	value := common.Value{1}

	id, node := ctxt.Build(&Account{
		address: addr,
		info:    AccountInfo{common.Nonce{1}, common.Balance{1}, common.Hash{0xAA}},
		storage: &Value{key: key, value: value},
	})
	ctxt.Freeze(id)

	newId, _ := ctxt.ExpectCreateAccount() // new account will be created

	handle := node.GetWriteHandle()
	defer handle.Release()

	path := keyToNibbles(key)
	newRoot, changed, err := handle.Get().ClearStorage(ctxt, id, handle, addr, path[:])
	if newRoot != newId || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", newId, false, newRoot, changed, err)
	}

	// check value exists for the original node
	if _, exists, _ := handle.Get().GetSlot(ctxt, addr, path[:], key); !exists {
		t.Errorf("value for key: %s should exist: ", key)
	}

	// check value is gone for the new root
	newHandle, _ := ctxt.getNode(newRoot)
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

	id, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	storage.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(false, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitPrune(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	id, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	id, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseAbort)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

func TestAccountNode_VisitAbortByChild(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	storage := NewMockNode(ctrl)
	visitor := NewMockNodeVisitor(ctrl)

	id, node := ctxt.Build(&Account{
		info:    AccountInfo{Nonce: common.Nonce{1}},
		storage: &Mock{storage},
	})

	handle := node.GetWriteHandle()
	defer handle.Release()

	storage.EXPECT().Visit(gomock.Any(), gomock.Any(), 3, visitor).Return(true, nil)

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got(%v,%v)", abort, err)
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

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

	id := ValueId(12)
	node := &ValueNode{}

	if _, _, err := node.SetAccount(ctxt, id, shared.WriteHandle[Node]{}, addr, path[:], info); err == nil {
		t.Fatalf("SetAccount call should always return an error")
	}
}

func TestValueNode_Frozen_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(&Value{})
	ctxt.Freeze(id)

	handle := node.GetWriteHandle()
	defer handle.Release()
	if _, _, err := handle.Get().SetAccount(ctxt, id, handle, addr, path[:], info); err == nil {
		t.Fatalf("SetAccount call should always return an error")
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

func TestValueNode_SetAccount_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id, node := ctxt.Build(&Value{key, value})
	backup, _ := ctxt.Clone(id)

	// Update the value with the same value.
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backup, id)
}

func TestValueNode_Frozen_SetAccount_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id, node := ctxt.Build(&Value{key, value})
	backup, _ := ctxt.Clone(id)
	ctxt.Freeze(id)

	// Update the value with the same value.
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value)
	if newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()
	ctxt.ExpectEqualTries(t, backup, id)
	ctxt.ExpectEqualTries(t, backup, newRoot)
}

func TestValueNode_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key, value1})
	after, _ := ctxt.Build(&Value{key, value2})

	handle := node.GetWriteHandle()
	ctxt.EXPECT().update(id, handle).Return(nil)

	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	before, _ := ctxt.Build(&Value{key, value1})
	id, node := ctxt.Clone(before)
	after, _ := ctxt.Build(&Value{key, value2})

	ctxt.Freeze(id)

	ctxt.ExpectCreateValue()

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key, value1})
	after, _ := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, EmptyId())
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key, value1})
	id, node := ctxt.Clone(before)
	after, _ := ctxt.Build(Empty{})

	ctxt.Freeze(id)

	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key1, value1})

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Value{key1, value1},
		3: &Value{key2, value2},
	}, dirty: []int{2, 3}})

	// This operation creates one new value node and a branch.
	res, _ := ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2); newRoot != res || changed || err != nil {
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

	before, _ := ctxt.Build(&Value{key1, value1})

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Branch{children: Children{
		2: &Value{key1, value1},
		3: &Value{key2, value2},
	}, dirty: []int{2, 3}})

	// This operation creates one new value node and a branch.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key1, value1})

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Value{key1, value1},
			0xB: &Value{key2, value2},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates one new value, branch, and extension node.
	ctxt.ExpectCreateBranch()
	res, _ := ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2); newRoot != res || changed || err != nil {
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

	before, _ := ctxt.Build(&Value{key1, value1})

	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)

	after, _ := ctxt.Build(&Extension{
		path: []Nibble{1, 2, 3},
		next: &Branch{children: Children{
			0xA: &Value{key1, value1},
			0xB: &Value{key2, value2},
		}, dirty: []int{0xA, 0xB}},
		dirtyHash: true,
	})

	// This operation creates one new value, branch, and extension node.
	ctxt.ExpectCreateBranch()
	ctxt.ExpectCreateExtension()
	ctxt.ExpectCreateValue()

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key1, value1})
	after, _ := ctxt.Clone(id)

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key1, value1})
	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)
	after, _ := ctxt.Build(&Value{key1, value1})

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key1, value1})
	after, _ := ctxt.Clone(id)

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	if newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	handle.Release()

	ctxt.ExpectEqualTries(t, after, id)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	before, _ := ctxt.Build(&Value{key1, value1})
	id, node := ctxt.Clone(before)
	ctxt.Freeze(id)
	after, _ := ctxt.Build(&Value{key1, value1})

	path := keyToNibbles(key2)
	handle := node.GetWriteHandle()
	newRoot, changed, err := handle.Get().SetValue(ctxt, id, handle, key2, path[:], value2)
	handle.Release()
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	ctxt.ExpectEqualTries(t, before, id)
	ctxt.ExpectEqualTries(t, after, newRoot)
}

func TestValueNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	id, node := ctxt.Build(&Value{})

	ctxt.EXPECT().release(id).Return(nil)
	handle := node.GetWriteHandle()
	if err := handle.Get().Release(ctxt, id, handle); err != nil {
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

	id, node := ctxt.Build(&Value{})

	handle := node.GetWriteHandle()
	defer handle.Release()

	depth2 := 2
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth2}).Return(VisitResponseContinue)
	depth4 := 4
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth4}).Return(VisitResponseAbort)
	depth6 := 6
	visitor.EXPECT().Visit(handle.Get(), NodeInfo{Id: id, Depth: &depth6}).Return(VisitResponsePrune)

	if abort, err := handle.Get().Visit(ctxt, id, 2, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got (%v, %v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, id, 4, visitor); !abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (true,nil), got (%v, %v)", abort, err)
	}

	if abort, err := handle.Get().Visit(ctxt, id, 6, visitor); abort || err != nil {
		t.Errorf("unexpected result of visit, wanted (false,nil), got (%v, %v)", abort, err)
	}
}

// ----------------------------------------------------------------------------
//                               Encoders
// ----------------------------------------------------------------------------

func TestAccountNodeEncoderWithNodeHash(t *testing.T) {
	node := AccountNode{
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage: NodeId(12),
		hash:    common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
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
		storage:     NodeId(12),
		storageHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}
	encoder := AccountNodeEncoderWithChildHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	node.hashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}
func TestAccountNodeWithPathLengthEncoderWithNodeHash(t *testing.T) {
	node := AccountNode{
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage:    NodeId(12),
		pathLength: 14,
		hash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
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
		children:         [16]NodeId{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		hashes:           [16]common.Hash{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}},
		embeddedChildren: 12,
	}
	encoder := BranchNodeEncoderWithChildHashes{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := BranchNode{}
	encoder.Load(buffer, &recovered)
	node.hashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestBranchNodeEncoderWithNodeHash(t *testing.T) {
	node := BranchNode{
		children:         [16]NodeId{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		embeddedChildren: 12,
		hash:             common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	encoder := BranchNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := BranchNode{}
	encoder.Load(buffer, &recovered)
	node.dirtyHashes = ^uint16(0)
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
		next:           NodeId(12),
		nextHash:       common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		nextIsEmbedded: true,
	}
	encoder := ExtensionNodeEncoderWithChildHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ExtensionNode{}
	encoder.Load(buffer, &recovered)
	node.hashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}
func TestExtensionNodeEncoderWithNodeHash(t *testing.T) {
	node := ExtensionNode{
		path: Path{
			path:   [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			length: 7,
		},
		next:           NodeId(12),
		hash:           common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		nextIsEmbedded: true,
	}
	encoder := ExtensionNodeEncoderWithNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ExtensionNode{}
	encoder.Load(buffer, &recovered)
	node.nextHashDirty = true
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
	node.hashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeEncoderWithNodeHash(t *testing.T) {
	node := ValueNode{
		key:   common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value: common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		hash:  common.Hash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
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
		value:      common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		pathLength: 12,
	}
	encoder := ValueNodeWithPathLengthEncoderWithoutNodeHash{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	node.hashDirty = true
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeWithPathLengthEncoderWithNodeHash(t *testing.T) {
	node := ValueNode{
		key:        common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value:      common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		hash:       common.Hash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
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
//                               Utilities
// ----------------------------------------------------------------------------

// NodeDesc is used to describe the structure of a MPT node for unit tests. It
// is intended to be used to build convenient, readable test-structures of nodes
// on which operations are to be exercised.
type NodeDesc interface {
	Build(*nodeContext) (NodeId, *shared.Shared[Node])
}

type Empty struct{}

func (Empty) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	return EmptyId(), shared.MakeShared[Node](EmptyNode{})
}

type Mock struct {
	node Node
}

func (m *Mock) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	return ValueId(ctx.nextIndex()), shared.MakeShared[Node](m.node)
}

type Account struct {
	address          common.Address
	info             AccountInfo
	pathLength       byte
	storage          NodeDesc
	storageHashDirty bool
}

func (a *Account) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	storage := EmptyId()
	if a.storage != nil {
		id, _ := ctx.Build(a.storage)
		storage = id
	}
	return AccountId(ctx.nextIndex()), shared.MakeShared[Node](&AccountNode{
		address:          a.address,
		info:             a.info,
		pathLength:       a.pathLength,
		storage:          storage,
		storageHashDirty: a.storageHashDirty,
	})
}

type Children map[Nibble]NodeDesc

type Branch struct {
	children Children
	dirty    []int
	frozen   []int
}

func (b *Branch) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	id := BranchId(ctx.nextIndex())
	res := &BranchNode{}
	for i, desc := range b.children {
		id, _ := ctx.Build(desc)
		res.children[i] = id
	}
	for _, i := range b.dirty {
		res.markChildHashDirty(byte(i))
	}
	for _, i := range b.frozen {
		res.setChildFrozen(byte(i), true)
	}
	return id, shared.MakeShared[Node](res)
}

type Extension struct {
	path      []Nibble
	next      NodeDesc
	dirtyHash bool
}

func (e *Extension) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	id := ExtensionId(ctx.nextIndex())
	res := &ExtensionNode{}
	res.path = CreatePathFromNibbles(e.path)
	res.next, _ = ctx.Build(e.next)
	res.nextHashDirty = e.dirtyHash
	return id, shared.MakeShared[Node](res)
}

type Tag struct {
	label  string
	nested NodeDesc
}

func (t *Tag) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	id, res := ctx.Build(t.nested)
	ctx.tags[t.label] = entry{id, res}
	return id, res
}

type Value struct {
	key   common.Key
	value common.Value
}

func (v *Value) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	return ValueId(ctx.nextIndex()), shared.MakeShared[Node](&ValueNode{
		key:   v.key,
		value: v.value,
	})
}

type ValueWithLength struct {
	key    common.Key
	value  common.Value
	length byte
}

func (v *ValueWithLength) Build(ctx *nodeContext) (NodeId, *shared.Shared[Node]) {
	return ValueId(ctx.nextIndex()), shared.MakeShared[Node](&ValueNode{
		key:        v.key,
		value:      v.value,
		pathLength: v.length,
	})
}

type entry struct {
	id   NodeId
	node *shared.Shared[Node]
}
type nodeContext struct {
	*MockNodeManager
	index     map[NodeId]entry
	cache     map[NodeDesc]entry
	tags      map[string]entry
	lastIndex uint64
	config    MptConfig
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
	res.EXPECT().getHashFor(gomock.Any()).AnyTimes().Return(common.Hash{}, nil)

	// The empty node is always present.
	res.Build(Empty{})

	// Make sure that in the end all node handles have been released.
	t.Cleanup(func() {
		for _, entry := range res.index {
			handle, ok := entry.node.TryGetWriteHandle()
			if !ok {
				t.Errorf("failed to acquire exclusive access to node %v at end of test -- looks like not all handle have been released", entry.id)
			} else {
				handle.Release()
			}
		}
	})

	return res
}

func (c *nodeContext) Build(desc NodeDesc) (NodeId, *shared.Shared[Node]) {
	if desc == nil {
		return EmptyId(), nil
	}
	e, exists := c.cache[desc]
	if exists {
		return e.id, e.node
	}

	id, node := desc.Build(c)
	c.EXPECT().getNode(id).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return node.GetReadHandle(), nil
	})
	c.EXPECT().getMutableNode(id).AnyTimes().DoAndReturn(func(NodeId) (shared.WriteHandle[Node], error) {
		return node.GetWriteHandle(), nil
	})
	c.index[id] = entry{id, node}
	c.cache[desc] = entry{id, node}
	return id, node
}

func (c *nodeContext) ExpectCreateAccount() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Account{})
	c.EXPECT().createAccount().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	c.EXPECT().update(id, handle).Return(nil)
	handle.Release()
	return id, instance
}

func (c *nodeContext) ExpectCreateBranch() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Branch{})
	c.EXPECT().createBranch().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	c.EXPECT().update(id, handle).Return(nil)
	handle.Release()
	return id, instance
}

func (c *nodeContext) ExpectCreateTemporaryBranch() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Branch{})
	c.EXPECT().createBranch().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	c.EXPECT().release(id).Return(nil)
	return id, instance
}

func (c *nodeContext) ExpectCreateExtension() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Extension{})
	c.EXPECT().createExtension().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	c.EXPECT().update(id, handle).Return(nil)
	handle.Release()
	return id, instance
}

func (c *nodeContext) ExpectCreateTemporaryExtension() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Extension{})
	c.EXPECT().createExtension().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	c.EXPECT().release(id).Return(nil)
	return id, instance
}

func (c *nodeContext) ExpectCreateValue() (NodeId, *shared.Shared[Node]) {
	id, instance := c.Build(&Value{})
	c.EXPECT().createValue().DoAndReturn(func() (NodeId, shared.WriteHandle[Node], error) {
		return id, instance.GetWriteHandle(), nil
	})
	handle := instance.GetWriteHandle()
	c.EXPECT().update(id, handle).Return(nil)
	handle.Release()
	return id, instance
}

func (c *nodeContext) Get(label string) (NodeId, *shared.Shared[Node]) {
	e, exists := c.tags[label]
	if !exists {
		panic("requested non-existing element")
	}
	return e.id, e.node
}

func (c *nodeContext) nextIndex() uint64 {
	c.lastIndex++
	return c.lastIndex
}

func (c *nodeContext) Check(t *testing.T, id NodeId) {
	handle := c.tryGetNode(t, id)
	defer handle.Release()
	if err := handle.Get().Check(c, nil); err != nil {
		handle.Get().Dump(c, id, "")
		t.Fatalf("inconsistent node structure encountered:\n%v", err)
	}
}

func (c *nodeContext) Freeze(id NodeId) {
	handle, _ := c.getMutableNode(id)
	defer handle.Release()
	handle.Get().Freeze(c, handle)
}

func (c *nodeContext) tryGetNode(t *testing.T, id NodeId) shared.ReadHandle[Node] {
	entry, found := c.index[id]
	if !found {
		t.Fatalf("unknown node: %v", id)
	}
	handle, succ := entry.node.TryGetReadHandle()
	if !succ {
		t.Fatalf("failed to gain read access to node %v -- forgot to release some handles?", id)
	}
	return handle
}

func (c *nodeContext) ExpectEqualTries(t *testing.T, want, got NodeId) {
	t.Helper()
	wantHandle := c.tryGetNode(t, want)
	defer wantHandle.Release()
	gotHandle := c.tryGetNode(t, got)
	defer gotHandle.Release()
	c.ExpectEqual(t, wantHandle.Get(), gotHandle.Get())
}

func (c *nodeContext) ExpectEqual(t *testing.T, want, got Node) {
	t.Helper()
	if !c.equal(want, got) {
		fmt.Printf("Want:\n")
		want.Dump(c, NodeId(0), "")
		fmt.Printf("Have:\n")
		got.Dump(c, NodeId(0), "")
		t.Errorf("unexpected resulting node structure")
	}
}

func (c *nodeContext) Clone(id NodeId) (NodeId, *shared.Shared[Node]) {
	if id.IsEmpty() {
		return EmptyId(), c.index[id].node
	}

	handle, _ := c.getNode(id)
	defer handle.Release()
	id, res := c.cloneInternal(handle.Get())
	c.EXPECT().getNode(id).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return res.GetReadHandle(), nil
	})
	c.EXPECT().getMutableNode(id).AnyTimes().DoAndReturn(func(NodeId) (shared.WriteHandle[Node], error) {
		return res.GetWriteHandle(), nil
	})
	c.index[id] = entry{id, res}
	return id, res
}

func (c *nodeContext) cloneInternal(node Node) (NodeId, *shared.Shared[Node]) {
	clone := func(id NodeId) NodeId {
		id, _ = c.Clone(id)
		return id
	}

	if a, ok := node.(*AccountNode); ok {
		res := &AccountNode{}
		*res = *a
		res.storage = clone(a.storage)
		return AccountId(c.nextIndex()), shared.MakeShared[Node](res)
	}

	if e, ok := node.(*ExtensionNode); ok {
		res := &ExtensionNode{}
		*res = *e
		res.next = clone(e.next)
		return ExtensionId(c.nextIndex()), shared.MakeShared[Node](res)
	}

	if b, ok := node.(*BranchNode); ok {
		id := BranchId(c.nextIndex())
		res := &BranchNode{}
		*res = *b
		for i, next := range b.children {
			res.children[i] = clone(next)
		}
		return id, shared.MakeShared[Node](res)
	}

	if v, ok := node.(*ValueNode); ok {
		res := &ValueNode{}
		*res = *v
		return ValueId(c.nextIndex()), shared.MakeShared[Node](res)
	}

	panic(fmt.Sprintf("encountered unsupported node type: %v", reflect.TypeOf(node)))
}

func (c *nodeContext) equal(a, b Node) bool {
	if _, ok := a.(EmptyNode); ok {
		_, ok := b.(EmptyNode)
		return ok
	}

	if a, ok := a.(*AccountNode); ok {
		if b, ok := b.(*AccountNode); ok {
			eq := a.address == b.address
			eq = eq && a.info == b.info
			eq = eq && a.storageHashDirty == b.storageHashDirty
			// eq = eq && a.frozen == b.frozen  // < TODO: add support
			eq = eq && c.equalTries(a.storage, b.storage)
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
			eq = eq && a.nextHashDirty == b.nextHashDirty
			eq = eq && a.frozen == b.frozen
			eq = eq && c.equalTries(a.next, b.next)
			return eq
		}
		return false
	}

	if a, ok := a.(*BranchNode); ok {
		if b, ok := b.(*BranchNode); ok {
			/* TODO: add support
			if a.frozen != b.frozen {
				return false
			}
			*/
			if a.dirtyHashes != b.dirtyHashes {
				return false
			}
			if a.frozenChildren != b.frozenChildren {
				return false
			}
			for i, next := range a.children {
				if !c.equalTries(next, b.children[i]) {
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
			// eq = eq && a.frozen == b.frozen // TODO: add support
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

func (c *nodeContext) equalTries(a, b NodeId) bool {
	nodeA, _ := c.getNode(a)
	nodeB, _ := c.getNode(b)
	defer nodeA.Release()
	defer nodeB.Release()
	return c.equal(nodeA.Get(), nodeB.Get())
}

func addressToNibbles(addr common.Address) []Nibble {
	return AddressToNibblePath(addr, nil)
}

func keyToNibbles(key common.Key) []Nibble {
	return KeyToNibblePath(key, nil)
}
