package mpt

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
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
	ctxt := newNodeContext(ctrl)

	addr := common.Address{1}
	info := AccountInfo{Nonce: common.Nonce{1}}

	// The state before the insert.
	id, node := ctxt.Build(Empty{})

	// The state after the insert.
	_, after := ctxt.Build(&Account{addr, info})

	// The operation is creating one account node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)

	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	if !changed {
		t.Errorf("added account information not indicated as a change")
	}
	if newRoot != accountId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", accountId, newRoot)
	}

	got, _ := ctxt.getNode(accountId)
	ctxt.ExpectEqual(t, after, got)
}

func TestEmptyNode_SetAccount_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)

	addr := common.Address{1}
	info := AccountInfo{Nonce: common.Nonce{1}}

	// The state before the insert.
	id, node := ctxt.Build(Empty{})

	// The state after the insert with the proper length.
	_, after := ctxt.Build(&AccountWithLength{addr, info, 33})

	// The operation is creating one account node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)

	path := addressToNibbles(addr)
	path = path[7:] // pretent the node is nested somewhere.
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	if !changed {
		t.Errorf("added account information not indicated as a change")
	}
	if newRoot != accountId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", accountId, newRoot)
	}

	got, _ := ctxt.getNode(accountId)
	ctxt.ExpectEqual(t, after, got)
}

func TestEmptyNode_SetAccount_ToEmptyInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{1}
	info := AccountInfo{}

	// The state before the insert.
	id, node := ctxt.Build(Empty{})

	// The state after the insert should remain unchanged.
	resId, after := id, node

	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	if changed {
		t.Errorf("adding empty account information should have not changed the trie")
	}
	if newRoot != resId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", resId, newRoot)
	}

	got, _ := ctxt.getNode(resId)
	ctxt.ExpectEqual(t, after, got)
}

func TestEmptyNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	id, node := ctxt.Build(Empty{})

	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestEmptyNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	_, node := ctxt.Build(Empty{})

	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

func TestBranchNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	_, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)

	// Case 1: the trie does not contain the requested account.
	trg := common.Address{}
	path := addressToNibbles(trg)
	if info, exists, err := node.GetAccount(ctxt, trg, path[:]); !info.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return empty info, got %v, exists %v, err %v", info, exists, err)
	}

	// Case 2: the trie contains the requested account.
	trg = common.Address{0x81}
	path = addressToNibbles(trg)
	if res, exists, err := node.GetAccount(ctxt, trg, path[:]); res != info || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", info, res, exists, err)
	}
}

func TestBranchNode_SetAccount_WithExistingAccount_NoChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, false, newRoot, changed, err)
	}
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_NoChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node")
	}

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, false, newRoot, changed, err)
	}
}

func TestBranchNode_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info1},
			8: &Account{common.Address{0x81}, info1},
		},
	)
	ctxt.Check(t, node)

	// The account node that is targeted should marked to be upated.
	branch := node.(*BranchNode)
	account, _ := ctxt.getNode(branch.children[8])
	ctxt.EXPECT().update(branch.children[8], account)
	ctxt.EXPECT().invalidateHash(id)

	info2 := AccountInfo{Nonce: common.Nonce{2}}
	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
}

func TestBranchNode_Frozen_SetAccount_WithExistingAccount_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	_, before := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info1},
			8: &Account{common.Address{0x81}, info1},
		},
	)
	ctxt.Check(t, before)

	_, after := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info1},
			8: &Account{common.Address{0x81}, info2},
		},
	)
	ctxt.Check(t, after)

	// Create and freeze the target node.
	id, node := ctxt.Clone(before)
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node")
	}

	// This operation should create a new account and branch node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	addr := common.Address{0x81}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2)
	if err != nil {
		t.Fatalf("setting account failed: %v", err)
	}
	if changed {
		t.Errorf("frozen node should never change")
	}
	if id == newRoot {
		t.Errorf("modification did not create a new root")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(
		&Branch{
			2: &Tag{"A", &Account{common.Address{0x21}, info}},
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, after)

	accountId, account := ctxt.Get("A")
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	ctxt.EXPECT().update(id, node).Return(nil)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InEmptyBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x21}, info},
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and a new branch.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(
		&Branch{
			4: &Branch{
				0: &Account{common.Address{0x40}, info},
				1: &Account{common.Address{0x41}, info},
			},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, after)

	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	ctxt.EXPECT().update(id, node).Return(nil)

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_WithNewAccount_InOccupiedBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Branch{
			4: &Branch{
				0: &Account{common.Address{0x40}, info},
				1: &Account{common.Address{0x41}, info},
			},
			8: &Account{common.Address{0x81}, info},
		},
	)
	ctxt.Check(t, after)

	// This operation is expected to create a new account and 2 new branches.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	branchId, branch = ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x20}, info},
			4: &Tag{"A", &Account{common.Address{0x41}, info}},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x20}, info},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().update(id, node).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_MoreThanTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x20}, info},
			4: &Account{common.Address{0x41}, info},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x20}, info},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, after)

	// This situaton should create a new branch node to be used as a result.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch)

	empty := AccountInfo{}
	addr := common.Address{0x41}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x41}, info},
			8: &Tag{"A", &Account{common.Address{0x82}, info}},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(&Account{common.Address{0x41}, info})
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := node.(*BranchNode).children[4]
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x41}, info},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&Account{common.Address{0x41}, info})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Tag{"R", &AccountWithLength{common.Address{0x41}, info, 39}},
			8: &Tag{"A", &AccountWithLength{common.Address{0x82}, info, 39}},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(&AccountWithLength{common.Address{0x41}, info, 40})
	ctxt.Check(t, after)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	// The remaining account is updated because its length has changed.
	accountId, account := ctxt.Get("R")
	ctxt.EXPECT().update(accountId, account).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := node.(*BranchNode).children[4]
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranches_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &AccountWithLength{common.Address{0x41}, info, 39},
			8: &AccountWithLength{common.Address{0x82}, info, 39},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&AccountWithLength{common.Address{0x41}, info, 40})
	ctxt.Check(t, after)

	// This operation creates a temporary branch node that gets removed again.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// It also creates a new account node with a modified length.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Tag{"E", &Extension{
				[]Nibble{1, 2, 3},
				&Branch{
					1: &Account{common.Address{0x41, 0x23, 0x10}, info},
					2: &Account{common.Address{0x41, 0x23, 0x20}, info},
				},
			}},
			8: &Tag{"A", &Account{common.Address{0x82}, info}},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(&Extension{
		[]Nibble{4, 1, 2, 3},
		&Branch{
			1: &Account{common.Address{0x41, 0x23, 0x10}, info},
			2: &Account{common.Address{0x41, 0x23, 0x20}, info},
		},
	})
	ctxt.Check(t, after)

	extensionId, extension := ctxt.Get("E")
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := node.(*BranchNode).children[4]
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_OnlyTwoBranchesWithRemainingExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Extension{
				[]Nibble{1, 2, 3},
				&Branch{
					1: &Account{common.Address{0x41, 0x23, 0x10}, info},
					2: &Account{common.Address{0x41, 0x23, 0x20}, info},
				},
			},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&Extension{
		[]Nibble{4, 1, 2, 3},
		&Branch{
			1: &Account{common.Address{0x41, 0x23, 0x10}, info},
			2: &Account{common.Address{0x41, 0x23, 0x20}, info},
		},
	})
	ctxt.Check(t, after)

	// This update creates a temporary branch that is released again.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// Also, a new extension node (the result) is expected to be created.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Branch{
				1: &Account{common.Address{0x41, 0x20}, info},
				2: &Account{common.Address{0x42, 0x84}, info},
			},
			8: &Tag{"A", &Account{common.Address{0x82}, info}},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(&Extension{
		[]Nibble{4},
		&Branch{
			1: &Account{common.Address{0x41, 0x20}, info},
			2: &Account{common.Address{0x42, 0x84}, info},
		},
	})
	ctxt.Check(t, after)

	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	ctxt.EXPECT().release(id).Return(nil)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)
	wantId := extensionId
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
}

func TestBranchNode_Frozen_SetAccount_ToDefaultValue_CausingBranchToBeReplacedByExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Branch{
				1: &Account{common.Address{0x41, 0x20}, info},
				2: &Account{common.Address{0x42, 0x84}, info},
			},
			8: &Account{common.Address{0x82}, info},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&Extension{
		[]Nibble{4},
		&Branch{
			1: &Account{common.Address{0x41, 0x20}, info},
			2: &Account{common.Address{0x42, 0x84}, info},
		},
	})
	ctxt.Check(t, after)

	// This update creates a temporary branch that is released again.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// Also, a new extension node (the result) is expected to be created.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension)

	empty := AccountInfo{}
	addr := common.Address{0x82}
	path := addressToNibbles(addr)

	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestBranchNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	id, node := ctxt.Build(&Branch{
		1: &Tag{"A", &Account{}},
		4: &Tag{"B", &Account{}},
		8: &Tag{"C", &Account{}},
	})

	ctxt.EXPECT().release(id).Return(nil)
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("B")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("C")
	ctxt.EXPECT().release(accountId).Return(nil)

	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestBranchNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	node1 := NewMockNode(ctrl)
	node2 := NewMockNode(ctrl)
	node3 := NewMockNode(ctrl)

	_, node := ctxt.Build(&Branch{
		1: &Mock{node: node1},
		4: &Mock{node: node2},
		8: &Mock{node: node3},
	})

	node1.EXPECT().Freeze(gomock.Any()).Return(nil)
	node2.EXPECT().Freeze(gomock.Any()).Return(nil)
	node3.EXPECT().Freeze(gomock.Any()).Return(nil)

	if node.(*BranchNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !node.(*BranchNode).frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

func TestExtensionNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	_, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)
	ctxt.Check(t, node)

	// Case 1: try to locate a non-existing address
	trg := common.Address{}
	path := addressToNibbles(trg)
	if res, exists, err := node.GetAccount(ctxt, trg, path[:]); !res.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", AccountInfo{}, res, exists, err)
	}

	// Case 2: locate an existing address
	trg = common.Address{0x12, 0x35}
	path = addressToNibbles(trg)
	if res, exists, err := node.GetAccount(ctxt, trg, path[:]); res != info || !exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", info, res, exists, err)
	}

	// Case 3: locate an address with a partial extension path overlap only
	trg = common.Address{0x12, 0x4F}
	path = addressToNibbles(trg)
	if res, exists, err := node.GetAccount(ctxt, trg, path[:]); !res.IsEmpty() || exists || err != nil {
		t.Fatalf("lookup should return %v, got %v, exists %v, err %v", AccountInfo{}, res, exists, err)
	}
}

func TestExtensionNode_SetAccount_ExistingLeaf_UnchangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)
	ctxt.Check(t, node)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	if newRoot, changed, err := node.SetAccount(ctxt, id, trg, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, false, newRoot, changed, err)
	}

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, node)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_UnchangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)
	_, before := ctxt.Clone(node)
	_, after := ctxt.Clone(node)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	if newRoot, changed, err := node.SetAccount(ctxt, id, trg, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, false, newRoot, changed, err)
	}

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, node)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Tag{"B", &Branch{
				5: &Tag{"A", &Account{common.Address{0x12, 0x35}, info1}},
				8: &Account{common.Address{0x12, 0x38}, info2},
			}},
		},
	)
	ctxt.Check(t, node)

	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info2},
				8: &Account{common.Address{0x12, 0x38}, info2},
			},
		},
	)
	ctxt.Check(t, after)

	accountId, account := ctxt.Get("A")
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().invalidateHash(branchId).Return()
	ctxt.EXPECT().invalidateHash(id).Return()

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	if newRoot, changed, err := node.SetAccount(ctxt, id, trg, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info1},
				8: &Account{common.Address{0x12, 0x38}, info2},
			},
		},
	)
	ctxt.Check(t, node)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Account{common.Address{0x12, 0x35}, info2},
				8: &Account{common.Address{0x12, 0x38}, info2},
			},
		},
	)
	ctxt.Check(t, after)

	// The following update creates a new account, branch, and extension.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(trg)
	newRoot, changed, err := node.SetAccount(ctxt, id, trg, path[:], info2)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)

	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2},
			&Branch{
				3: &Extension{
					[]Nibble{4},
					&Branch{
						0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
						0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
					},
				},
				4: &Account{common.Address{0x12, 0x40}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch, extension and account is created.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	ctxt.EXPECT().update(id, node).Return(nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != extensionId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", extensionId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(extensionId)

	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_PartialExtensionCovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2},
			&Branch{
				3: &Extension{
					[]Nibble{4},
					&Branch{
						0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
						0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
					},
				},
				4: &Account{common.Address{0x12, 0x40}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch, two extensions, and account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)
	extensionId, extension = ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)

	_, after := ctxt.Build(
		&Branch{
			1: &Extension{
				[]Nibble{2, 3, 4},
				&Branch{
					0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
					0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
				},
			},
			4: &Account{common.Address{0x40}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	ctxt.EXPECT().update(id, node).Return(nil)

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(branchId)

	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Branch{
			1: &Extension{
				[]Nibble{2, 3, 4},
				&Branch{
					0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
					0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
				},
			},
			4: &Account{common.Address{0x40}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	addr := common.Address{0x40}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)

	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				4: &Branch{
					0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
					0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
				},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	ctxt.EXPECT().update(id, node).Return(nil)

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_NoRemainingSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3, 4},
			&Branch{
				0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
				0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
			},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				4: &Branch{
					0xA: &Account{common.Address{0x12, 0x34, 0xAB}, info},
					0xE: &Account{common.Address{0x12, 0x34, 0xEF}, info},
				},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch, account, and extension is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Branch{
				0xA: &Account{common.Address{0x1A}, info},
				0xE: &Account{common.Address{0x1E}, info},
			},
		},
	)

	_, after := ctxt.Build(
		&Branch{
			1: &Branch{
				0xA: &Account{common.Address{0x1A}, info},
				0xE: &Account{common.Address{0x1E}, info},
			},
			2: &Account{common.Address{0x20}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case a banch and account is created and an extension released.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	ctxt.EXPECT().release(id).Return(nil)

	addr := common.Address{0x20}
	path := addressToNibbles(addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(branchId)
	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_NewAccount_ExtensionBecomesObsolete(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Branch{
				0xA: &Account{common.Address{0x1A}, info},
				0xE: &Account{common.Address{0x1E}, info},
			},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Branch{
			1: &Branch{
				0xA: &Account{common.Address{0x1A}, info},
				0xE: &Account{common.Address{0x1E}, info},
			},
			2: &Account{common.Address{0x20}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, before)
	ctxt.Check(t, after)

	// The following update creates and discards a temporary extension.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().release(extensionId)

	// Also, the creation of a new account.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account)

	// And the creation of a new branch.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch)

	addr := common.Address{0x20}
	path := addressToNibbles(addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &Branch{
					1: &Account{common.Address{0x11, 0x10}, info},
					2: &Account{common.Address{0x11, 0x20}, info},
				},
				2: &Tag{"A", &Account{common.Address{0x12}, info}},
			}},
		},
	)

	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 1},
			&Branch{
				1: &Account{common.Address{0x11, 0x10}, info},
				2: &Account{common.Address{0x11, 0x20}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// This case elminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)
	ctxt.EXPECT().release(extensionId).Return(nil)

	ctxt.EXPECT().update(id, node).Return(nil)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionFusesWithNextExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &Branch{
					1: &Account{common.Address{0x11, 0x10}, info},
					2: &Account{common.Address{0x11, 0x20}, info},
				},
				2: &Tag{"A", &Account{common.Address{0x12}, info}},
			}},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(
		&Extension{
			[]Nibble{1, 1},
			&Branch{
				1: &Account{common.Address{0x11, 0x10}, info},
				2: &Account{common.Address{0x11, 0x20}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, before)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// It also requires a temporary extension.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension)
	ctxt.EXPECT().release(extensionId)

	// And it also creates a new extension that constitutes the result.
	extensionId, extension = ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &Tag{"R", &Account{common.Address{0x11, 0x10}, info}},
				2: &Tag{"A", &Account{common.Address{0x12}, info}},
			}},
		},
	)

	_, after := ctxt.Build(&Account{common.Address{0x11, 0x10}, info})

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// This case elminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	ctxt.EXPECT().release(id).Return(nil)

	resultId, result := ctxt.Get("R")

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != resultId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultId, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, result)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &Tag{"R", &Account{common.Address{0x11, 0x10}, info}},
				2: &Tag{"A", &Account{common.Address{0x12}, info}},
			}},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&Account{common.Address{0x11, 0x10}, info})

	ctxt.Check(t, before)
	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// It creates and discards an extension.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().release(extensionId)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	rId, _ := ctxt.Get("R")
	if newRoot != rId {
		t.Errorf("operatoin should return pre-existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &Tag{"R", &AccountWithLength{common.Address{0x11, 0x10}, info, 38}},
				2: &Tag{"A", &AccountWithLength{common.Address{0x12}, info, 38}},
			}},
		},
	)

	_, after := ctxt.Build(&AccountWithLength{common.Address{0x11, 0x10}, info, 40})

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// This case elminates an account and a branch. Also, it introduces
	// a temporary extension that is removed again.
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId)

	branchId, _ := ctxt.Get("B")
	ctxt.EXPECT().release(branchId)

	ctxt.EXPECT().release(id).Return(nil)

	resultId, result := ctxt.Get("R")

	// The result's path length changes, so an update needs to be called.
	// The first time when removing the branch, the second time when removing the extension.
	ctxt.EXPECT().update(resultId, result).Times(2)

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty); newRoot != resultId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", resultId, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, result)
}

func TestExtensionNode_Frozen_SetAccount_RemovedAccount_ExtensionReplacedByLeaf_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1},
			&Tag{"B", &Branch{
				1: &AccountWithLength{common.Address{0x11, 0x10}, info, 38},
				2: &AccountWithLength{common.Address{0x12}, info, 38},
			}},
		},
	)
	node.Freeze(ctxt)

	_, before := ctxt.Clone(node)
	_, after := ctxt.Build(&AccountWithLength{common.Address{0x11, 0x10}, info, 40})

	ctxt.Check(t, before)
	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// The following update creates and release a temporary branch.
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(branchId)

	// It creates and discards an extension.
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().release(extensionId)

	// It also creates a new account with an altered length.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Times(2) // 1x by branch, 1x by extension

	addr := common.Address{0x12}
	path := addressToNibbles(addr)
	empty := AccountInfo{}
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], empty)
	if err != nil {
		t.Fatalf("failed to set account for extension node: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestExtensionNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Tag{"C", &Branch{
				1: &Tag{"A", &Account{}},
				4: &Tag{"B", &Account{}},
			}},
		})

	ctxt.EXPECT().release(id).Return(nil)
	accountId, _ := ctxt.Get("A")
	ctxt.EXPECT().release(accountId).Return(nil)
	accountId, _ = ctxt.Get("B")
	ctxt.EXPECT().release(accountId).Return(nil)
	branchId, _ := ctxt.Get("C")
	ctxt.EXPECT().release(branchId).Return(nil)

	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestExtensionNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	next := NewMockNode(ctrl)

	_, node := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Mock{next},
	})

	next.EXPECT().Freeze(gomock.Any()).Return(nil)

	if node.(*ExtensionNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !node.(*ExtensionNode).frozen {
		t.Errorf("node not marked as frozen after call")
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
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(&Account{addr, info})

	// Update the account information with the same information.
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, node, node)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_SameInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	_, before := ctxt.Build(&Account{addr, info})
	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)
	_, after := ctxt.Build(&Account{addr, info})

	// Update the account information with the same information.
	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{addr, info1})
	_, after := ctxt.Build(&Account{addr, info2})

	ctxt.EXPECT().update(id, node).Return(nil)

	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	_, before := ctxt.Build(&Account{addr, info1})
	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)
	_, after := ctxt.Build(&Account{addr, info2})

	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)

	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{addr, info1})
	_, after := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	if newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(EmptyId())
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	_, before := ctxt.Build(&Account{addr, info1})
	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)
	_, after := ctxt.Build(Empty{})

	newRoot, changed, err := node.SetAccount(ctxt, id, addr, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{addr1, info1})

	res, after := ctxt.Build(&Branch{
		2: &Account{addr1, info1},
		3: &Account{addr2, info2},
	})

	// This operation creates one new account node and a branch.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().createBranch().Return(res, after, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	ctxt.EXPECT().update(res, after).Return(nil)

	path := addressToNibbles(addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_NoCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	_, before := ctxt.Build(&Account{addr1, info1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Branch{
		2: &Account{addr1, info1},
		3: &Account{addr2, info2},
	})

	// This operation creates one new account node and a branch.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	path := addressToNibbles(addr2)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{addr1, info1})

	res, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &Account{addr1, info1},
			0xB: &Account{addr2, info2},
		},
	})

	// This operation creates one new account, branch, and extension node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().createExtension().Return(res, after, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	ctxt.EXPECT().update(res, after).Return(nil)

	path := addressToNibbles(addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	_, before := ctxt.Build(&Account{addr1, info1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &Account{addr1, info1},
			0xB: &Account{addr2, info2},
		},
	})

	// This operation creates one new account, branch, and extension node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	path := addressToNibbles(addr2)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&AccountWithLength{addr1, info1, 40})

	res, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &AccountWithLength{addr1, info1, 36},
			0xB: &AccountWithLength{addr2, info2, 36},
		},
	})

	// This operation creates one new account, branch, and extension node.
	accountId, account := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().createExtension().Return(res, after, nil)
	ctxt.EXPECT().update(accountId, account).Return(nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	ctxt.EXPECT().update(res, after).Return(nil)

	// Also the old node is to be updated, since its length changed.
	ctxt.EXPECT().update(id, node).Return(nil)

	path := addressToNibbles(addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_NonZeroInfo_WithLengthTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(ctrl, PathLengthTracking)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	_, before := ctxt.Build(&AccountWithLength{addr1, info1, 40})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &AccountWithLength{addr1, info1, 36},
			0xB: &AccountWithLength{addr2, info2, 36},
		},
	})

	// This operation creates two new accounts, one branch, and extension node.
	accountId1, account1 := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId1, account1, nil)
	ctxt.EXPECT().update(accountId1, account1).Return(nil)
	accountId2, account2 := ctxt.Build(&Account{})
	ctxt.EXPECT().createAccount().Return(accountId2, account2, nil)
	ctxt.EXPECT().update(accountId2, account2).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	path := addressToNibbles(addr2)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{addr1, info1})
	res, after := id, node

	path := addressToNibbles(addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_NoCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x21}
	addr2 := common.Address{0x34}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	_, before := ctxt.Build(&Account{addr1, info1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Account{addr1, info1})

	path := addressToNibbles(addr2)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{addr1, info1})

	res, after := id, node

	path := addressToNibbles(addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_Frozen_SetAccount_WithDifferentAccount_WithCommonPrefix_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr1 := common.Address{0x12, 0x3A}
	addr2 := common.Address{0x12, 0x3B}
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	_, before := ctxt.Build(&Account{addr1, info1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Account{addr1, info1})

	path := addressToNibbles(addr2)
	newRoot, changed, err := node.SetAccount(ctxt, id, addr2, path[:], info2)
	if err != nil {
		t.Fatalf("failed to SetAccount on AccountNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestAccountNode_GetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	node := &AccountNode{}

	key := common.Key{}
	path := keyToNibbles(key)
	if _, _, err := node.GetValue(ctxt, key, path[:]); err == nil {
		t.Fatalf("GetValue call should always return an error")
	}
}

func TestAccountNode_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id := AccountId(12)
	node := &AccountNode{}

	if _, _, err := node.SetValue(ctxt, id, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
}

func TestAccountNode_Frozen_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id := AccountId(12)
	node := &AccountNode{}
	node.Freeze(ctxt)

	if _, _, err := node.SetValue(ctxt, id, key, path[:], value); err == nil {
		t.Fatalf("SetValue call should always return an error")
	}
}

func TestAccountNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	id, node := ctxt.Build(&Account{})

	ctxt.EXPECT().release(id).Return(nil)
	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestAccountNode_ReleaseStateTrie(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	storage, _ := ctxt.Build(&Value{})
	id, node := ctxt.Build(&Account{})
	node.(*AccountNode).storage = storage

	ctxt.EXPECT().release(id).Return(nil)
	ctxt.EXPECT().release(storage).Return(nil)

	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestAccountNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	storageRoot := NewMockNode(ctrl)

	storage, _ := ctxt.Build(&Mock{storageRoot})
	_, node := ctxt.Build(&Account{})
	node.(*AccountNode).storage = storage

	storageRoot.EXPECT().Freeze(gomock.Any()).Return(nil)

	if node.(*AccountNode).frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !node.(*AccountNode).frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

func TestValueNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	node := &ValueNode{}

	addr := common.Address{}
	path := addressToNibbles(addr)
	if _, _, err := node.GetAccount(ctxt, addr, path[:]); err == nil {
		t.Fatalf("GetAccount call should always return an error")
	}
}

func TestValueNode_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id := ValueId(12)
	node := &ValueNode{}

	if _, _, err := node.SetAccount(ctxt, id, addr, path[:], info); err == nil {
		t.Fatalf("SetAccount call should always return an error")
	}
}

func TestValueNode_Frozen_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id := ValueId(12)
	node := &ValueNode{}
	node.Freeze(ctxt)

	if _, _, err := node.SetAccount(ctxt, id, addr, path[:], info); err == nil {
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
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id, node := ctxt.Build(&Value{key, value})

	// Update the value with the same value.
	if newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, node, node)
}

func TestValueNode_Frozen_SetAccount_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value := common.Value{1}

	id, node := ctxt.Build(&Value{key, value})
	node.Freeze(ctxt)

	// Update the value with the same value.
	if newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, node, node)
}

func TestValueNode_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key, value1})
	_, after := ctxt.Build(&Value{key, value2})

	ctxt.EXPECT().update(id, node).Return(nil)

	if newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	_, before := ctxt.Build(&Value{key, value1})
	id, node := ctxt.Clone(before)
	_, after := ctxt.Build(&Value{key, value2})

	node.Freeze(ctxt)

	valueId, value := ctxt.Build(&Value{})
	ctxt.EXPECT().createValue().Return(valueId, value, nil)
	ctxt.EXPECT().update(valueId, value).Return(nil)

	newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key, value1})
	_, after := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	if newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(EmptyId())
	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(key)
	value1 := common.Value{1}
	value2 := common.Value{}

	_, before := ctxt.Build(&Value{key, value1})
	id, node := ctxt.Clone(before)
	_, after := ctxt.Build(Empty{})

	node.Freeze(ctxt)

	newRoot, changed, err := node.SetValue(ctxt, id, key, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key1, value1})

	res, after := ctxt.Build(&Branch{
		2: &Value{key1, value1},
		3: &Value{key2, value2},
	})

	// This operation creates one new value node and a branch.
	valueId, value := ctxt.Build(&Value{})
	ctxt.EXPECT().createValue().Return(valueId, value, nil)
	ctxt.EXPECT().createBranch().Return(res, after, nil)
	ctxt.EXPECT().update(valueId, value).Return(nil)
	ctxt.EXPECT().update(res, after).Return(nil)

	path := keyToNibbles(key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_NoCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{2}

	_, before := ctxt.Build(&Value{key1, value1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Branch{
		2: &Value{key1, value1},
		3: &Value{key2, value2},
	})

	// This operation creates one new value node and a branch.
	valueId, value := ctxt.Build(&Value{})
	ctxt.EXPECT().createValue().Return(valueId, value, nil)
	ctxt.EXPECT().update(valueId, value).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)

	path := keyToNibbles(key2)
	newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key1, value1})

	res, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &Value{key1, value1},
			0xB: &Value{key2, value2},
		},
	})

	// This operation creates one new value, branch, and extension node.
	valueId, value := ctxt.Build(&Value{})
	ctxt.EXPECT().createValue().Return(valueId, value, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().createExtension().Return(res, after, nil)
	ctxt.EXPECT().update(valueId, value).Return(nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	ctxt.EXPECT().update(res, after).Return(nil)

	path := keyToNibbles(key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_WithCommonPrefix_NonZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{2}

	_, before := ctxt.Build(&Value{key1, value1})

	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)

	_, after := ctxt.Build(&Extension{
		[]Nibble{1, 2, 3},
		&Branch{
			0xA: &Value{key1, value1},
			0xB: &Value{key2, value2},
		},
	})

	// This operation creates one new value, branch, and extension node.
	valueId, value := ctxt.Build(&Value{})
	ctxt.EXPECT().createValue().Return(valueId, value, nil)
	ctxt.EXPECT().update(valueId, value).Return(nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().update(branchId, branch).Return(nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)
	ctxt.EXPECT().update(extensionId, extension).Return(nil)

	path := keyToNibbles(key2)
	newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key1, value1})
	res, after := id, node

	path := keyToNibbles(key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_NoCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x21}
	key2 := common.Key{0x34}
	value1 := common.Value{1}
	value2 := common.Value{}

	_, before := ctxt.Build(&Value{key1, value1})
	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)
	_, after := ctxt.Build(&Value{key1, value1})

	path := keyToNibbles(key2)
	newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key1, value1})

	res, after := id, node

	path := keyToNibbles(key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_Frozen_SetValue_WithDifferentKey_WithCommonPrefix_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key1 := common.Key{0x12, 0x3A}
	key2 := common.Key{0x12, 0x3B}
	value1 := common.Value{1}
	value2 := common.Value{}

	_, before := ctxt.Build(&Value{key1, value1})
	id, node := ctxt.Clone(before)
	node.Freeze(ctxt)
	_, after := ctxt.Build(&Value{key1, value1})

	path := keyToNibbles(key2)
	newRoot, changed, err := node.SetValue(ctxt, id, key2, path[:], value2)
	if err != nil {
		t.Fatalf("failed to SetValue on frozen ValueNode: %v", err)
	}
	if changed {
		t.Errorf("frozen nodes should never change")
	}
	if newRoot != id {
		t.Errorf("update should return existing node")
	}

	res, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, before, node)
	ctxt.ExpectEqual(t, after, res)
}

func TestValueNode_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	id, node := ctxt.Build(&Value{})

	ctxt.EXPECT().release(id).Return(nil)
	if err := node.Release(ctxt, id); err != nil {
		t.Errorf("failed to release node: %v", err)
	}
}

func TestValueNode_Freeze(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	node := &ValueNode{}

	if node.frozen {
		t.Errorf("node was created in frozen state")
	}
	if err := node.Freeze(ctxt); err != nil {
		t.Errorf("failed to freeze node: %v", err)
	}
	if !node.frozen {
		t.Errorf("node not marked as frozen after call")
	}
}

// ----------------------------------------------------------------------------
//                               Encoders
// ----------------------------------------------------------------------------

func TestAccountNodeEncoder(t *testing.T) {
	node := AccountNode{
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage: NodeId(12),
	}
	encoder := AccountNodeEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestAccountNodeWithPathLengthEncoder(t *testing.T) {
	node := AccountNode{
		info: AccountInfo{
			Nonce:    common.Nonce{1, 2, 3, 4, 5, 6, 7, 8},
			Balance:  common.Balance{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			CodeHash: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		},
		storage:    NodeId(12),
		pathLength: 14,
	}
	encoder := AccountNodeWithPathLengthEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := AccountNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestBranchNodeEncoder(t *testing.T) {
	node := BranchNode{
		children: [16]NodeId{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	encoder := BranchNodeEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := BranchNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestExtensionNodeEncoder(t *testing.T) {
	node := ExtensionNode{
		path: Path{
			path:   [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			length: 7,
		},
		next: NodeId(12),
	}
	encoder := ExtensionNodeEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ExtensionNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeEncoder(t *testing.T) {
	node := ValueNode{
		key:   common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value: common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}
	encoder := ValueNodeEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	encoder.Store(buffer, &node)
	recovered := ValueNode{}
	encoder.Load(buffer, &recovered)
	if !reflect.DeepEqual(node, recovered) {
		t.Errorf("encoding/decoding failed, wanted %v, got %v", node, recovered)
	}
}

func TestValueNodeWithPathLengthEncoder(t *testing.T) {
	node := ValueNode{
		key:        common.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		value:      common.Value{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		pathLength: 12,
	}
	encoder := ValueNodeWithPathLengthEncoder{}
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
// on which oeprations are to be exercised.
type NodeDesc interface {
	Build(*nodeContext) (NodeId, Node)
}

type Empty struct{}

func (Empty) Build(ctx *nodeContext) (NodeId, Node) {
	return EmptyId(), EmptyNode{}
}

type Mock struct {
	node Node
}

func (m *Mock) Build(ctx *nodeContext) (NodeId, Node) {
	return ValueId(ctx.nextIndex()), m.node
}

type Account struct {
	address common.Address
	info    AccountInfo
}

func (a *Account) Build(ctx *nodeContext) (NodeId, Node) {
	return AccountId(ctx.nextIndex()), &AccountNode{
		address: a.address,
		info:    a.info,
	}
}

type AccountWithLength struct {
	address common.Address
	info    AccountInfo
	length  byte
}

func (a *AccountWithLength) Build(ctx *nodeContext) (NodeId, Node) {
	return AccountId(ctx.nextIndex()), &AccountNode{
		address:    a.address,
		info:       a.info,
		pathLength: a.length,
	}
}

type Branch map[Nibble]NodeDesc

func (b *Branch) Build(ctx *nodeContext) (NodeId, Node) {
	id := BranchId(ctx.nextIndex())
	res := &BranchNode{}
	for i, desc := range *b {
		id, _ := ctx.Build(desc)
		res.children[i] = id
	}
	return id, res
}

type Extension struct {
	path []Nibble
	next NodeDesc
}

func (e *Extension) Build(ctx *nodeContext) (NodeId, Node) {
	id := ExtensionId(ctx.nextIndex())
	res := &ExtensionNode{}
	res.path = CreatePathFromNibbles(e.path)
	res.next, _ = ctx.Build(e.next)
	return id, res
}

type Tag struct {
	label  string
	nested NodeDesc
}

func (t *Tag) Build(ctx *nodeContext) (NodeId, Node) {
	id, res := ctx.Build(t.nested)
	ctx.tags[t.label] = entry{id, res}
	return id, res
}

type Value struct {
	key   common.Key
	value common.Value
}

func (v *Value) Build(ctx *nodeContext) (NodeId, Node) {
	return ValueId(ctx.nextIndex()), &ValueNode{
		key:   v.key,
		value: v.value,
	}
}

type ValueWithLength struct {
	key    common.Key
	value  common.Value
	length byte
}

func (v *ValueWithLength) Build(ctx *nodeContext) (NodeId, Node) {
	return ValueId(ctx.nextIndex()), &ValueNode{
		key:        v.key,
		value:      v.value,
		pathLength: v.length,
	}
}

type entry struct {
	id   NodeId
	node Node
}
type nodeContext struct {
	*MockNodeManager
	cache     map[NodeDesc]entry
	tags      map[string]entry
	lastIndex uint64
	config    MptConfig
}

func newNodeContext(ctrl *gomock.Controller) *nodeContext {
	return newNodeContextWithConfig(ctrl, S4Config)
}

func newNodeContextWithConfig(ctrl *gomock.Controller, config MptConfig) *nodeContext {
	res := &nodeContext{
		MockNodeManager: NewMockNodeManager(ctrl),
		cache:           map[NodeDesc]entry{},
		tags:            map[string]entry{},
		config:          config,
	}
	res.EXPECT().getConfig().AnyTimes().Return(config)
	res.EXPECT().getNode(EmptyId()).AnyTimes().Return(EmptyNode{}, nil)
	res.EXPECT().getHashFor(gomock.Any()).AnyTimes().Return(common.Hash{}, nil)
	return res
}

func (c *nodeContext) Build(desc NodeDesc) (NodeId, Node) {
	if desc == nil {
		return EmptyId(), EmptyNode{}
	}
	e, exists := c.cache[desc]
	if exists {
		return e.id, e.node
	}

	id, node := desc.Build(c)
	c.EXPECT().getNode(id).AnyTimes().Return(node, nil)
	c.cache[desc] = entry{id, node}
	return id, node
}

func (c *nodeContext) Get(label string) (NodeId, Node) {
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

func (c *nodeContext) Check(t *testing.T, a Node) {
	if err := a.Check(c, nil); err != nil {
		a.Dump(c, NodeId(0), "")
		t.Fatalf("inconsistent node structure encountered:\n%v", err)
	}
}

func (c *nodeContext) ExpectEqual(t *testing.T, want, got Node) {
	if !c.equal(want, got) {
		fmt.Printf("Want:\n")
		want.Dump(c, NodeId(0), "")
		fmt.Printf("Have:\n")
		got.Dump(c, NodeId(0), "")
		t.Errorf("unexpected resulting node structure")
	}
}

func (c *nodeContext) Clone(node Node) (NodeId, Node) {
	id, res := c.cloneInternal(node)
	c.EXPECT().getNode(id).AnyTimes().Return(res, nil)
	return id, res
}

func (c *nodeContext) cloneInternal(node Node) (NodeId, Node) {
	if _, ok := node.(EmptyNode); ok {
		return EmptyId(), EmptyNode{}
	}

	clone := func(id NodeId) NodeId {
		original, _ := c.getNode(id)
		cloneId, _ := c.Clone(original)
		return cloneId
	}

	if a, ok := node.(*AccountNode); ok {
		return AccountId(c.nextIndex()), &AccountNode{
			address:    a.address,
			info:       a.info,
			storage:    clone(a.storage),
			pathLength: a.pathLength,
		}
	}

	if e, ok := node.(*ExtensionNode); ok {
		return ExtensionId(c.nextIndex()), &ExtensionNode{
			path: e.path,
			next: clone(e.next),
		}
	}

	if b, ok := node.(*BranchNode); ok {
		id := BranchId(c.nextIndex())
		res := &BranchNode{}
		for i, next := range b.children {
			res.children[i] = clone(next)
		}
		return id, res
	}

	if v, ok := node.(*ValueNode); ok {
		return ValueId(c.nextIndex()), &ValueNode{
			key:        v.key,
			value:      v.value,
			pathLength: v.pathLength,
		}
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
			if !(a.address == b.address && a.info == b.info && c.equalTries(a.storage, b.storage)) {
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
			return a.path == b.path && c.equalTries(a.next, b.next)
		}
		return false
	}

	if a, ok := a.(*BranchNode); ok {
		if b, ok := b.(*BranchNode); ok {
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
			if !(a.key == b.key && a.value == b.value) {
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
	return c.equal(nodeA, nodeB)
}

func addressToNibbles(addr common.Address) []Nibble {
	return AddressToNibblePath(addr, nil)
}

func keyToNibbles(key common.Key) []Nibble {
	return KeyToNibblePath(key, nil)
}
