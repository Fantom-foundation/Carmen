package s4

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	gomock "github.com/golang/mock/gomock"
)

// ----------------------------------------------------------------------------
//                               Empty Node
// ----------------------------------------------------------------------------

func TestEmptyNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)

	addr := common.Address{1}

	empty := EmptyNode{}
	path := addressToNibbles(&addr)
	if info, err := empty.GetAccount(mgr, &addr, path[:]); !info.IsEmpty() || err != nil {
		t.Fatalf("lookup should return empty info, got %v, err %v", info, err)
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
	resId, after := ctxt.Build(&Account{addr, info})

	// The operation is creating one account node.
	ctxt.EXPECT().createAccount().Return(resId, after, nil)
	ctxt.EXPECT().update(resId, after).Return(nil)

	path := addressToNibbles(&addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	if !changed {
		t.Errorf("added account information not indicated as a change")
	}
	if newRoot != resId {
		t.Errorf("failed to return new root node ID, wanted %v, got %v", resId, newRoot)
	}

	got, _ := ctxt.getNode(resId)
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

	path := addressToNibbles(&addr)
	newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info)
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
	path := addressToNibbles(&trg)
	if info, err := node.GetAccount(ctxt, &trg, path[:]); !info.IsEmpty() || err != nil {
		t.Fatalf("lookup should return empty info, got %v, err %v", info, err)
	}

	// Case 2: the trie contains the requested account.
	trg = common.Address{0x81}
	path = addressToNibbles(&trg)
	if res, err := node.GetAccount(ctxt, &trg, path[:]); res != info || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", info, res, err)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != id || changed || err != nil {
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

	info2 := AccountInfo{Nonce: common.Nonce{2}}
	addr := common.Address{0x81}
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &empty); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", id, true, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	wantId := node.(*BranchNode).children[4]
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	wantId := node.(*BranchNode).children[4]
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &empty); newRoot != wantId || !changed || err != nil {
		t.Fatalf("update should return (%v, %v), got (%v, %v), err %v", wantId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(wantId)
	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&trg)
	if res, err := node.GetAccount(ctxt, &trg, path[:]); !res.IsEmpty() || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", AccountInfo{}, res, err)
	}

	// Case 2: locate an existing address
	trg = common.Address{0x12, 0x35}
	path = addressToNibbles(&trg)
	if res, err := node.GetAccount(ctxt, &trg, path[:]); res != info || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", info, res, err)
	}

	// Case 3: locate an address with a partial extension path overlap only
	trg = common.Address{0x12, 0x4F}
	path = addressToNibbles(&trg)
	if res, err := node.GetAccount(ctxt, &trg, path[:]); !res.IsEmpty() || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", AccountInfo{}, res, err)
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
	path := addressToNibbles(&trg)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &trg, path[:], &info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, false, newRoot, changed, err)
	}

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, node)
}

func TestExtensionNode_SetAccount_ExistingLeaf_ChangedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Tag{"A", &Account{common.Address{0x12, 0x35}, info1}},
				8: &Account{common.Address{0x12, 0x38}, info2},
			},
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

	// Attempt to create an existing account.
	trg := common.Address{0x12, 0x35}
	path := addressToNibbles(&trg)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &trg, path[:], &info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
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
	accountId, account := ctxt.Build(&Account{info: info})
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != extensionId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", extensionId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(extensionId)

	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}
	node, _ = ctxt.getNode(branchId)

	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != branchId || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err %v", branchId, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(branchId)
	ctxt.ExpectEqual(t, after, node)
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
	path := addressToNibbles(&addr)
	if res, err := node.GetAccount(mgr, &addr, path[:]); !res.IsEmpty() || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", AccountInfo{}, res, err)
	}

	// Case 2: the node contains the requested info.
	node.address = addr
	if res, err := node.GetAccount(mgr, &addr, path[:]); info != res || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", info, res, err)
	}
}

func TestAccountNode_SetAccount_WithMatchingAccount_SameInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(&addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(&Account{addr, info})

	// Update the account information with the same information.
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, node, node)
}

func TestAccountNode_SetAccount_WithMatchingAccount_DifferentInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(&addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{Nonce: common.Nonce{2}}

	id, node := ctxt.Build(&Account{addr, info1})
	_, after := ctxt.Build(&Account{addr, info2})

	ctxt.EXPECT().update(id, node).Return(nil)

	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_SetAccount_WithMatchingAccount_ZeroInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(&addr)
	info1 := AccountInfo{Nonce: common.Nonce{1}}
	info2 := AccountInfo{}

	id, node := ctxt.Build(&Account{addr, info1})
	_, after := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr, path[:], &info2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(EmptyId())
	ctxt.ExpectEqual(t, after, node)
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

	path := addressToNibbles(&addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr2, path[:], &info2); newRoot != res || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := addressToNibbles(&addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr2, path[:], &info2); newRoot != res || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := addressToNibbles(&addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr2, path[:], &info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := addressToNibbles(&addr2)
	if newRoot, changed, err := node.SetAccount(ctxt, id, &addr2, path[:], &info2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

func TestAccountNode_GetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	node := &AccountNode{}

	key := common.Key{}
	path := keyToNibbles(&key)
	if _, err := node.GetValue(ctxt, &key, path[:]); err == nil {
		t.Fatalf("GetValue call should always return an error")
	}
}

func TestAccountNode_SetValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(&key)
	value := common.Value{1}

	id := AccountId(12)
	node := &AccountNode{}

	if _, _, err := node.SetValue(ctxt, id, &key, path[:], &value); err == nil {
		t.Fatalf("SetValue call should always return an error")
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
	path := addressToNibbles(&addr)
	if _, err := node.GetAccount(ctxt, &addr, path[:]); err == nil {
		t.Fatalf("GetAccount call should always return an error")
	}
}

func TestValueNode_SetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	addr := common.Address{0x21}
	path := addressToNibbles(&addr)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id := ValueId(12)
	node := &ValueNode{}

	if _, _, err := node.SetAccount(ctxt, id, &addr, path[:], &info); err == nil {
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
	path := keyToNibbles(&key)
	if res, err := node.GetValue(mgr, &key, path[:]); res != (common.Value{}) || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", common.Value{}, res, err)
	}

	// Case 2: the node contains the requested info.
	node.key = key
	if res, err := node.GetValue(mgr, &key, path[:]); value != res || err != nil {
		t.Fatalf("lookup should return %v, got %v, err %v", value, res, err)
	}
}

func TestValueNode_SetAccount_WithMatchingKey_SameValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(&key)
	value := common.Value{1}

	id, node := ctxt.Build(&Value{key, value})

	// Update the value with the same value.
	if newRoot, changed, err := node.SetValue(ctxt, id, &key, path[:], &value); newRoot != id || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, false, newRoot, changed, err)
	}
	ctxt.ExpectEqual(t, node, node)
}

func TestValueNode_SetValue_WithMatchingKey_DifferentValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(&key)
	value1 := common.Value{1}
	value2 := common.Value{2}

	id, node := ctxt.Build(&Value{key, value1})
	_, after := ctxt.Build(&Value{key, value2})

	ctxt.EXPECT().update(id, node).Return(nil)

	if newRoot, changed, err := node.SetValue(ctxt, id, &key, path[:], &value2); newRoot != id || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", id, true, newRoot, changed, err)
	}

	ctxt.ExpectEqual(t, after, node)
}

func TestValueNode_SetValue_WithMatchingKey_ZeroValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)

	key := common.Key{0x21}
	path := keyToNibbles(&key)
	value1 := common.Value{1}
	value2 := common.Value{}

	id, node := ctxt.Build(&Value{key, value1})
	_, after := ctxt.Build(Empty{})

	ctxt.EXPECT().release(id).Return(nil)

	if newRoot, changed, err := node.SetValue(ctxt, id, &key, path[:], &value2); !newRoot.IsEmpty() || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", EmptyId(), true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(EmptyId())
	ctxt.ExpectEqual(t, after, node)
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

	path := keyToNibbles(&key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, &key2, path[:], &value2); newRoot != res || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := keyToNibbles(&key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, &key2, path[:], &value2); newRoot != res || !changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, true, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := keyToNibbles(&key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, &key2, path[:], &value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
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

	path := keyToNibbles(&key2)
	if newRoot, changed, err := node.SetValue(ctxt, id, &key2, path[:], &value2); newRoot != res || changed || err != nil {
		t.Fatalf("update should return (%v,%v), got (%v,%v), err: %v", res, false, newRoot, changed, err)
	}

	node, _ = ctxt.getNode(res)
	ctxt.ExpectEqual(t, after, node)
}

// ----------------------------------------------------------------------------
//                               Utilities
// ----------------------------------------------------------------------------

type NodeDesc interface {
	Build(*nodeContext) (NodeId, Node)
}

type Empty struct{}

func (Empty) Build(ctx *nodeContext) (NodeId, Node) {
	return EmptyId(), EmptyNode{}
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

type entry struct {
	id   NodeId
	node Node
}
type nodeContext struct {
	*MockNodeManager
	cache     map[NodeDesc]entry
	tags      map[string]entry
	lastIndex uint32
}

func newNodeContext(ctrl *gomock.Controller) *nodeContext {
	res := &nodeContext{
		MockNodeManager: NewMockNodeManager(ctrl),
		cache:           map[NodeDesc]entry{},
		tags:            map[string]entry{},
	}
	res.EXPECT().getNode(EmptyId()).AnyTimes().Return(EmptyNode{}, nil)
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

func (c *nodeContext) nextIndex() uint32 {
	c.lastIndex++
	return c.lastIndex
}

func (c *nodeContext) Check(t *testing.T, a Node) {
	if err := a.Check(c, nil); err != nil {
		a.Dump(c, "")
		t.Fatalf("inconsistent node structure encountered:\n%v", err)
	}
}

func (c *nodeContext) ExpectEqual(t *testing.T, want, got Node) {
	if !c.equal(want, got) {
		fmt.Printf("Want:\n")
		want.Dump(c, "")
		fmt.Printf("Have:\n")
		got.Dump(c, "")
		t.Errorf("unexpected resulting node structure")
	}
}

func (c *nodeContext) equal(a, b Node) bool {
	if _, ok := a.(EmptyNode); ok {
		_, ok := b.(EmptyNode)
		return ok
	}

	if a, ok := a.(*AccountNode); ok {
		if b, ok := b.(*AccountNode); ok {
			return a.address == b.address && a.info == b.info && c.equalTries(a.state, b.state)
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
			return a.key == b.key && a.value == b.value
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
