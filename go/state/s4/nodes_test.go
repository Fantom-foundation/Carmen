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
	if leaf, err := empty.GetAccount(mgr, &addr, path[:]); leaf != nil || err != nil {
		t.Fatalf("lookup should return nil pointer, got %v, err %v", leaf, err)
	}
}

func TestEmptyNode_GetOrCreateAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)

	// The node to be created.
	accountNodeId := AccountId(123)
	accountNode := &AccountNode{}
	mgr.EXPECT().createAccount().Return(accountNodeId, accountNode, nil)

	addr := common.Address{1}

	empty := EmptyNode{}
	path := addressToNibbles(&addr)
	newRoot, leaf, err := empty.GetOrCreateAccount(mgr, EmptyId(), &addr, path[:])
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	if newRoot != accountNodeId || leaf != accountNode {
		t.Errorf("produced wrong results, wanted (%v,%v), got (%v,%v)", accountNodeId, accountNode, newRoot, leaf)
	}
	if got, want := accountNode.address, addr; got != want {
		t.Errorf("invalid path in account node, wanted %v, got %v", want, got)
	}
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

func TestBranchNode_GetAccount2(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	_, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Tag{"A", &Account{common.Address{0x81}, info}},
		},
	)
	ctxt.Check(t, node)

	// Case 1: the trie does not contain the requested value.
	trg := common.Address{}
	path := addressToNibbles(&trg)
	if leaf, err := node.GetAccount(ctxt, &trg, path[:]); leaf != nil || err != nil {
		t.Fatalf("lookup should return nil pointer, got %v, err %v", leaf, err)
	}

	// Case 2: the trie contains the requested value.
	trg = common.Address{0x81}
	path = addressToNibbles(&trg)
	leaf := ctxt.Get("A")
	if res, err := node.GetAccount(ctxt, &trg, path[:]); res != leaf || err != nil {
		t.Fatalf("lookup should return %p, got %v, err %v", leaf, res, err)
	}
}

func TestBranchNode_GetOrCreateAccount_WithExistingAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			4: &Account{common.Address{0x40}, info},
			8: &Tag{"A", &Account{common.Address{0x81}, info}},
		},
	)
	ctxt.Check(t, node)

	// Trie creating an already existing account.
	addr := common.Address{0x81}
	path := addressToNibbles(&addr)
	leaf := ctxt.Get("A")
	if newRoot, res, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != id || res != leaf || err != nil {
		t.Fatalf("lookup should return (%v, %p), got (%v, %p), err %v", id, leaf, newRoot, res, err)
	}
}

func TestBranchNode_GetOrCreateAccount_InExistingBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			3: &Account{common.Address{0x3a}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)

	_, after := ctxt.Build(
		&Branch{
			3: &Branch{
				0xa: &Account{common.Address{0x3a}, info},
				0xb: &Account{common.Address{0x3b}, info},
			},
			8: &Account{common.Address{0x81}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// Expect a number of new nodes to be created.
	siblingId, sibling := ctxt.Build(&Account{info: info})
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createAccount().Return(siblingId, sibling, nil)
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)

	// Request the creation of a new account leaf in an existing branch.
	addr := common.Address{0x3b}
	path := addressToNibbles(&addr)
	if newRoot, res, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != id || res != sibling || err != nil {
		t.Fatalf("lookup should return (%v, %p), got (%v, %p), err %v", id, sibling, newRoot, res, err)
	}

	ctxt.ExpectEqual(t, node, after)
}

func TestBranchNode_GetOrCreateAccount_InNewBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Branch{
			3: &Account{common.Address{0x3a}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)

	_, after := ctxt.Build(
		&Branch{
			3: &Account{common.Address{0x3a}, info},
			5: &Account{common.Address{0x51}, info},
			8: &Account{common.Address{0x81}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, only a single account node should be created.
	siblingId, sibling := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(siblingId, sibling, nil)

	// Request the creation of a new account leaf in an existing branch.
	addr := common.Address{0x51}
	path := addressToNibbles(&addr)
	if newRoot, res, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != id || res != sibling || err != nil {
		t.Fatalf("lookup should return (%v, %p), got (%v, %p), err %v", id, sibling, newRoot, res, err)
	}

	ctxt.ExpectEqual(t, node, after)
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
				5: &Tag{"A", &Account{common.Address{0x12, 0x35}, info}},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)
	ctxt.Check(t, node)

	// Case 1: try to locate a non-existing address
	trg := common.Address{}
	path := addressToNibbles(&trg)
	if leaf, err := node.GetAccount(ctxt, &trg, path[:]); leaf != nil || err != nil {
		t.Fatalf("lookup should return nil pointer, got %v, err %v", leaf, err)
	}

	// Case 2: locate an existing address
	leaf := ctxt.Get("A").(*AccountNode)
	trg = leaf.address
	path = addressToNibbles(&trg)
	if res, err := node.GetAccount(ctxt, &trg, path[:]); res != leaf || err != nil {
		t.Fatalf("lookup should return %p, got %v, err %v", leaf, res, err)
	}

	// Case 3: locate an address with a partial extension path overlap only
	trg = common.Address{1, 2, 4, 8}
	path = addressToNibbles(&trg)
	if leaf, err := node.GetAccount(ctxt, &trg, path[:]); leaf != nil || err != nil {
		t.Fatalf("lookup should return nil pointer, got %v, err %v", leaf, err)
	}
}

func TestExtensionNode_GetOrAddAccount_ExistingLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Extension{
			[]Nibble{1, 2, 3},
			&Branch{
				5: &Tag{"A", &Account{common.Address{0x12, 0x35}, info}},
				8: &Account{common.Address{0x12, 0x38}, info},
			},
		},
	)
	ctxt.Check(t, node)

	// Attempt to create an existing account.
	leaf := ctxt.Get("A").(*AccountNode)
	trg := leaf.address
	path := addressToNibbles(&trg)
	if newRoot, res, err := node.GetOrCreateAccount(ctxt, id, &trg, path[:]); newRoot != id || res != leaf || err != nil {
		t.Fatalf("lookup should return (%v,%v), got (%v,%v), err %v", id, leaf, newRoot, res, err)
	}

	// Make sure the tree fragment was not corrupted.
	ctxt.Check(t, node)
}

func TestExtensionNode_GetOrAddAccount_PartialExtensionCovered(t *testing.T) {
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
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x40}
	path := addressToNibbles(&addr)
	if newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != extensionId || leaf != account || err != nil {
		t.Fatalf("lookup should return (%v,%v), got (%v,%v), err %v", extensionId, account, newRoot, leaf, err)
	}
	node, _ = ctxt.getNode(extensionId)

	ctxt.ExpectEqual(t, node, after)
}

func TestExtensionNode_GetOrAddAccount_NoCommonPrefix(t *testing.T) {
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
			8: &Account{common.Address{0x82}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch and an account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x82}
	path := addressToNibbles(&addr)
	if newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != branchId || leaf != account || err != nil {
		t.Fatalf("lookup should return (%v,%v), got (%v,%v), err %v", branchId, account, newRoot, leaf, err)
	}
	node, _ = ctxt.getNode(branchId)

	ctxt.ExpectEqual(t, node, after)
}

func TestExtensionNode_GetOrAddAccount_NoRemainingSuffix(t *testing.T) {
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
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x12, 0x38}
	path := addressToNibbles(&addr)
	if newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != id || leaf != account || err != nil {
		t.Fatalf("lookup should return (%v,%v), got (%v,%v), err %v", id, account, newRoot, leaf, err)
	}

	ctxt.ExpectEqual(t, node, after)
}

func TestExtensionNode_GetOrAddAccount_ExtensionIsEliminated(t *testing.T) {
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
			8: &Account{common.Address{0x83}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, a new branch and account is created and the extension is removed.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	ctxt.EXPECT().release(id)

	// Attempt to create a new account that is partially covered by the extension.
	addr := common.Address{0x83}
	path := addressToNibbles(&addr)
	if newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:]); newRoot != branchId || leaf != account || err != nil {
		t.Fatalf("lookup should return (%v,%v), got (%v,%v), err %v", branchId, account, newRoot, leaf, err)
	}

	result, _ := ctxt.getNode(branchId)
	ctxt.ExpectEqual(t, result, after)
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

func TestAccountNode_GetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)

	addr := common.Address{1}
	node := &AccountNode{}

	// Case 1: the node does not contain the requested value.
	path := addressToNibbles(&addr)
	if leaf, err := node.GetAccount(mgr, &addr, path[:]); leaf != nil || err != nil {
		t.Fatalf("lookup should return nil pointer, got %v, err %v", leaf, err)
	}

	// Case 2: the node contains the value with the full path in the node.
	node.address = addr
	if leaf, err := node.GetAccount(mgr, &addr, path[:]); leaf != node || err != nil {
		t.Fatalf("lookup should return %p, got %v, err %v", node, leaf, err)
	}
}

func TestAccountNode_GetOrCreateAccount_WithExistingAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mgr := NewMockNodeManager(ctrl)

	addr := common.Address{1}
	path := addressToNibbles(&addr)

	node := &AccountNode{}
	node.address = addr

	nodeId := AccountId(123)
	newRoot, leaf, err := node.GetOrCreateAccount(mgr, nodeId, &addr, path[:])
	if err != nil {
		t.Fatalf("failed to fetch account: %v", err)
	}
	if newRoot != nodeId || leaf != node {
		t.Errorf("produced wrong results, wanted (%v,%v), got (%v,%v)", nodeId, node, newRoot, leaf)
	}
}

func TestAccountNode_GetOrCreateAccount_WithMissingAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Account{common.Address{0x00, 0x02}, info},
	)

	_, after := ctxt.Build(
		&Extension{
			[]Nibble{0, 0, 0},
			&Branch{
				2: &Account{common.Address{0x00, 0x02}, info},
				4: &Account{common.Address{0x00, 0x04}, info},
			},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch, extension and account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)
	extensionId, extension := ctxt.Build(&Extension{})
	ctxt.EXPECT().createExtension().Return(extensionId, extension, nil)

	// Run the creation of the new node.
	addr := common.Address{0x00, 0x04}
	path := addressToNibbles(&addr)
	newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:])
	if err != nil {
		t.Fatalf("failed to add new account: %v", err)
	}
	if newRoot != extensionId || leaf != account {
		t.Errorf("produced wrong results, wanted (%v,%v), got (%v,%v)", extensionId, account, newRoot, leaf)
	}

	result, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, result, after)
}

func TestAccountNode_GetOrCreateAccount_WithMissingAccountAndNoCommonPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(ctrl)
	info := AccountInfo{Nonce: common.Nonce{1}}

	id, node := ctxt.Build(
		&Account{common.Address{0x22}, info},
	)

	_, after := ctxt.Build(
		&Branch{
			2: &Account{common.Address{0x22}, info},
			4: &Account{common.Address{0x41}, info},
		},
	)

	ctxt.Check(t, node)
	ctxt.Check(t, after)

	// In this case, one new branch and account is created.
	accountId, account := ctxt.Build(&Account{info: info})
	ctxt.EXPECT().createAccount().Return(accountId, account, nil)
	branchId, branch := ctxt.Build(&Branch{})
	ctxt.EXPECT().createBranch().Return(branchId, branch, nil)

	// Run the creation of the new node.
	addr := common.Address{0x41}
	path := addressToNibbles(&addr)
	newRoot, leaf, err := node.GetOrCreateAccount(ctxt, id, &addr, path[:])
	if err != nil {
		t.Fatalf("failed to add new account: %v", err)
	}
	if newRoot != branchId || leaf != account {
		t.Errorf("produced wrong results, wanted (%v,%v), got (%v,%v)", branchId, account, newRoot, leaf)
	}

	result, _ := ctxt.getNode(newRoot)
	ctxt.ExpectEqual(t, result, after)
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

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
		account: a.info,
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

func (c *nodeContext) Get(label string) Node {
	e, exists := c.tags[label]
	if !exists {
		panic("requested non-existing element")
	}
	return e.node
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

func (c *nodeContext) ExpectEqual(t *testing.T, a, b Node) {
	if !c.equal(a, b) {
		fmt.Printf("Want:\n")
		b.Dump(c, "")
		fmt.Printf("Have:\n")
		a.Dump(c, "")
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
			return a.address == b.address && a.account == b.account && c.equalTries(a.state, b.state)
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
