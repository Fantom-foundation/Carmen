package s4

//go:generate mockgen -source nodes.go -destination nodes_mocks.go -package s4

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type Node interface {
	GetAccount(source NodeSource, address *common.Address, path []Nibble) (*AccountNode, error)
	GetValue(source NodeSource, key *common.Key, path []Nibble) (*ValueNode, error)

	GetOrCreateAccount(manager NodeManager, current NodeId, address *common.Address, path []Nibble) (newRoot NodeId, leaf *AccountNode, err error)
	/*
		GetOrCreateValue(manager NodeManager, path []Nibble) (*ValueNode, error)

		DeleteAccount(manager NodeManager, path []Nibble) error
		DeleteValue(manager NodeManager, path []Nibble) error
	*/
	Check(source NodeSource, path []Nibble) error
	Dump(source NodeSource, indent string)
}

type NodeSource interface {
	getNode(NodeId) (Node, error)
}

type NodeManager interface {
	NodeSource

	createAccount() (NodeId, *AccountNode, error)
	createBranch() (NodeId, *BranchNode, error)
	createExtension() (NodeId, *ExtensionNode, error)
	createValue() (NodeId, *ValueNode, error)

	release(NodeId) error
}

// ----------------------------------------------------------------------------
//                               Empty Node
// ----------------------------------------------------------------------------

type EmptyNode struct{}

func (EmptyNode) GetAccount(NodeSource, *common.Address, []Nibble) (*AccountNode, error) {
	return nil, nil
}

func (EmptyNode) GetValue(NodeSource, *common.Key, []Nibble) (*ValueNode, error) {
	return nil, nil
}

func (EmptyNode) GetOrCreateAccount(manager NodeManager, current NodeId, address *common.Address, path []Nibble) (newRoot NodeId, leaf *AccountNode, err error) {
	id, res, err := manager.createAccount()
	if err != nil {
		return 0, nil, err
	}
	res.address = *address
	return id, res, nil
}

func (EmptyNode) Check(NodeSource, []Nibble) error {
	// No invariants to be checked.
	return nil
}

func (EmptyNode) Dump(source NodeSource, indent string) {
	fmt.Printf("%s-empty-\n", indent)
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

type BranchNode struct {
	children [16]NodeId
}

func (n *BranchNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (*AccountNode, error) {
	next := n.children[path[0]]
	if next.IsEmpty() {
		return nil, nil
	}
	node, err := source.getNode(next)
	if err != nil {
		return nil, err
	}
	return node.GetAccount(source, address, path[1:])
}

func (n *BranchNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (*ValueNode, error) {
	next := n.children[path[0]]
	if next.IsEmpty() {
		return nil, nil
	}
	node, err := source.getNode(next)
	if err != nil {
		return nil, err
	}
	return node.GetValue(source, key, path[1:])
}

func (n *BranchNode) GetOrCreateAccount(manager NodeManager, current NodeId, address *common.Address, path []Nibble) (NodeId, *AccountNode, error) {
	next := n.children[path[0]]
	// If there is a child covering the next step, forward the call.
	if !next.IsEmpty() {
		node, err := manager.getNode(next)
		if err != nil {
			return 0, nil, err
		}
		root, res, err := node.GetOrCreateAccount(manager, next, address, path[1:])
		n.children[path[0]] = root
		return current, res, err
	}

	// Create a new account node and insert it into the trie.
	id, res, err := manager.createAccount()
	if err != nil {
		return 0, nil, err
	}
	n.children[path[0]] = id
	res.address = *address
	return current, res, nil
}

func (n *BranchNode) Check(source NodeSource, path []Nibble) error {
	// Checked invariants:
	//  - must have 2+ children
	//  - child trees must be error free
	numChildren := 0
	errs := []error{}
	for i, child := range n.children {
		if child.IsEmpty() {
			continue
		}
		numChildren++

		if node, err := source.getNode(child); err == nil {
			if err := node.Check(source, append(path, Nibble(i))); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("unable to resolve node %v: %v", child, err))
		}
	}
	if numChildren < 2 {
		errs = append(errs, fmt.Errorf("insufficient child nodes: %d", numChildren))
	}
	return errors.Join(errs...)
}

func (n *BranchNode) Dump(source NodeSource, indent string) {
	fmt.Printf("%sBranch:\n", indent)
	for i, child := range n.children {
		if child.IsEmpty() {
			continue
		}

		if node, err := source.getNode(child); err == nil {
			node.Dump(source, fmt.Sprintf("%s  %v ", indent, Nibble(i)))
		} else {
			fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, child, err)
		}
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

type ExtensionNode struct {
	path Path
	next NodeId
}

func (n *ExtensionNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (*AccountNode, error) {
	if !n.path.IsPrefixOf(path) {
		return nil, nil
	}
	node, err := source.getNode(n.next)
	if err != nil {
		return nil, err
	}
	return node.GetAccount(source, address, path[n.path.Length():])
}

func (n *ExtensionNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (*ValueNode, error) {
	if !n.path.IsPrefixOf(path) {
		return nil, nil
	}
	node, err := source.getNode(n.next)
	if err != nil {
		return nil, err
	}
	return node.GetValue(source, key, path[n.path.Length():])
}

func (n *ExtensionNode) GetOrCreateAccount(manager NodeManager, current NodeId, address *common.Address, path []Nibble) (NodeId, *AccountNode, error) {
	// Check whether the extension can be preserved.
	if n.path.IsPrefixOf(path) {
		next, err := manager.getNode(n.next)
		if err != nil {
			return 0, nil, nil
		}
		_, res, err := next.GetOrCreateAccount(manager, n.next, address, path[n.path.Length():])
		return current, res, err
	}

	// Extension needs to be replaced by a combination of
	//  - an optional common prefix extension
	//  - a branch node
	//  - an optional extension connecting to the previous next node

	// Create the branch node that will be needed in any case.
	branchId, branch, err := manager.createBranch()
	if err != nil {
		return 0, nil, err
	}
	newRoot := branchId

	// Determine the point at which the prefix need to be split.
	commonPrefixLength := n.path.GetCommonPrefixLength(path)

	// Build the extension connecting the branch to the next node.
	thisNodeWasReused := false
	if commonPrefixLength < n.path.Length()-1 {
		// We re-use the current node for this - all we need is to update the path.
		branch.children[n.path.Get(commonPrefixLength)] = current
		n.path.ShiftLeft(commonPrefixLength + 1)
		thisNodeWasReused = true
	} else {
		branch.children[n.path.Get(commonPrefixLength)] = n.next
	}

	// Build the extension covering the common prefix.
	if commonPrefixLength > 0 {
		// Reuse current node unless already taken.
		extension := n
		extensionId := current
		if thisNodeWasReused {
			extensionId, extension, err = manager.createExtension()
			if err != nil {
				return 0, nil, err
			}
		} else {
			thisNodeWasReused = true
		}

		extension.path = CreatePathFromNibbles(path[0:commonPrefixLength])
		extension.next = branchId
		newRoot = extensionId
	}

	// If this node was not needed any more, we can discard it.
	if !thisNodeWasReused {
		manager.release(current)
	}

	// Continue insertion of new account at new branch level.
	_, leaf, err := branch.GetOrCreateAccount(manager, branchId, address, path[commonPrefixLength:])
	if err != nil {
		return 0, nil, err
	}
	return newRoot, leaf, nil
}

func (n *ExtensionNode) Check(source NodeSource, path []Nibble) error {
	// Checked invariants:
	//  - extension path have a lenght > 0
	//  - extension can only be followed by a branch
	//  - sub-trie is correct
	errs := []error{}
	if n.path.Length() <= 0 {
		errs = append(errs, fmt.Errorf("extension path must not be empty"))
	}
	if !n.next.IsBranch() {
		errs = append(errs, fmt.Errorf("extension path must be followed by a branch"))
	}
	if node, err := source.getNode(n.next); err == nil {
		extended := path
		for i := 0; i < n.path.Length(); i++ {
			extended = append(extended, n.path.Get(i))
		}
		if err := node.Check(source, extended); err != nil {
			errs = append(errs, err)
		}
	} else {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (n *ExtensionNode) Dump(source NodeSource, indent string) {
	fmt.Printf("%sExtension: %v\n", indent, &n.path)
	if node, err := source.getNode(n.next); err == nil {
		node.Dump(source, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.next, err)
	}
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

type AccountNode struct {
	address common.Address
	account AccountInfo
	state   NodeId
}

func (n *AccountNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (*AccountNode, error) {
	if n.address == *address {
		return n, nil
	}
	return nil, nil
}

func (n *AccountNode) GetValue(NodeSource, *common.Key, []Nibble) (*ValueNode, error) {
	return nil, fmt.Errorf("invalid request: value query should not reach accounts")
}

func (n *AccountNode) GetOrCreateAccount(manager NodeManager, current NodeId, address *common.Address, path []Nibble) (NodeId, *AccountNode, error) {
	// Check whether this is the correct account.
	if n.address == *address {
		return current, n, nil
	}

	// This single node needs to be split into
	//  - an optional common prefix extension
	//  - a branch node linking this node and
	//  - a new sibling account node to be returned

	branchId, branch, err := manager.createBranch()
	if err != nil {
		return 0, nil, err
	}
	newRoot := branchId

	siblingId, sibling, err := manager.createAccount()
	if err != nil {
		return 0, nil, err
	}
	sibling.address = *address

	// Check whether there is a common prefix.
	fullPath := addressToNibbles(&n.address)
	partialPath := fullPath[len(fullPath)-len(path):]
	commonPrefixLength := getCommonPrefixLength(partialPath, path)
	if commonPrefixLength > 0 {
		extensionId, extension, err := manager.createExtension()
		if err != nil {
			return 0, nil, err
		}
		newRoot = extensionId

		extension.path = CreatePathFromNibbles(path[0:commonPrefixLength])
		extension.next = branchId
	}

	// Add this node and the new sibling node to the branch node.
	branch.children[partialPath[commonPrefixLength]] = current
	branch.children[path[commonPrefixLength]] = siblingId

	return newRoot, sibling, nil
}

func (n *AccountNode) Check(source NodeSource, path []Nibble) error {
	// Checked invariants:
	//  - account information must not be empty
	//  - the account is at a correct position in the trie
	//  - state sub-trie is correct
	errs := []error{}

	fullPath := addressToNibbles(&n.address)
	if !isPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("account node %v located in wrong branch: %v", n.address, path))
	}

	if n.account.IsEmpty() {
		errs = append(errs, fmt.Errorf("account information must not be empty"))
	}

	if !n.state.IsEmpty() {
		if node, err := source.getNode(n.state); err == nil {
			if err := node.Check(source, make([]Nibble, 0, common.KeySize*2)); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (n *AccountNode) Dump(source NodeSource, indent string) {
	fmt.Printf("%sAccount: %v - %v\n", indent, n.address, n.account)
	if n.state.IsEmpty() {
		return
	}
	if node, err := source.getNode(n.state); err == nil {
		node.Dump(source, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.state, err)
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

type ValueNode struct {
	key   common.Key
	value common.Value
}

func (n *ValueNode) GetAccount(NodeSource, *common.Address, []Nibble) (*AccountNode, error) {
	return nil, fmt.Errorf("invalid request: account query should not reach values")
}

func (n *ValueNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (*ValueNode, error) {
	if n.key == *key {
		return n, nil
	}
	return nil, nil
}

func (n *ValueNode) GetOrCreateAccount(NodeManager, NodeId, *common.Address, []Nibble) (NodeId, *AccountNode, error) {
	return 0, nil, fmt.Errorf("invalid request: account query should not reach values")
}

func (n *ValueNode) Check(source NodeSource, path []Nibble) error {
	// Checked invariants:
	//  - value must not be empty
	//  - values are in the right position of the trie
	errs := []error{}

	fullPath := keyToNibbles(&n.key)
	if !isPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("value node %v located in wrong branch: %v", n.key, path))
	}

	if n.value == (common.Value{}) {
		errs = append(errs, fmt.Errorf("value slot must not be empty"))
	}

	return errors.Join(errs...)
}

func (n *ValueNode) Dump(source NodeSource, indent string) {
	fmt.Printf("%sValue: %v - %v\n", indent, n.key, n.value)
}

// ----------------------------------------------------------------------------
//                               Node Encoders
// ----------------------------------------------------------------------------

type BranchNodeEncoder struct{}

func (BranchNodeEncoder) GetEncodedSize() int {
	encoder := NodeIdEncoder{}
	return encoder.GetEncodedSize() * 16
}

func (BranchNodeEncoder) Store(dst []byte, node *BranchNode) error {
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		if err := encoder.Store(dst[i*step:], &node.children[i]); err != nil {
			return err
		}
	}
	return nil
}

func (BranchNodeEncoder) Load(src []byte, node *BranchNode) error {
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	res := BranchNode{}
	for i := 0; i < 16; i++ {
		if err := encoder.Load(src[i*step:], &res.children[i]); err != nil {
			return err
		}
	}
	return nil
}

type ExtensionNodeEncoder struct{}

func (ExtensionNodeEncoder) GetEncodedSize() int {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	return pathEncoder.GetEncodedSize() + idEncoder.GetEncodedSize()
}

func (ExtensionNodeEncoder) Store(dst []byte, value *ExtensionNode) error {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Store(dst, &value.path); err != nil {
		return err
	}
	return idEncoder.Store(dst[pathEncoder.GetEncodedSize():], &value.next)
}

func (ExtensionNodeEncoder) Load(src []byte, node *ExtensionNode) error {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Load(src, &node.path); err != nil {
		return err
	}
	if err := idEncoder.Load(src[pathEncoder.GetEncodedSize():], &node.next); err != nil {
		return err
	}
	return nil
}

type AccountNodeEncoder struct{}

func (AccountNodeEncoder) GetEncodedSize() int {
	return common.AddressSize +
		AccountInfoEncoder{}.GetEncodedSize() +
		NodeIdEncoder{}.GetEncodedSize()
}

func (AccountNodeEncoder) Store(dst []byte, node *AccountNode) error {
	copy(dst, node.address[:])
	dst = dst[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Store(dst, &node.account); err != nil {
		return err
	}
	dst = dst[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	return idEncoder.Store(dst, &node.state)
}

func (AccountNodeEncoder) Load(src []byte, node *AccountNode) error {
	copy(node.address[:], src)
	src = src[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Load(src, &node.account); err != nil {
		return err
	}
	src = src[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	if err := idEncoder.Load(src, &node.state); err != nil {
		return err
	}

	return nil
}

type ValueNodeEncoder struct{}

func (ValueNodeEncoder) GetEncodedSize() int {
	return PathEncoder{}.GetEncodedSize() + common.ValueSize
}

func (ValueNodeEncoder) Store(dst []byte, node *ValueNode) error {
	copy(dst, node.key[:])
	dst = dst[len(node.key):]
	copy(dst, node.value[:])
	return nil
}

func (ValueNodeEncoder) Load(src []byte, node *ValueNode) error {
	copy(node.key[:], src)
	src = src[len(node.key):]
	copy(node.value[:], src)
	return nil
}
