package s4

//go:generate mockgen -source nodes.go -destination nodes_mocks.go -package s4

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type Node interface {
	GetAccount(source NodeSource, address *common.Address, path []Nibble) (AccountInfo, bool, error)
	SetAccount(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, info *AccountInfo) (newRoot NodeId, changed bool, err error)

	GetValue(source NodeSource, key *common.Key, path []Nibble) (common.Value, bool, error)
	SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (newRoot NodeId, changed bool, err error)

	GetSlot(source NodeSource, address *common.Address, path []Nibble, key *common.Key) (common.Value, bool, error)
	SetSlot(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, key *common.Key, value *common.Value) (newRoot NodeId, changed bool, err error)
	ClearStorage(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble) (newRoot NodeId, changed bool, err error)

	Release(manager NodeManager, thisId NodeId) error
	Freeze(source NodeSource) error

	Check(source NodeSource, path []Nibble) error
	Dump(source NodeSource, thisId NodeId, indent string)
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

	update(NodeId, Node) error
	invalidateHash(NodeId)

	release(NodeId) error
}

// ----------------------------------------------------------------------------
//                               Empty Node
// ----------------------------------------------------------------------------

type EmptyNode struct{}

func (EmptyNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (AccountInfo, bool, error) {
	return AccountInfo{}, false, nil
}

func (EmptyNode) GetValue(NodeSource, *common.Key, []Nibble) (common.Value, bool, error) {
	return common.Value{}, false, nil
}

func (EmptyNode) GetSlot(NodeSource, *common.Address, []Nibble, *common.Key) (common.Value, bool, error) {
	return common.Value{}, false, nil
}

func (e EmptyNode) SetAccount(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, info *AccountInfo) (NodeId, bool, error) {
	if info.IsEmpty() {
		return thisId, false, nil
	}
	id, res, err := manager.createAccount()
	if err != nil {
		return 0, false, err
	}
	res.address = *address
	res.info = *info
	if err := manager.update(id, res); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (e EmptyNode) SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (NodeId, bool, error) {
	if *value == (common.Value{}) {
		return thisId, false, nil
	}
	id, res, err := manager.createValue()
	if err != nil {
		return 0, false, err
	}
	res.key = *key
	res.value = *value
	if err := manager.update(id, res); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (e EmptyNode) SetSlot(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, key *common.Key, value *common.Value) (NodeId, bool, error) {
	// We can stop here, since the account does not exist and it should not
	// be implicitly created by setting a value.
	// Note: this function can only be reached while looking for the account.
	// Once the account is reached, the SetValue(..) function is used.
	return thisId, false, nil
}

func (e EmptyNode) ClearStorage(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	return thisId, false, nil
}

func (e EmptyNode) Release(NodeManager, NodeId) error {
	return nil
}

func (e EmptyNode) Freeze(NodeSource) error {
	// empty nodes are always frozen
	return nil
}

func (EmptyNode) Check(NodeSource, []Nibble) error {
	// No invariants to be checked.
	return nil
}

func (EmptyNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%s-empty- (ID: %v)\n", indent, thisId)
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

type BranchNode struct {
	children [16]NodeId
	frozen   bool
}

func getNextNodeInBranch[V any](
	n *BranchNode,
	source NodeSource,
	path []Nibble,
	consume func(Node, []Nibble) (V, bool, error),
) (V, bool, error) {
	next := n.children[path[0]]
	node, err := source.getNode(next)
	if err != nil {
		var val V
		return val, false, err
	}
	return consume(node, path[1:])
}

func (n *BranchNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (AccountInfo, bool, error) {
	return getNextNodeInBranch[AccountInfo](n, source, path,
		func(node Node, path []Nibble) (AccountInfo, bool, error) {
			return node.GetAccount(source, address, path)
		},
	)
}

func (n *BranchNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (common.Value, bool, error) {
	return getNextNodeInBranch[common.Value](n, source, path,
		func(node Node, path []Nibble) (common.Value, bool, error) {
			return node.GetValue(source, key, path)
		},
	)
}

func (n *BranchNode) GetSlot(source NodeSource, address *common.Address, path []Nibble, key *common.Key) (common.Value, bool, error) {
	return getNextNodeInBranch[common.Value](n, source, path,
		func(node Node, path []Nibble) (common.Value, bool, error) {
			return node.GetSlot(source, address, path, key)
		},
	)
}

func (n *BranchNode) setNextNode(
	manager NodeManager,
	thisId NodeId,
	path []Nibble,
	createSubTree func(NodeId, Node, []Nibble) (NodeId, bool, error),
) (NodeId, bool, error) {
	// Forward call to child node.
	next := n.children[path[0]]
	node, err := manager.getNode(next)
	if err != nil {
		return 0, false, err
	}
	newRoot, changed, err := createSubTree(next, node, path[1:])
	if err != nil {
		return 0, false, err
	}

	if newRoot == next {
		if changed {
			manager.invalidateHash(thisId)
		}
		return thisId, changed, nil
	}

	// If frozen, clone the current node and modify copy.
	isClone := false
	if n.frozen {
		newId, newNode, err := manager.createBranch()
		if err != nil {
			return 0, false, err
		}
		newNode.children = n.children
		n = newNode
		thisId = newId
		isClone = true
	}

	n.children[path[0]] = newRoot

	// If a branch got removed, check that there are enough children left.
	if !next.IsEmpty() && newRoot.IsEmpty() {
		count := 0
		var remainingPos Nibble
		var remaining NodeId
		for i, cur := range n.children {
			if !cur.IsEmpty() {
				count++
				if count > 1 {
					break
				}
				remainingPos = Nibble(i)
				remaining = cur
			}
		}
		if count < 2 {
			newRoot := remaining
			// This branch became obsolete and needs to be removed.
			if remaining.IsExtension() {
				// The present extension can be extended.
				extension, err := manager.getNode(remaining)
				if err != nil {
					return 0, false, err
				}
				extensionNode := extension.(*ExtensionNode)
				extensionNode.path.Prepend(remainingPos)
				manager.update(remaining, extension)
			} else if remaining.IsBranch() {
				// An extension needs to replace this branch.
				extensionId, extension, err := manager.createExtension()
				if err != nil {
					return 0, false, err
				}
				extension.path = SingleStepPath(remainingPos)
				extension.next = remaining
				manager.update(extensionId, extension)
				newRoot = extensionId
			}
			manager.release(thisId)
			return newRoot, !isClone, nil
		}
	}

	manager.update(thisId, n)
	return thisId, !isClone, err
}

func (n *BranchNode) SetAccount(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, info *AccountInfo) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetAccount(manager, next, address, path, info)
		},
	)
}

func (n *BranchNode) SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetValue(manager, next, key, path, value)
		},
	)
}

func (n *BranchNode) SetSlot(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, key *common.Key, value *common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetSlot(manager, next, address, path, key, value)
		},
	)
}

func (n *BranchNode) ClearStorage(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.ClearStorage(manager, next, address, path)
		},
	)
}

func (n *BranchNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	for _, cur := range n.children {
		if !cur.IsEmpty() {
			node, err := manager.getNode(cur)
			if err != nil {
				return err
			}
			err = node.Release(manager, cur)
			if err != nil {
				return err
			}
		}
	}
	return manager.release(thisId)
}

func (n *BranchNode) Freeze(source NodeSource) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	for _, cur := range n.children {
		if cur.IsEmpty() {
			continue
		}
		node, err := source.getNode(cur)
		if err != nil {
			return err
		}
		if err := node.Freeze(source); err != nil {
			return err
		}
	}
	return nil
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

func (n *BranchNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sBranch (ID: %v/%t):\n", indent, thisId, n.frozen)
	for i, child := range n.children {
		if child.IsEmpty() {
			continue
		}

		if node, err := source.getNode(child); err == nil {
			node.Dump(source, child, fmt.Sprintf("%s  %v ", indent, Nibble(i)))
		} else {
			fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, child, err)
		}
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

type ExtensionNode struct {
	path   Path
	next   NodeId
	frozen bool
}

func getNextNodeInExtension[V any](
	n *ExtensionNode,
	source NodeSource,
	path []Nibble,
	consume func(Node, []Nibble) (V, bool, error),
) (V, bool, error) {
	if !n.path.IsPrefixOf(path) {
		return *new(V), false, nil
	}
	node, err := source.getNode(n.next)
	if err != nil {
		var val V
		return val, false, err
	}
	return consume(node, path[n.path.Length():])
}

func (n *ExtensionNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (AccountInfo, bool, error) {
	return getNextNodeInExtension[AccountInfo](n, source, path,
		func(node Node, path []Nibble) (AccountInfo, bool, error) {
			return node.GetAccount(source, address, path)
		},
	)
}

func (n *ExtensionNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (common.Value, bool, error) {
	return getNextNodeInExtension[common.Value](n, source, path,
		func(node Node, path []Nibble) (common.Value, bool, error) {
			return node.GetValue(source, key, path)
		},
	)
}

func (n *ExtensionNode) GetSlot(source NodeSource, address *common.Address, path []Nibble, key *common.Key) (common.Value, bool, error) {
	return getNextNodeInExtension[common.Value](n, source, path,
		func(node Node, path []Nibble) (common.Value, bool, error) {
			return node.GetSlot(source, address, path, key)
		},
	)
}

func (n *ExtensionNode) setNextNode(
	manager NodeManager,
	thisId NodeId,
	path []Nibble,
	valueIsEmpty bool,
	createSubTree func(NodeId, Node, []Nibble) (NodeId, bool, error),
) (NodeId, bool, error) {
	// Check whether the updates targest the node referenced by this extension.
	if n.path.IsPrefixOf(path) {
		next, err := manager.getNode(n.next)
		if err != nil {
			return 0, false, err
		}
		newRoot, changed, err := createSubTree(n.next, next, path[n.path.Length():])
		if err != nil {
			return 0, false, err
		}

		if newRoot.IsEmpty() {
			if n.frozen {
				return EmptyId(), false, nil
			}
			manager.release(thisId)
			return newRoot, true, nil
		}

		if newRoot != n.next {

			// If frozen, modify a clone.
			isClone := false
			if n.frozen {
				newId, newNode, err := manager.createExtension()
				if err != nil {
					return 0, false, err
				}
				newNode.path = n.path
				newNode.next = n.next
				thisId, n = newId, newNode
				isClone = true
			}

			if newRoot.IsExtension() {
				// If the new next is an extension, merge it into this extension.
				node, err := manager.getNode(newRoot)
				if err != nil {
					return 0, false, err
				}
				extension := node.(*ExtensionNode)
				n.path.AppendAll(&extension.path)
				n.next = extension.next
				manager.update(thisId, n)
				manager.release(newRoot)
			} else if newRoot.IsBranch() {
				n.next = newRoot
				manager.update(thisId, n)
			} else {
				// If the next node is anything but a branch or extension, remove this extension.
				manager.release(thisId)
				return newRoot, !isClone, nil
			}
		} else if changed {
			manager.invalidateHash(thisId)
		}
		return thisId, changed, err
	}

	// Skip creation of a new sub-tree if the info is empty.
	if valueIsEmpty {
		return thisId, false, nil
	}

	// If frozen, modify a clone.
	isClone := false
	if n.frozen {
		newId, newNode, err := manager.createExtension()
		if err != nil {
			return 0, false, err
		}
		newNode.path = n.path
		newNode.next = n.next
		thisId, n = newId, newNode
		isClone = true
	}

	// Extension needs to be replaced by a combination of
	//  - an optional common prefix extension
	//  - a branch node
	//  - an optional extension connecting to the previous next node

	// Create the branch node that will be needed in any case.
	branchId, branch, err := manager.createBranch()
	if err != nil {
		return 0, false, err
	}
	newRoot := branchId

	// Determine the point at which the prefix need to be split.
	commonPrefixLength := n.path.GetCommonPrefixLength(path)

	// Build the extension connecting the branch to the next node.
	thisNodeWasReused := false
	if commonPrefixLength < n.path.Length()-1 {
		// We re-use the current node for this - all we need is to update the path.
		branch.children[n.path.Get(commonPrefixLength)] = thisId
		n.path.ShiftLeft(commonPrefixLength + 1)
		thisNodeWasReused = true
		manager.update(thisId, n)
	} else {
		branch.children[n.path.Get(commonPrefixLength)] = n.next
	}

	// Build the extension covering the common prefix.
	if commonPrefixLength > 0 {
		// Reuse current node unless already taken.
		extension := n
		extensionId := thisId
		if thisNodeWasReused {
			extensionId, extension, err = manager.createExtension()
			if err != nil {
				return 0, false, err
			}
		} else {
			thisNodeWasReused = true
		}

		extension.path = CreatePathFromNibbles(path[0:commonPrefixLength])
		extension.next = branchId
		manager.update(extensionId, extension)
		newRoot = extensionId
	}

	// If this node was not needed any more, we can discard it.
	if !thisNodeWasReused {
		manager.release(thisId)
	}

	// Continue insertion of new account at new branch level.
	_, _, err = createSubTree(branchId, branch, path[commonPrefixLength:])
	if err != nil {
		return 0, false, err
	}
	return newRoot, !isClone, nil
}

func (n *ExtensionNode) SetAccount(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, info *AccountInfo) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, info.IsEmpty(),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetAccount(manager, next, address, path, info)
		},
	)
}

func (n *ExtensionNode) SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, *value == (common.Value{}),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetValue(manager, next, key, path, value)
		},
	)
}

func (n *ExtensionNode) SetSlot(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, key *common.Key, value *common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, *value == (common.Value{}),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetSlot(manager, next, address, path, key, value)
		},
	)
}

func (n *ExtensionNode) ClearStorage(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	return n.setNextNode(manager, thisId, path, true,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.ClearStorage(manager, next, address, path)
		},
	)
}

func (n *ExtensionNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	node, err := manager.getNode(n.next)
	if err != nil {
		return err
	}
	err = node.Release(manager, n.next)
	if err != nil {
		return err
	}
	return manager.release(thisId)
}

func (n *ExtensionNode) Freeze(source NodeSource) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	node, err := source.getNode(n.next)
	if err != nil {
		return err
	}
	return node.Freeze(source)
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

func (n *ExtensionNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sExtension (ID: %v): %v\n", indent, thisId, &n.path)
	if node, err := source.getNode(n.next); err == nil {
		node.Dump(source, n.next, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.next, err)
	}
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

type AccountNode struct {
	address common.Address
	info    AccountInfo
	state   NodeId
	frozen  bool
}

func (n *AccountNode) GetAccount(source NodeSource, address *common.Address, path []Nibble) (AccountInfo, bool, error) {
	if n.address == *address {
		return n.info, true, nil
	}
	return AccountInfo{}, false, nil
}

func (n *AccountNode) GetValue(NodeSource, *common.Key, []Nibble) (common.Value, bool, error) {
	return common.Value{}, false, fmt.Errorf("invalid request: value query should not reach accounts")
}

func (n *AccountNode) GetSlot(source NodeSource, address *common.Address, path []Nibble, key *common.Key) (common.Value, bool, error) {
	if n.address != *address {
		return common.Value{}, false, nil
	}
	subPath := keyToNibbles(key)
	root, err := source.getNode(n.state)
	if err != nil {
		return common.Value{}, false, err
	}
	return root.GetValue(source, key, subPath[:])
}

func (n *AccountNode) SetAccount(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, info *AccountInfo) (NodeId, bool, error) {
	// Check whether this is the correct account.
	if n.address == *address {
		if *info == n.info {
			return thisId, false, nil
		}
		if info.IsEmpty() {
			// TODO: test this
			if n.frozen {
				return EmptyId(), true, nil
			}
			// Recursively release the entire state DB.
			// TODO: consider performing this asynchroniously.
			root, err := manager.getNode(n.state)
			if err != nil {
				return 0, false, err
			}
			err = root.Release(manager, n.state)
			if err != nil {
				return 0, false, err
			}
			// Release this account node and remove it from the trie.
			manager.release(thisId)
			return EmptyId(), true, nil
		}

		// If this node is frozen, we need to write the result in
		// a new account node.
		// TODO: add a unit test for this
		if n.frozen {
			newId, newNode, err := manager.createAccount()
			if err != nil {
				return 0, false, err
			}
			newNode.address = *address
			newNode.info = *info
			newNode.state = n.state
			return newId, false, nil
		}

		n.info = *info
		manager.update(thisId, n)
		return thisId, true, nil
	}

	// Skip restructuring the tree if the new info is empty.
	if info.IsEmpty() {
		return thisId, false, nil
	}

	// Create a new node for the sibling to be added.
	siblingId, sibling, err := manager.createAccount()
	if err != nil {
		return 0, false, err
	}
	sibling.address = *address
	sibling.info = *info

	thisPath := addressToNibbles(&n.address)
	return splitLeafNode(manager, thisId, thisPath[:], path, siblingId, sibling)
}

func splitLeafNode(
	manager NodeManager,
	thisId NodeId,
	thisPath []Nibble,
	siblingPath []Nibble,
	siblingId NodeId,
	sibling Node,
) (NodeId, bool, error) {
	// This single node needs to be split into
	//  - an optional common prefix extension
	//  - a branch node linking this node and
	//  - a new sibling account node to be returned

	branchId, branch, err := manager.createBranch()
	if err != nil {
		return 0, false, err
	}
	newRoot := branchId

	// Check whether there is a common prefix.
	partialPath := thisPath[len(thisPath)-len(siblingPath):]
	commonPrefixLength := getCommonPrefixLength(partialPath, siblingPath)
	if commonPrefixLength > 0 {
		extensionId, extension, err := manager.createExtension()
		if err != nil {
			return 0, false, err
		}
		newRoot = extensionId

		extension.path = CreatePathFromNibbles(siblingPath[0:commonPrefixLength])
		extension.next = branchId
		manager.update(extensionId, extension)
	}

	// Add this node and the new sibling node to the branch node.
	branch.children[partialPath[commonPrefixLength]] = thisId
	branch.children[siblingPath[commonPrefixLength]] = siblingId

	// Commit the changes to the sibling and the branch node.
	manager.update(siblingId, sibling)
	manager.update(branchId, branch)

	return newRoot, true, nil
}

func (n *AccountNode) SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("setValue call should not reach account nodes")
}

func (n *AccountNode) SetSlot(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble, key *common.Key, value *common.Value) (NodeId, bool, error) {
	// If this is not the correct account, the real account does not exist
	// and the insert can be skipped. The insertion of a slot value shall
	// not create an account.
	if n.address != *address {
		return thisId, false, nil
	}

	// Continue from here with a value insertion.
	node, err := manager.getNode(n.state)
	if err != nil {
		return 0, false, err
	}
	subPath := keyToNibbles(key)
	root, changed, err := node.SetValue(manager, n.state, key, subPath[:], value)
	if err != nil {
		return 0, false, err
	}
	if root != n.state {
		// If this node is frozen, we need to write the result in
		// a new account node.
		// TODO: add a unit test for this
		if n.frozen {
			newId, newNode, err := manager.createAccount()
			if err != nil {
				return 0, false, err
			}
			newNode.address = *address
			newNode.info = n.info
			newNode.state = root
			return newId, false, nil
		}
		n.state = root
		manager.update(thisId, n)
	} else if changed {
		manager.invalidateHash(thisId)
	}
	return thisId, changed, nil
}

func (n *AccountNode) ClearStorage(manager NodeManager, thisId NodeId, address *common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	if n.address != *address || n.state.IsEmpty() {
		return thisId, false, nil
	}

	// If this node is frozen, we need to write the result in
	// a new account node.
	// TODO: add a unit test for this
	if n.frozen {
		newId, newNode, err := manager.createAccount()
		if err != nil {
			return thisId, false, err
		}
		newNode.address = *address
		newNode.info = n.info
		newNode.state = EmptyId()
		return newId, false, nil
	}

	root, err := manager.getNode(n.state)
	if err != nil {
		return thisId, false, err
	}

	err = root.Release(manager, n.state)
	n.state = EmptyId()
	return thisId, true, err
}

func (n *AccountNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	if !n.state.IsEmpty() {
		if err := manager.release(n.state); err != nil {
			return err
		}
	}
	return manager.release(thisId)
}

func (n *AccountNode) Freeze(source NodeSource) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	node, err := source.getNode(n.state)
	if err != nil {
		return err
	}
	return node.Freeze(source)
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

	if n.info.IsEmpty() {
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

func (n *AccountNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sAccount (ID: %v/%t): %v - %v\n", indent, thisId, n.frozen, n.address, n.info)
	if n.state.IsEmpty() {
		return
	}
	if node, err := source.getNode(n.state); err == nil {
		node.Dump(source, n.state, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.state, err)
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

type ValueNode struct {
	key    common.Key
	value  common.Value
	frozen bool
}

func (n *ValueNode) GetAccount(NodeSource, *common.Address, []Nibble) (AccountInfo, bool, error) {
	return AccountInfo{}, false, fmt.Errorf("invalid request: account query should not reach values")
}

func (n *ValueNode) GetValue(source NodeSource, key *common.Key, path []Nibble) (common.Value, bool, error) {
	if n.key == *key {
		return n.value, true, nil
	}
	return common.Value{}, false, nil
}

func (n *ValueNode) GetSlot(NodeSource, *common.Address, []Nibble, *common.Key) (common.Value, bool, error) {
	return common.Value{}, false, fmt.Errorf("invalid request: slot query should not reach values")
}

func (n *ValueNode) SetAccount(NodeManager, NodeId, *common.Address, []Nibble, *AccountInfo) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: account update should not reach values")
}

func (n *ValueNode) SetValue(manager NodeManager, thisId NodeId, key *common.Key, path []Nibble, value *common.Value) (NodeId, bool, error) {
	// Check whether this is the correct account.
	if n.key == *key {
		if *value == n.value {
			return thisId, false, nil
		}
		if *value == (common.Value{}) {
			manager.release(thisId)
			return EmptyId(), true, nil
		}
		if n.frozen {
			newId, newNode, err := manager.createValue()
			if err != nil {
				return 0, false, nil
			}
			newNode.key = n.key
			newNode.value = *value
			return newId, false, nil
		}
		n.value = *value
		manager.update(thisId, n)
		return thisId, true, nil
	}

	// Skip restructuring the tree if the new info is empty.
	if *value == (common.Value{}) {
		return thisId, false, nil
	}

	// Create a new node for the sibling to be added.
	siblingId, sibling, err := manager.createValue()
	if err != nil {
		return 0, false, err
	}
	sibling.key = *key
	sibling.value = *value

	thisPath := keyToNibbles(&n.key)
	return splitLeafNode(manager, thisId, thisPath[:], path, siblingId, sibling)
}

func (n *ValueNode) SetSlot(NodeManager, NodeId, *common.Address, []Nibble, *common.Key, *common.Value) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: slot update should not reach values")
}

func (n *ValueNode) ClearStorage(NodeManager, NodeId, *common.Address, []Nibble) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: clear storage should not reach values")
}

func (n *ValueNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	return manager.release(thisId)
}

func (n *ValueNode) Freeze(NodeSource) error {
	n.frozen = true
	return nil
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

func (n *ValueNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sValue (ID: %v/%t): %v - %v\n", indent, thisId, n.frozen, n.key, n.value)
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
	for i := 0; i < 16; i++ {
		if err := encoder.Load(src[i*step:], &node.children[i]); err != nil {
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
	if err := infoEncoder.Store(dst, &node.info); err != nil {
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
	if err := infoEncoder.Load(src, &node.info); err != nil {
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
