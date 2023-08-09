package s4

//go:generate mockgen -source nodes.go -destination nodes_mocks.go -package s4

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// This file defines the interface and implementation of all node types in a
// Merkle Patricia Tries (MPT). There are five different types of nodes:
//
//  - empty nodes     ... the root node of empty sub-tries
//  - branch nodes    ... inner trie nodes splitting navigation paths
//  - extension nodes ... shortcuts for long-sequences of 1-child branches
//  - account nodes   ... mid-level nodes reached after consuming an address
//                        path storing account information and being the root
//                        of the account's storage trie. It can be considered
//                        the leaf nodes of the state trie and the root of the
//                        per-account storage tries.
//  - value nodes     ... leaf-level nodes reached after consuming a key path
//                        rooted by an account's storage root node.
//
// All nodes implement a common interface as defined below. Besides allowing
// the encoding of account and storage information in the node structure, nodes
// can also be frozen or released. Frozen nodes can no longer be modified and
// subsequent modifications cause modifications to be applied on a clone of the
// targeted node. Releasing nodes frees up allocated resources for itself and
// all nodes in the sub-tree rooted by the released node.
//
// To address nodes during navigation, NodeIds are used.
//
// Nodes are designed to be used in Forests, which is a multi-rooted extension
// of trees. Thus, individual nodes may be part of multiple trees induced by
// different root nodes in the forest. Tree-shaped MPTs are a special case of
// a forest with a single root. To avoid unwanted side-effects, all nodes
// shared as part of multiple trees should be frozen before being shared.

// Node defines an interface for all nodes in the MPT.
type Node interface {
	// GetAccount retrieves the account information associated to a given
	// account. All non-covered accounts have the implicit empty-info
	// associated.
	// The function requires the following parameters:
	//  - source  ... providing abstract access to resolving other nodes
	//  - address ... the address of the account to be located
	//  - path    ... the remaining path to be navigated to reach the account
	// The following results are produced:
	//  - info    ... the value associated to the key or zero
	//  - exists  ... true if the value is present, false otherwise
	//  - err     ... if the resolution of some node failed
	// This function is only supported for nodes in the MPT located between
	// the root node and an AccountNode.
	GetAccount(source NodeSource, address common.Address, path []Nibble) (info AccountInfo, exists bool, err error)

	// SetAccount updates the AccountInformation associated to a given
	// address in this trie. If the new AccountInfo is empty, the
	// account and all its storage is deleted.
	// The function requires the following parameters:
	//  - manager ... to look-up, create, and release nodes
	//  - thisId  ... the NodeID of the node this function has been called on
	//  - address ... the Address of the account to be updated
	//  - path    ... the remaining path to be navigated to reach the account
	//  - info    ... the new information to be assigned to the account
	// The following results are produced:
	//  - newRoot ... the new root of the sub-trie after the update (it may no
	//                longer be thisId and callers need to react accordingly)
	//  - changed ... true if the content of the sub-trie has changed and, for
	//                instance, the node's hash needs to be updated
	//  - err     ... if resolving, creating, or releasing nodes failed at some
	//                point during the update.
	// This function is only supported for nodes in the MPT located between
	// the root node and an AccountNode.
	SetAccount(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, info AccountInfo) (newRoot NodeId, changed bool, err error)

	// GetValue retrieves a value associated to a key in the storage trie
	// associated to an account in an MPT. All non-covered locations have the
	// implicit zero value.
	// The function requires the following parameters:
	//  - source  ... providing abstract access to resolving other nodes
	//  - key     ... the key of the value to be located
	//  - path    ... the remaining path to be navigated to reach the value
	// The following results are produced:
	//  - value   ... the value associated to the key or zero
	//  - exists  ... true if the value is present, false otherwise
	//  - err     ... if the resolution of some node failed
	// This function is only supported for nodes in the MPT located in a
	// storage trie rooted by an AccountNode.
	GetValue(source NodeSource, key common.Key, path []Nibble) (value common.Value, exists bool, err error)

	// SetValue updates the value associated to a given key in the storage
	// trie associated to an account in an MPT. If the new value is zero the
	// path reaching the value is removed from the MPT.
	// The function requires the following parameters:
	//  - manager ... to look-up, create, and release nodes
	//  - thisId  ... the NodeID of the node this function has been called on
	//  - key     ... the key of the value to be updated
	//  - path    ... the remaining path to be navigated to reach the value
	//  - value    ... the new value to be assigned with the key
	// The following results are produced:
	//  - newRoot ... the new root of the sub-trie after the update (it may no
	//                longer be thisId and callers need to react accordingly)
	//  - changed ... true if the content of the sub-trie has changed and, for
	//                instance, the node's hash needs to be updated
	//  - err     ... if resolving, creating, or releasing nodes failed at some
	//                point during the update.
	// This function is only supported for nodes in the MPT located in a
	// storage trie rooted by an AccountNode.
	SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (newRoot NodeId, changed bool, err error)

	// GetSlot retrieves a value of a slot addressed by a given key being part
	// of a given account. It is a combination of GetAccount() followed by
	// GetValue().
	// The function requires the following parameters:
	//  - source  ... providing abstract access to resolving other nodes
	//  - address ... the Address of the account to be updated
	//  - key     ... the key of the value to be located
	//  - path    ... the remaining path to be navigated to reach the account
	//                or, if already passed, the value
	// The following results are produced:
	//  - value   ... the value associated to the key or zero
	//  - exists  ... true if the value is present, false otherwise
	//  - err     ... if the resolution of some node failed
	// This function is only supported for nodes in the MPT located between
	// the root node and an AccountNode.
	GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (value common.Value, exists bool, err error)

	// SetSlot updates a value of a slot addressed by a given key being part
	// of a given account. It is a combination of GetAccount() followed by
	// SetValue().
	// The function requires the following parameters:
	//  - manager ... to look-up, create, and release nodes
	//  - thisId  ... the NodeID of the node this function has been called on
	//  - address ... the Address of the account to be updated
	//  - key     ... the key of the value to be updated
	//  - path    ... the remaining path to be navigated to reach the account
	//                or, if already passed, the value
	//  - value   ... the new value to be assigned with the key
	// The following results are produced:
	//  - newRoot ... the new root of the sub-trie after the update (it may no
	//                longer be thisId and callers need to react accordingly)
	//  - changed ... true if the content of the sub-trie has changed and, for
	//                instance, the node's hash needs to be updated
	//  - err     ... if resolving, creating, or releasing nodes failed at some
	//                point during the update.
	// This function is only supported for nodes in the MPT located between
	// the root node and an AccountNode.
	SetSlot(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, key common.Key, value common.Value) (newRoot NodeId, changed bool, err error)

	// ClearStorage deletes the entire storage associated to an account. For
	// parameter information and return values see SetValue().
	ClearStorage(manager NodeManager, thisId NodeId, address common.Address, path []Nibble) (newRoot NodeId, changed bool, err error)

	// Release releases this node and all non-frozen nodes in the sub-tree
	// rooted by this node. Only non-frozen nodes can be released.
	Release(manager NodeManager, thisId NodeId) error

	// IsFrozen indicates whether the given node is frozen or not.
	IsFrozen() bool

	// Freeze freezes this node and the entire sub-tree induced by it. After
	// freezing the node it can no longer be modified or released.
	Freeze(source NodeSource) error

	// MarkFrozen marks the current node as frozen, without freezing the
	// sub-tree. This might be used when loading frozen nodes from secondary
	// storage.
	MarkFrozen()

	// Check verifies internal invariants of this node and all nodes in its
	// induced sub-tree. It is mainly intended to validate invariants in unit
	// tests. It may be very costly for larger instances since it requires a
	// full tree-scan (linear in size of the trie).
	Check(source NodeSource, path []Nibble) error

	// Dumps this node and its induced sub-tree to the console. It is mainly
	// intended for debugging and may be very costly for larger instances.
	Dump(source NodeSource, thisId NodeId, indent string)
}

// NodeSource is a interface for any object capable of resolving NodeIds into
// Nodes. It is intended to be implemented by a Node-governing component
// handling the life-cycle of nodes and loading/storing nodes to persistent
// storage. It also serves as a central source for trie configuration flags.
type NodeSource interface {
	getConfig() MptConfig
	getNode(NodeId) (Node, error)
	getHashFor(NodeId) (common.Hash, error)
}

// NodeManager is a mutable extension of a NodeSource enabling the creation,
// update, invalidation, and releasing of nodes.
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

// EmptyNode is the node type used to represent an empty sub-trie. Empty nodes
// have no state and can thus not be modified. Any modification results in the
// creation of new nodes representing the new state.
type EmptyNode struct{}

func (EmptyNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	return AccountInfo{}, false, nil
}

func (EmptyNode) GetValue(NodeSource, common.Key, []Nibble) (common.Value, bool, error) {
	return common.Value{}, false, nil
}

func (EmptyNode) GetSlot(NodeSource, common.Address, []Nibble, common.Key) (common.Value, bool, error) {
	return common.Value{}, false, nil
}

func (e EmptyNode) SetAccount(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, info AccountInfo) (NodeId, bool, error) {
	if info.IsEmpty() {
		return thisId, false, nil
	}
	id, res, err := manager.createAccount()
	if err != nil {
		return 0, false, err
	}
	res.address = address
	res.info = info
	res.pathLength = byte(len(path))
	if err := manager.update(id, res); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (e EmptyNode) SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (NodeId, bool, error) {
	if value == (common.Value{}) {
		return thisId, false, nil
	}
	id, res, err := manager.createValue()
	if err != nil {
		return 0, false, err
	}
	res.key = key
	res.value = value
	res.pathLength = byte(len(path))
	if err := manager.update(id, res); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (e EmptyNode) SetSlot(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, key common.Key, value common.Value) (NodeId, bool, error) {
	// We can stop here, since the account does not exist and it should not
	// be implicitly created by setting a value.
	// Note: this function can only be reached while looking for the account.
	// Once the account is reached, the SetValue(..) function is used.
	return thisId, false, nil
}

func (e EmptyNode) ClearStorage(manager NodeManager, thisId NodeId, address common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	return thisId, false, nil
}

func (e EmptyNode) Release(NodeManager, NodeId) error {
	return nil
}

func (e EmptyNode) IsFrozen() bool {
	return true
}

func (e EmptyNode) MarkFrozen() {}

func (e EmptyNode) Freeze(NodeSource) error {
	// empty nodes are always frozen
	return nil
}

func (EmptyNode) Check(NodeSource, []Nibble) error {
	// No invariants to be checked.
	return nil
}

func (EmptyNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%s-empty- (ID: %v, Hash: %s)\n", indent, thisId, formatHashForDump(source, thisId))
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

// BranchNode implements a node consuming one Nibble along the path from the
// root to a leaf node in a trie. The Nibble is used to select one out of 16
// potential child nodes. Each BranchNode has at least 2 non-empty children.
type BranchNode struct {
	children [16]NodeId
	frozen   bool
}

func (n *BranchNode) getNextNodeInBranch(
	source NodeSource,
	path []Nibble,
) (Node, []Nibble, error) {
	next := n.children[path[0]]
	node, err := source.getNode(next)
	if err != nil {
		return nil, nil, err
	}
	return node, path[1:], err
}

func (n *BranchNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return AccountInfo{}, false, err
	}
	return next.GetAccount(source, address, subPath)
}

func (n *BranchNode) GetValue(source NodeSource, key common.Key, path []Nibble) (common.Value, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	return next.GetValue(source, key, subPath)
}

func (n *BranchNode) GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (common.Value, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	return next.GetSlot(source, address, subPath, key)
}

func (n *BranchNode) setNextNode(
	manager NodeManager,
	thisId NodeId,
	path []Nibble,
	createSubTree func(NodeId, Node, []Nibble) (NodeId, bool, error),
) (NodeId, bool, error) {
	// Forward call to child node.
	child := n.children[path[0]]
	node, err := manager.getNode(child)
	if err != nil {
		return 0, false, err
	}
	newRoot, hasChanged, err := createSubTree(child, node, path[1:])
	if err != nil {
		return 0, false, err
	}

	if newRoot == child {
		if hasChanged {
			manager.invalidateHash(thisId)
		}
		return thisId, hasChanged, nil
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
	if !child.IsEmpty() && newRoot.IsEmpty() {
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

				// If the extension is frozen, we need to modify a copy.
				if extensionNode.frozen {
					copyId, copy, err := manager.createExtension()
					if err != nil {
						return 0, false, nil
					}
					copy.path = extensionNode.path
					copy.next = extensionNode.next
					extension = copy
					extensionNode = copy
					remaining = copyId
					newRoot = copyId
				}

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
			} else if manager.getConfig().TrackSuffixLengthsInLeafNodes {
				// If suffix lengths need to be tracked, leaf nodes require an update.
				if remaining.IsAccount() {
					node, err := manager.getNode(remaining)
					if err != nil {
						return 0, false, err
					}
					newRoot, _, err = node.(*AccountNode).setPathLength(manager, remaining, byte(len(path)))
					if err != nil {
						return 0, false, err
					}
				} else if remaining.IsValue() {
					node, err := manager.getNode(remaining)
					if err != nil {
						return 0, false, err
					}
					newRoot, _, err = node.(*ValueNode).setPathLength(manager, remaining, byte(len(path)))
					if err != nil {
						return 0, false, err
					}
				}
			}
			manager.release(thisId)
			return newRoot, !isClone, nil
		}
	}

	manager.update(thisId, n)
	return thisId, !isClone, err
}

func (n *BranchNode) SetAccount(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, info AccountInfo) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetAccount(manager, next, address, path, info)
		},
	)
}

func (n *BranchNode) SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetValue(manager, next, key, path, value)
		},
	)
}

func (n *BranchNode) SetSlot(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, key common.Key, value common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path,
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetSlot(manager, next, address, path, key, value)
		},
	)
}

func (n *BranchNode) ClearStorage(manager NodeManager, thisId NodeId, address common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
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

func (n *BranchNode) IsFrozen() bool {
	return n.frozen
}

func (n *BranchNode) MarkFrozen() {
	n.frozen = true
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
	fmt.Printf("%sBranch (ID: %v/%t, Hash: %v):\n", indent, thisId, n.frozen, formatHashForDump(source, thisId))
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

// ExtensionNode are covering one or more Nibbles along the path from a root
// node to a leaf node in a trie. Neither the path nor the referenced sub-trie
// must be empty.
type ExtensionNode struct {
	path   Path
	next   NodeId
	frozen bool
}

func (n *ExtensionNode) getNextNodeInExtension(
	source NodeSource,
	path []Nibble,
) (Node, []Nibble, error) {
	if !n.path.IsPrefixOf(path) {
		return EmptyNode{}, nil, nil
	}
	node, err := source.getNode(n.next)
	if err != nil {
		return nil, nil, err
	}
	return node, path[n.path.Length():], nil
}

func (n *ExtensionNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	next, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return AccountInfo{}, false, err
	}
	return next.GetAccount(source, address, rest)
}

func (n *ExtensionNode) GetValue(source NodeSource, key common.Key, path []Nibble) (common.Value, bool, error) {
	next, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	return next.GetValue(source, key, rest)
}

func (n *ExtensionNode) GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (common.Value, bool, error) {
	next, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	return next.GetSlot(source, address, rest, key)
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
		newRoot, hasChanged, err := createSubTree(n.next, next, path[n.path.Length():])
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

				// Grow path length of next nodes if tracking of length is enabled.
				if manager.getConfig().TrackSuffixLengthsInLeafNodes {
					root, err := manager.getNode(newRoot)
					if err != nil {
						return 0, false, err
					}
					if newRoot.IsAccount() {
						newRoot, _, err = root.(*AccountNode).setPathLength(manager, newRoot, byte(len(path)))
					} else if newRoot.IsValue() {
						newRoot, _, err = root.(*ValueNode).setPathLength(manager, newRoot, byte(len(path)))
					} else {
						panic(fmt.Sprintf("unsupported new root type: %v", newRoot))
					}
					if err != nil {
						return 0, false, err
					}
				}

				return newRoot, !isClone, nil
			}
		} else if hasChanged {
			manager.invalidateHash(thisId)
		}
		return thisId, hasChanged, err
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

func (n *ExtensionNode) SetAccount(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, info AccountInfo) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, info.IsEmpty(),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetAccount(manager, next, address, path, info)
		},
	)
}

func (n *ExtensionNode) SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, value == (common.Value{}),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetValue(manager, next, key, path, value)
		},
	)
}

func (n *ExtensionNode) SetSlot(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, key common.Key, value common.Value) (NodeId, bool, error) {
	return n.setNextNode(manager, thisId, path, value == (common.Value{}),
		func(next NodeId, node Node, path []Nibble) (NodeId, bool, error) {
			return node.SetSlot(manager, next, address, path, key, value)
		},
	)
}

func (n *ExtensionNode) ClearStorage(manager NodeManager, thisId NodeId, address common.Address, path []Nibble) (newRoot NodeId, hasChanged bool, err error) {
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

func (n *ExtensionNode) IsFrozen() bool {
	return n.frozen
}

func (n *ExtensionNode) MarkFrozen() {
	n.frozen = true
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
	fmt.Printf("%sExtension (ID: %v/%t, Hash: %v): %v\n", indent, thisId, n.frozen, formatHashForDump(source, thisId), &n.path)
	if node, err := source.getNode(n.next); err == nil {
		node.Dump(source, n.next, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.next, err)
	}
}

// ----------------------------------------------------------------------------
//                               Account Node
// ----------------------------------------------------------------------------

// AccountNode is the node type found in the middle of an MPT structure
// representing an account. It stores the account's information and references
// the root node of the account's storage trie. It forms the boundary between
// the usage of addresses for navigating the trie and the usage of keys.
// No AccountNode may be present in the trie rooted by an accounts storage
// root. Also, the retained account information must not be empty.
type AccountNode struct {
	address common.Address
	info    AccountInfo
	storage NodeId
	frozen  bool
	// pathLengh is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
}

func (n *AccountNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	if n.address == address {
		return n.info, true, nil
	}
	return AccountInfo{}, false, nil
}

func (n *AccountNode) GetValue(NodeSource, common.Key, []Nibble) (common.Value, bool, error) {
	return common.Value{}, false, fmt.Errorf("invalid request: value query should not reach accounts")
}

func (n *AccountNode) GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (common.Value, bool, error) {
	if n.address != address {
		return common.Value{}, false, nil
	}
	subPath := ToNibblePath(key[:], source.getConfig().UseHashedPaths)
	root, err := source.getNode(n.storage)
	if err != nil {
		return common.Value{}, false, err
	}
	return root.GetValue(source, key, subPath[:])
}

func (n *AccountNode) SetAccount(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, info AccountInfo) (NodeId, bool, error) {
	// Check whether this is the correct account.
	if n.address == address {
		if info == n.info {
			return thisId, false, nil
		}
		if info.IsEmpty() {
			// TODO: test this
			if n.frozen {
				return EmptyId(), false, nil
			}
			// Recursively release the entire state DB.
			// TODO: consider performing this asynchroniously.
			root, err := manager.getNode(n.storage)
			if err != nil {
				return 0, false, err
			}
			err = root.Release(manager, n.storage)
			if err != nil {
				return 0, false, err
			}
			// Release this account node and remove it from the trie.
			manager.release(thisId)
			return EmptyId(), true, nil
		}

		// If this node is frozen, we need to write the result in
		// a new account node.
		if n.frozen {
			newId, newNode, err := manager.createAccount()
			if err != nil {
				return 0, false, err
			}
			newNode.address = address
			newNode.info = info
			newNode.pathLength = n.pathLength
			manager.update(newId, newNode)
			return newId, false, nil
		}

		n.info = info
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
	sibling.address = address
	sibling.info = info

	thisPath := ToNibblePath(n.address[:], manager.getConfig().UseHashedPaths)
	newRoot, err := splitLeafNode(manager, thisId, thisPath[:], n, path, siblingId, sibling)
	return newRoot, false, err
}

type leafNode interface {
	Node
	setPathLength(manager NodeManager, thisId NodeId, length byte) (newRoot NodeId, changed bool, err error)
}

func splitLeafNode(
	manager NodeManager,
	thisId NodeId,
	thisPath []Nibble,
	this leafNode,
	siblingPath []Nibble,
	siblingId NodeId,
	sibling leafNode,
) (NodeId, error) {
	// This single node needs to be split into
	//  - an optional common prefix extension
	//  - a branch node linking this node and
	//  - a new sibling account node to be returned

	branchId, branch, err := manager.createBranch()
	if err != nil {
		return 0, err
	}
	newRoot := branchId

	// Check whether there is a common prefix.
	partialPath := thisPath[len(thisPath)-len(siblingPath):]
	commonPrefixLength := GetCommonPrefixLength(partialPath, siblingPath)
	if commonPrefixLength > 0 {
		extensionId, extension, err := manager.createExtension()
		if err != nil {
			return 0, err
		}
		newRoot = extensionId

		extension.path = CreatePathFromNibbles(siblingPath[0:commonPrefixLength])
		extension.next = branchId
		manager.update(extensionId, extension)
	}

	// If enabled, keep track of the suffix length of leaf values.
	remainingPathLength := byte(len(partialPath)-commonPrefixLength) - 1
	if manager.getConfig().TrackSuffixLengthsInLeafNodes {
		sibling.setPathLength(manager, siblingId, remainingPathLength)
		thisId, _, err = this.setPathLength(manager, thisId, remainingPathLength)
		if err != nil {
			return 0, err
		}
	} else {
		// Commit the changes to the sibling.
		manager.update(siblingId, sibling)
	}

	// Add this node and the new sibling node to the branch node.
	branch.children[partialPath[commonPrefixLength]] = thisId
	branch.children[siblingPath[commonPrefixLength]] = siblingId

	// Commit the changes to the the branch node.
	manager.update(branchId, branch)

	return newRoot, nil
}

func (n *AccountNode) SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("setValue call should not reach account nodes")
}

func (n *AccountNode) SetSlot(manager NodeManager, thisId NodeId, address common.Address, path []Nibble, key common.Key, value common.Value) (NodeId, bool, error) {
	// If this is not the correct account, the real account does not exist
	// and the insert can be skipped. The insertion of a slot value shall
	// not create an account.
	if n.address != address {
		return thisId, false, nil
	}

	// Continue from here with a value insertion.
	node, err := manager.getNode(n.storage)
	if err != nil {
		return 0, false, err
	}
	subPath := ToNibblePath(key[:], manager.getConfig().UseHashedPaths)
	root, hasChanged, err := node.SetValue(manager, n.storage, key, subPath[:], value)
	if err != nil {
		return 0, false, err
	}
	if root != n.storage {
		// If this node is frozen, we need to write the result in
		// a new account node.
		// TODO: add a unit test for this
		if n.frozen {
			newId, newNode, err := manager.createAccount()
			if err != nil {
				return 0, false, err
			}
			newNode.address = address
			newNode.info = n.info
			newNode.pathLength = n.pathLength
			newNode.storage = root
			return newId, false, nil
		}
		n.storage = root
		hasChanged = true
		manager.update(thisId, n)
	} else if hasChanged {
		manager.invalidateHash(thisId)
	}
	return thisId, hasChanged, nil
}

func (n *AccountNode) ClearStorage(manager NodeManager, thisId NodeId, address common.Address, path []Nibble) (newRoot NodeId, changed bool, err error) {
	if n.address != address || n.storage.IsEmpty() {
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
		newNode.address = address
		newNode.info = n.info
		newNode.pathLength = n.pathLength
		newNode.storage = EmptyId()
		return newId, false, nil
	}

	root, err := manager.getNode(n.storage)
	if err != nil {
		return thisId, false, err
	}

	err = root.Release(manager, n.storage)
	n.storage = EmptyId()
	return thisId, true, err
}

func (n *AccountNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	if !n.storage.IsEmpty() {
		if err := manager.release(n.storage); err != nil {
			return err
		}
	}
	return manager.release(thisId)
}

func (n *AccountNode) setPathLength(manager NodeManager, thisId NodeId, length byte) (NodeId, bool, error) {
	if n.pathLength == length {
		return thisId, false, nil
	}
	if n.frozen {
		newId, newNode, err := manager.createAccount()
		if err != nil {
			return 0, false, err
		}
		newNode.address = n.address
		newNode.info = n.info
		newNode.pathLength = length
		return newId, false, manager.update(newId, newNode)
	}

	n.pathLength = length
	return thisId, true, manager.update(thisId, n)
}

func (n *AccountNode) IsFrozen() bool {
	return n.frozen
}

func (n *AccountNode) MarkFrozen() {
	n.frozen = true
}

func (n *AccountNode) Freeze(source NodeSource) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	node, err := source.getNode(n.storage)
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
	//  - path length
	errs := []error{}

	fullPath := ToNibblePath(n.address[:], source.getConfig().UseHashedPaths)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("account node %v located in wrong branch: %v", n.address, path))
	}

	if n.info.IsEmpty() {
		errs = append(errs, fmt.Errorf("account information must not be empty"))
	}

	if !n.storage.IsEmpty() {
		if node, err := source.getNode(n.storage); err == nil {
			if err := node.Check(source, make([]Nibble, 0, common.KeySize*2)); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, err)
		}
	}

	if source.getConfig().TrackSuffixLengthsInLeafNodes {
		if got, want := n.pathLength, byte(64-len(path)); got != want {
			errs = append(errs, fmt.Errorf("invalid path length, wanted %d, got %d", want, got))
		}
	}

	return errors.Join(errs...)
}

func (n *AccountNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sAccount (ID: %v/%t/%v, Hash: %v): %v - %v\n", indent, thisId, n.frozen, n.pathLength, formatHashForDump(source, thisId), n.address, n.info)
	if n.storage.IsEmpty() {
		return
	}
	if node, err := source.getNode(n.storage); err == nil {
		node.Dump(source, n.storage, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.storage, err)
	}
}

// ----------------------------------------------------------------------------
//                               Value Node
// ----------------------------------------------------------------------------

// ValueNode store the value of a storage slot of an account. Values must not
// be zero. Also, value nodes must not be reachable in a trie before crossing
// exactly one AccountNode.
type ValueNode struct {
	key    common.Key
	value  common.Value
	frozen bool
	// pathLengh is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
}

func (n *ValueNode) GetAccount(NodeSource, common.Address, []Nibble) (AccountInfo, bool, error) {
	return AccountInfo{}, false, fmt.Errorf("invalid request: account query should not reach values")
}

func (n *ValueNode) GetValue(source NodeSource, key common.Key, path []Nibble) (common.Value, bool, error) {
	if n.key == key {
		return n.value, true, nil
	}
	return common.Value{}, false, nil
}

func (n *ValueNode) GetSlot(NodeSource, common.Address, []Nibble, common.Key) (common.Value, bool, error) {
	return common.Value{}, false, fmt.Errorf("invalid request: slot query should not reach values")
}

func (n *ValueNode) SetAccount(NodeManager, NodeId, common.Address, []Nibble, AccountInfo) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: account update should not reach values")
}

func (n *ValueNode) SetValue(manager NodeManager, thisId NodeId, key common.Key, path []Nibble, value common.Value) (NodeId, bool, error) {
	// Check whether this is the correct value node.
	if n.key == key {
		if value == n.value {
			return thisId, false, nil
		}
		if value == (common.Value{}) {
			if !n.frozen {
				manager.release(thisId)
			}
			return EmptyId(), !n.frozen, nil
		}
		if n.frozen {
			newId, newNode, err := manager.createValue()
			if err != nil {
				return 0, false, nil
			}
			newNode.key = n.key
			newNode.value = value
			newNode.pathLength = n.pathLength
			manager.update(newId, newNode)
			return newId, false, nil
		}
		n.value = value
		manager.update(thisId, n)
		return thisId, true, nil
	}

	// Skip restructuring the tree if the new info is empty.
	if value == (common.Value{}) {
		return thisId, false, nil
	}

	// Create a new node for the sibling to be added.
	siblingId, sibling, err := manager.createValue()
	if err != nil {
		return 0, false, err
	}
	sibling.key = key
	sibling.value = value

	thisPath := ToNibblePath(n.key[:], manager.getConfig().UseHashedPaths)
	newRootId, err := splitLeafNode(manager, thisId, thisPath[:], n, path, siblingId, sibling)
	return newRootId, false, err
}

func (n *ValueNode) SetSlot(NodeManager, NodeId, common.Address, []Nibble, common.Key, common.Value) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: slot update should not reach values")
}

func (n *ValueNode) ClearStorage(NodeManager, NodeId, common.Address, []Nibble) (NodeId, bool, error) {
	return 0, false, fmt.Errorf("invalid request: clear storage should not reach values")
}

func (n *ValueNode) Release(manager NodeManager, thisId NodeId) error {
	if n.frozen {
		return nil
	}
	return manager.release(thisId)
}

func (n *ValueNode) setPathLength(manager NodeManager, thisId NodeId, length byte) (NodeId, bool, error) {
	if n.pathLength == length {
		return thisId, false, nil
	}
	if n.frozen {
		newId, newNode, err := manager.createValue()
		if err != nil {
			return 0, false, err
		}
		newNode.key = n.key
		newNode.value = n.value
		newNode.pathLength = length
		return newId, false, manager.update(newId, newNode)
	}

	n.pathLength = length
	return thisId, true, manager.update(thisId, n)
}

func (n *ValueNode) IsFrozen() bool {
	return n.frozen
}

func (n *ValueNode) MarkFrozen() {
	n.frozen = true
}

func (n *ValueNode) Freeze(NodeSource) error {
	n.frozen = true
	return nil
}

func (n *ValueNode) Check(source NodeSource, path []Nibble) error {
	// Checked invariants:
	//  - value must not be empty
	//  - values are in the right position of the trie
	//  - the path length is correct (if enabled to be tracked)
	errs := []error{}

	fullPath := ToNibblePath(n.key[:], source.getConfig().UseHashedPaths)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("value node %v located in wrong branch: %v", n.key, path))
	}

	if n.value == (common.Value{}) {
		errs = append(errs, fmt.Errorf("value slot must not be empty"))
	}

	if source.getConfig().TrackSuffixLengthsInLeafNodes {
		if got, want := n.pathLength, byte(64-len(path)); got != want {
			errs = append(errs, fmt.Errorf("invalid path length, wanted %d, got %d", want, got))
		}
	}

	return errors.Join(errs...)
}

func (n *ValueNode) Dump(source NodeSource, thisId NodeId, indent string) {
	fmt.Printf("%sValue (ID: %v/%t/%d, Hash: %v): %v - %v\n", indent, thisId, n.frozen, n.pathLength, formatHashForDump(source, thisId), n.key, n.value)
}

func formatHashForDump(source NodeSource, id NodeId) string {
	hash, err := source.getHashFor(id)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	return fmt.Sprintf("0x%x", hash)
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
	return idEncoder.Store(dst, &node.storage)
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
	if err := idEncoder.Load(src, &node.storage); err != nil {
		return err
	}

	return nil
}

// AccountNodeWithPathLengthEncoder is the same as the AccountNodeEncoder but
// also includes the length of the path covered by the leaf node in the output.
type AccountNodeWithPathLengthEncoder struct{}

func (AccountNodeWithPathLengthEncoder) GetEncodedSize() int {
	return AccountNodeEncoder{}.GetEncodedSize() + 1
}

func (AccountNodeWithPathLengthEncoder) Store(dst []byte, node *AccountNode) error {
	AccountNodeEncoder{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (AccountNodeWithPathLengthEncoder) Load(src []byte, node *AccountNode) error {
	AccountNodeEncoder{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}

type ValueNodeEncoder struct{}

func (ValueNodeEncoder) GetEncodedSize() int {
	return common.KeySize + common.ValueSize
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

// ValueNodeWithPathLengthEncoder is the same as the ValueNodeEncoder but
// also includes the length of the path covered by the leaf node in the output.
type ValueNodeWithPathLengthEncoder struct{}

func (ValueNodeWithPathLengthEncoder) GetEncodedSize() int {
	return ValueNodeEncoder{}.GetEncodedSize() + 1
}

func (ValueNodeWithPathLengthEncoder) Store(dst []byte, node *ValueNode) error {
	ValueNodeEncoder{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (ValueNodeWithPathLengthEncoder) Load(src []byte, node *ValueNode) error {
	ValueNodeEncoder{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}
