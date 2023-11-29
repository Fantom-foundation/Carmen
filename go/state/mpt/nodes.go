package mpt

//go:generate mockgen -source nodes.go -destination nodes_mocks.go -package mpt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
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
	SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (newRoot NodeReference, changed bool, err error)

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
	SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (newRoot NodeReference, changed bool, err error)

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
	SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (newRoot NodeReference, changed bool, err error)

	// ClearStorage deletes the entire storage associated to an account. For
	// parameter information and return values see SetValue().
	ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, changed bool, err error)

	// Release releases this node and all non-frozen nodes in the sub-tree
	// rooted by this node. Only non-frozen nodes can be released.
	Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error

	// GetHash obtains the potentially dirty hash currently retained for this node.
	GetHash() (hash common.Hash, dirty bool)

	// SetHash updates this nodes hash.
	SetHash(common.Hash)

	// IsFrozen indicates whether the given node is frozen or not.
	IsFrozen() bool

	// Freeze freezes this node and the entire sub-tree induced by it. After
	// freezing the node it can no longer be modified or released.
	Freeze(manager NodeManager, this shared.WriteHandle[Node]) error

	// MarkFrozen marks the current node as frozen, without freezing the
	// sub-tree. This might be used when loading frozen nodes from secondary
	// storage.
	MarkFrozen()

	// Check verifies internal invariants of this node and all nodes in its
	// induced sub-tree. It is mainly intended to validate invariants in unit
	// tests. It may be very costly for larger instances since it requires a
	// full tree-scan (linear in size of the trie).
	Check(source NodeSource, thisRef *NodeReference, path []Nibble, ancestors []NodeId) error

	// Dump dumps this node and its sub-trees to the console. It is mainly
	// intended for debugging and may be very costly for larger instances.
	Dump(source NodeSource, thisRef *NodeReference, indent string)

	// Visit visits this and all nodes in the respective sub-tree. The
	// visitor is called by each encountered node, with the proper NodeInfo
	// set. Visiting aborts if the visitor returns or prune sub-tree as
	// requested by the visitor. The function returns whether the visiting
	// process has been aborted and/or an error occurred.
	Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (abort bool, err error)
}

// NodeSource is a interface for any object capable of resolving NodeIds into
// Nodes. It is intended to be implemented by a Node-governing component
// handling the life-cycle of nodes and loading/storing nodes to persistent
// storage. It also serves as a central source for trie configuration flags.
type NodeSource interface {
	getConfig() MptConfig
	getReadAccess(*NodeReference) (shared.ReadHandle[Node], error)
	getViewAccess(*NodeReference) (shared.ViewHandle[Node], error)
	getHashFor(*NodeReference) (common.Hash, error)
	hashKey(common.Key) common.Hash
	hashAddress(address common.Address) common.Hash
}

// NodeManager is a mutable extension of a NodeSource enabling the creation,
// update, invalidation, and releasing of nodes.
type NodeManager interface {
	NodeSource

	getHashAccess(*NodeReference) (shared.HashHandle[Node], error)
	getWriteAccess(*NodeReference) (shared.WriteHandle[Node], error)

	createAccount() (NodeReference, shared.WriteHandle[Node], error)
	createBranch() (NodeReference, shared.WriteHandle[Node], error)
	createExtension() (NodeReference, shared.WriteHandle[Node], error)
	createValue() (NodeReference, shared.WriteHandle[Node], error)

	update(NodeId, shared.WriteHandle[Node]) error
	updateHash(NodeId, shared.HashHandle[Node]) error

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

func (e EmptyNode) SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (NodeReference, bool, error) {
	if info.IsEmpty() {
		return *thisRef, false, nil
	}
	ref, handle, err := manager.createAccount()
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	res := handle.Get().(*AccountNode)
	res.hashDirty = true
	res.address = address
	res.info = info
	res.pathLength = byte(len(path))
	if err := manager.update(ref.Id(), handle); err != nil {
		return NodeReference{}, false, err
	}
	return ref, true, nil
}

func (e EmptyNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	if value == (common.Value{}) {
		return *thisRef, false, nil
	}
	ref, handle, err := manager.createValue()
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	res := handle.Get().(*ValueNode)
	res.key = key
	res.value = value
	res.hashDirty = true
	res.pathLength = byte(len(path))
	if err := manager.update(ref.Id(), handle); err != nil {
		return NodeReference{}, false, err
	}
	return ref, true, nil
}

func (e EmptyNode) SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (NodeReference, bool, error) {
	// We can stop here, since the account does not exist and it should not
	// be implicitly created by setting a value.
	// Note: this function can only be reached while looking for the account.
	// Once the account is reached, the SetValue(..) function is used.
	return *thisRef, false, nil
}

func (e EmptyNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, changed bool, err error) {
	return *thisRef, false, nil
}

func (e EmptyNode) Release(NodeManager, *NodeReference, shared.WriteHandle[Node]) error {
	return nil
}

func (e EmptyNode) GetHash() (common.Hash, bool) {
	return common.Hash{}, true
}

func (e EmptyNode) SetHash(common.Hash) { /* ignored */ }

func (e EmptyNode) IsFrozen() bool {
	return true
}

func (e EmptyNode) MarkFrozen() {}

func (e EmptyNode) Freeze(NodeManager, shared.WriteHandle[Node]) error {
	// empty nodes are always frozen
	return nil
}

func (EmptyNode) Check(NodeSource, *NodeReference, []Nibble, []NodeId) error {
	// No invariants to be checked.
	return nil
}

func (EmptyNode) Dump(_ NodeSource, thisRef *NodeReference, indent string) {
	fmt.Printf("%s-empty- (ID: %v)\n", indent, thisRef.Id())
}

func (EmptyNode) Visit(_ NodeSource, ref *NodeReference, depth int, visitor NodeVisitor) (bool, error) {
	return visitor.Visit(EmptyNode{}, NodeInfo{Id: ref.Id(), Depth: &depth}) == VisitResponseAbort, nil
}

// ----------------------------------------------------------------------------
//                               Branch Node
// ----------------------------------------------------------------------------

// BranchNode implements a node consuming one Nibble along the path from the
// root to a leaf node in a trie. The Nibble is used to select one out of 16
// potential child nodes. Each BranchNode has at least 2 non-empty children.
type BranchNode struct {
	children         [16]NodeReference // the ID of child nodes
	hashes           [16]common.Hash   // the hashes of child nodes
	dirtyHashes      uint16            // a bit mask marking hashes as dirty; 0 .. clean, 1 .. dirty
	embeddedChildren uint16            // a bit mask marking children as embedded; 0 .. not, 1 .. embedded
	frozen           bool              // a flag marking the node as immutable
	frozenChildren   uint16            // a bit mask marking frozen children; not persisted
	hash             common.Hash       // the hash of this node (may be dirty)
	hashDirty        bool              // indicating whether this node's hash is dirty
}

func (n *BranchNode) getNextNodeInBranch(
	source NodeSource,
	path []Nibble,
) (shared.ReadHandle[Node], []Nibble, error) {
	next := &n.children[path[0]]
	node, err := source.getReadAccess(next)
	if err != nil {
		return shared.ReadHandle[Node]{}, nil, err
	}
	return node, path[1:], err
}

func (n *BranchNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return AccountInfo{}, false, err
	}
	defer next.Release()
	return next.Get().GetAccount(source, address, subPath)
}

func (n *BranchNode) GetValue(source NodeSource, key common.Key, path []Nibble) (common.Value, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	defer next.Release()
	return next.Get().GetValue(source, key, subPath)
}

func (n *BranchNode) GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (common.Value, bool, error) {
	next, subPath, err := n.getNextNodeInBranch(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	defer next.Release()
	return next.Get().GetSlot(source, address, subPath, key)
}

func (n *BranchNode) setNextNode(
	manager NodeManager,
	thisRef *NodeReference,
	this shared.WriteHandle[Node],
	path []Nibble,
	createSubTree func(*NodeReference, shared.WriteHandle[Node], []Nibble) (NodeReference, bool, error),
) (NodeReference, bool, error) {
	// Forward call to child node.
	child := &n.children[path[0]]
	node, err := manager.getWriteAccess(child)
	if err != nil {
		return NodeReference{}, false, err
	}
	newRoot, hasChanged, err := createSubTree(child, node, path[1:])
	node.Release()
	if err != nil {
		return NodeReference{}, false, err
	}

	if newRoot.Id() == child.Id() {
		if hasChanged {
			n.hashDirty = true
			n.markChildHashDirty(byte(path[0]))
		}
		return *thisRef, hasChanged, nil
	}

	// If frozen, clone the current node and modify copy.
	isClone := false
	if n.frozen {
		newRef, handle, err := manager.createBranch()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer handle.Release()
		newNode := handle.Get().(*BranchNode)
		*newNode = *n
		newNode.frozen = false
		n = newNode
		thisRef = &newRef
		this = handle
		isClone = true
	}

	wasEmpty := child.Id().IsEmpty()
	n.children[path[0]] = newRoot
	n.hashDirty = true
	n.markChildHashDirty(byte(path[0]))
	n.setChildFrozen(byte(path[0]), false)

	// If a branch got removed, check that there are enough children left.
	if !wasEmpty && newRoot.Id().IsEmpty() {
		count := 0
		var remainingPos Nibble
		var remaining NodeReference
		for i, cur := range n.children {
			if !cur.Id().IsEmpty() {
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
			if remaining.Id().IsExtension() {
				// The present extension can be extended.
				extension, err := manager.getWriteAccess(&remaining)
				if err != nil {
					return NodeReference{}, false, err
				}
				defer extension.Release()
				extensionNode := extension.Get().(*ExtensionNode)
				remainingHandle := extension

				// If the extension is frozen, we need to modify a copy.
				if extensionNode.frozen {
					copyId, handle, err := manager.createExtension()
					if err != nil {
						return NodeReference{}, false, nil
					}
					defer handle.Release()
					copy := handle.Get().(*ExtensionNode)
					*copy = *extensionNode
					copy.frozen = false
					extensionNode = copy
					remainingHandle = handle
					remaining = copyId
					newRoot = copyId
				}

				extensionNode.path.Prepend(remainingPos)
				extensionNode.hashDirty = true
				manager.update(remaining.Id(), remainingHandle)
			} else if remaining.Id().IsBranch() {
				// An extension needs to replace this branch.
				extensionRef, handle, err := manager.createExtension()
				if err != nil {
					return NodeReference{}, false, err
				}
				defer handle.Release()
				extension := handle.Get().(*ExtensionNode)
				extension.path = SingleStepPath(remainingPos)
				extension.next = remaining
				extension.hashDirty = true
				extension.nextHashDirty = true
				manager.update(extensionRef.Id(), handle)
				newRoot = extensionRef
			} else if manager.getConfig().TrackSuffixLengthsInLeafNodes {
				// If suffix lengths need to be tracked, leaf nodes require an update.
				if remaining.Id().IsAccount() {
					handle, err := manager.getWriteAccess(&remaining)
					if err != nil {
						return NodeReference{}, false, err
					}
					newRoot, _, err = handle.Get().(*AccountNode).setPathLength(manager, &remaining, handle, byte(len(path)))
					handle.Release()
					if err != nil {
						return NodeReference{}, false, err
					}
				} else if remaining.Id().IsValue() {
					handle, err := manager.getWriteAccess(&remaining)
					if err != nil {
						return NodeReference{}, false, err
					}
					newRoot, _, err = handle.Get().(*ValueNode).setPathLength(manager, &remaining, handle, byte(len(path)))
					handle.Release()
					if err != nil {
						return NodeReference{}, false, err
					}
				}
			}
			manager.release(thisRef.Id())
			return newRoot, !isClone, nil
		}
	}

	manager.update(thisRef.Id(), this)
	return *thisRef, !isClone, err
}

func (n *BranchNode) SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetAccount(manager, next, node, address, path, info)
		},
	)
}

func (n *BranchNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetValue(manager, next, node, key, path, value)
		},
	)
}

func (n *BranchNode) SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetSlot(manager, next, node, address, path, key, value)
		},
	)
}

func (n *BranchNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, changed bool, err error) {
	return n.setNextNode(manager, thisRef, this, path,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().ClearStorage(manager, next, node, address, path)
		},
	)
}

func (n *BranchNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	for _, cur := range n.children {
		if !cur.Id().IsEmpty() {
			handle, err := manager.getWriteAccess(&cur)
			if err != nil {
				return err
			}
			err = handle.Get().Release(manager, &cur, handle)
			handle.Release()
			if err != nil {
				return err
			}
		}
	}
	return manager.release(thisRef.Id())
}

func (n *BranchNode) GetHash() (common.Hash, bool) {
	return n.hash, n.hashDirty
}

func (n *BranchNode) SetHash(hash common.Hash) {
	n.hash = hash
	n.hashDirty = false
}

func (n *BranchNode) IsFrozen() bool {
	return n.frozen
}

func (n *BranchNode) MarkFrozen() {
	n.frozen = true
	n.frozenChildren = ^uint16(0)
}

func (n *BranchNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	for i := 0; i < len(n.children); i++ {
		if n.children[i].Id().IsEmpty() || n.isChildFrozen(byte(i)) {
			continue
		}
		handle, err := manager.getWriteAccess(&n.children[i])
		if err != nil {
			return err
		}
		err = handle.Get().Freeze(manager, handle)
		handle.Release()
		if err != nil {
			return err
		}
		n.setChildFrozen(byte(i), true)
	}
	return nil
}

func (n *BranchNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble, ancestors []NodeId) error {
	// Checked invariants:
	//  - no loop in trie
	//  - must have 2+ children
	//  - child trees must be error free
	if slices.Contains(ancestors, thisRef.Id()) {
		return fmt.Errorf("node cycle detected: %v in %v", thisRef.Id(), ancestors)
	}
	ancestors = append(ancestors, thisRef.Id())
	numChildren := 0
	errs := []error{}
	for i, child := range n.children {
		if child.Id().IsEmpty() {
			continue
		}
		numChildren++

		if handle, err := source.getViewAccess(&child); err == nil {
			defer handle.Release()
			if err := handle.Get().Check(source, &child, append(path, Nibble(i)), ancestors); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("unable to resolve node %v: %v", child, err))
		}

		if !n.isChildHashDirty(byte(i)) && !n.isEmbedded(byte(i)) {
			want, err := source.getHashFor(&child)
			if err != nil {
				errs = append(errs, err)
			} else if got := n.hashes[i]; want != got {
				errs = append(errs, fmt.Errorf("in node %v the hash for child %d is invalid\nwant: %v\ngot: %v", thisRef.Id(), i, want, got))
			}
		}
	}
	if numChildren < 2 {
		errs = append(errs, fmt.Errorf("node %v has an insufficient number of child nodes: %d", thisRef.Id(), numChildren))
	}
	return errors.Join(errs...)
}

func (n *BranchNode) Dump(source NodeSource, thisRef *NodeReference, indent string) {
	fmt.Printf("%sBranch (ID: %v/%t, Dirty: %016b, Embedded: %016b, Frozen: %016b, Hash: %v, dirtyHash: %t):\n", indent, thisRef.Id(), n.frozen, n.dirtyHashes, n.embeddedChildren, n.frozenChildren, formatHashForDump(n.hash), n.hashDirty)
	for i, child := range n.children {
		if child.Id().IsEmpty() {
			continue
		}

		if handle, err := source.getViewAccess(&child); err == nil {
			defer handle.Release()
			handle.Get().Dump(source, &child, fmt.Sprintf("%s  %v ", indent, Nibble(i)))
		} else {
			fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, child, err)
		}
	}
}

func (b *BranchNode) Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (bool, error) {
	switch visitor.Visit(b, NodeInfo{Id: thisRef.Id(), Depth: &depth}) {
	case VisitResponseAbort:
		return true, nil
	case VisitResponsePrune:
		return false, nil
	case VisitResponseContinue: /* keep going */
	}
	for _, child := range b.children {
		if child.Id().IsEmpty() {
			continue
		}

		if handle, err := source.getViewAccess(&child); err == nil {
			defer handle.Release()
			if abort, err := handle.Get().Visit(source, &child, depth+1, visitor); abort || err != nil {
				return abort, err
			}
		} else {
			return false, err
		}
	}
	return false, nil
}

func (n *BranchNode) markChildHashDirty(index byte) {
	n.dirtyHashes = n.dirtyHashes | (1 << index)
}

func (n *BranchNode) isChildHashDirty(index byte) bool {
	return (n.dirtyHashes & (1 << index)) != 0
}

func (n *BranchNode) clearChildHashDirtyFlags() {
	n.dirtyHashes = 0
}

func (n *BranchNode) isEmbedded(index byte) bool {
	return (n.embeddedChildren & (1 << index)) != 0
}

func (n *BranchNode) setEmbedded(index byte, embedded bool) {
	if embedded {
		n.embeddedChildren = n.embeddedChildren | (1 << index)
	} else {
		n.embeddedChildren = n.embeddedChildren & ^(1 << index)
	}
}

func (n *BranchNode) isChildFrozen(index byte) bool {
	return (n.frozenChildren & (1 << index)) != 0
}

func (n *BranchNode) setChildFrozen(index byte, frozen bool) {
	if frozen {
		n.frozenChildren = n.frozenChildren | (1 << index)
	} else {
		n.frozenChildren = n.frozenChildren & ^(1 << index)
	}
}

// ----------------------------------------------------------------------------
//                              Extension Node
// ----------------------------------------------------------------------------

// ExtensionNode are covering one or more Nibbles along the path from a root
// node to a leaf node in a trie. Neither the path nor the referenced sub-trie
// must be empty.
type ExtensionNode struct {
	path           Path
	next           NodeReference
	nextHash       common.Hash
	nextHashDirty  bool
	nextIsEmbedded bool // TODO: include this in encoding; also for the branch node
	frozen         bool
	hash           common.Hash // the hash of this node (may be dirty)
	hashDirty      bool        // indicating whether this node's hash is dirty
}

func (n *ExtensionNode) getNextNodeInExtension(
	source NodeSource,
	path []Nibble,
) (shared.ReadHandle[Node], []Nibble, error) {
	if !n.path.IsPrefixOf(path) {
		shared := shared.MakeShared[Node](EmptyNode{})
		return shared.GetReadHandle(), nil, nil
	}
	handle, err := source.getReadAccess(&n.next)
	if err != nil {
		return shared.ReadHandle[Node]{}, nil, err
	}
	return handle, path[n.path.Length():], nil
}

func (n *ExtensionNode) GetAccount(source NodeSource, address common.Address, path []Nibble) (AccountInfo, bool, error) {
	handle, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return AccountInfo{}, false, err
	}
	defer handle.Release()
	return handle.Get().GetAccount(source, address, rest)
}

func (n *ExtensionNode) GetValue(source NodeSource, key common.Key, path []Nibble) (common.Value, bool, error) {
	handle, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	defer handle.Release()
	return handle.Get().GetValue(source, key, rest)
}

func (n *ExtensionNode) GetSlot(source NodeSource, address common.Address, path []Nibble, key common.Key) (common.Value, bool, error) {
	handle, rest, err := n.getNextNodeInExtension(source, path)
	if err != nil {
		return common.Value{}, false, err
	}
	defer handle.Release()
	return handle.Get().GetSlot(source, address, rest, key)
}

func (n *ExtensionNode) setNextNode(
	manager NodeManager,
	thisRef *NodeReference,
	this shared.WriteHandle[Node],
	path []Nibble,
	valueIsEmpty bool,
	createSubTree func(*NodeReference, shared.WriteHandle[Node], []Nibble) (NodeReference, bool, error),
) (NodeReference, bool, error) {
	// Check whether the updates targets the node referenced by this extension.
	if n.path.IsPrefixOf(path) {
		handle, err := manager.getWriteAccess(&n.next)
		if err != nil {
			return NodeReference{}, false, err
		}
		defer handle.Release()
		newRoot, hasChanged, err := createSubTree(&n.next, handle, path[n.path.Length():])
		if err != nil {
			return NodeReference{}, false, err
		}

		if newRoot.Id().IsEmpty() {
			if n.frozen {
				return NewNodeReference(EmptyId()), false, nil
			}
			manager.release(thisRef.Id())
			return newRoot, true, nil
		}

		if newRoot != n.next {

			// If frozen, modify a clone.
			isClone := false
			if n.frozen {
				newRef, handle, err := manager.createExtension()
				if err != nil {
					return NodeReference{}, false, err
				}
				defer handle.Release()
				newNode := handle.Get().(*ExtensionNode)
				*newNode = *n
				newNode.frozen = false
				thisRef, this, n = &newRef, handle, newNode
				isClone = true
			}

			// The referenced sub-tree has changed, so the hash needs to be updated.
			n.nextHashDirty = true

			if newRoot.Id().IsExtension() {
				// If the new next is an extension, merge it into this extension.
				handle, err := manager.getWriteAccess(&newRoot)
				if err != nil {
					return NodeReference{}, false, err
				}
				defer handle.Release()
				extension := handle.Get().(*ExtensionNode)
				n.path.AppendAll(&extension.path)
				n.next = extension.next
				n.hashDirty = true
				n.nextHashDirty = true
				manager.update(thisRef.Id(), this)
				manager.release(newRoot.Id())
			} else if newRoot.Id().IsBranch() {
				n.next = newRoot
				n.hashDirty = true
				n.nextHashDirty = true
				manager.update(thisRef.Id(), this)
			} else {
				// If the next node is anything but a branch or extension, remove this extension.
				manager.release(thisRef.Id())

				// Grow path length of next nodes if tracking of length is enabled.
				if manager.getConfig().TrackSuffixLengthsInLeafNodes {
					root, err := manager.getWriteAccess(&newRoot)
					if err != nil {
						return NodeReference{}, false, err
					}
					if newRoot.Id().IsAccount() {
						newRoot, _, err = root.Get().(*AccountNode).setPathLength(manager, &newRoot, root, byte(len(path)))
					} else if newRoot.Id().IsValue() {
						newRoot, _, err = root.Get().(*ValueNode).setPathLength(manager, &newRoot, root, byte(len(path)))
					} else {
						panic(fmt.Sprintf("unsupported new next node type: %v", newRoot))
					}
					root.Release()
					if err != nil {
						return NodeReference{}, false, err
					}
				}

				return newRoot, !isClone, nil
			}
		} else if hasChanged {
			n.hashDirty = true
			n.nextHashDirty = true
		}
		return *thisRef, hasChanged, err
	}

	// Skip creation of a new sub-tree if the info is empty.
	if valueIsEmpty {
		return *thisRef, false, nil
	}

	// If frozen, modify a clone.
	isClone := false
	if n.frozen {
		newRef, handle, err := manager.createExtension()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer handle.Release()
		newNode := handle.Get().(*ExtensionNode)
		*newNode = *n
		newNode.frozen = false
		thisRef, this, n = &newRef, handle, newNode
		isClone = true
	}

	// Extension needs to be replaced by a combination of
	//  - an optional common prefix extension
	//  - a branch node
	//  - an optional extension connecting to the previous next node

	// Create the branch node that will be needed in any case.
	branchRef, branchHandle, err := manager.createBranch()
	if err != nil {
		return NodeReference{}, false, err
	}
	defer branchHandle.Release()
	newRoot := branchRef
	branch := branchHandle.Get().(*BranchNode)

	// Determine the point at which the prefix need to be split.
	commonPrefixLength := n.path.GetCommonPrefixLength(path)

	// Build the extension connecting the branch to the next node.
	thisNodeWasReused := false
	if commonPrefixLength < n.path.Length()-1 {
		// We re-use the current node for this - all we need is to update the path.
		branch.children[n.path.Get(commonPrefixLength)] = *thisRef
		branch.markChildHashDirty(byte(n.path.Get(commonPrefixLength)))
		n.path.ShiftLeft(commonPrefixLength + 1)
		n.hashDirty = true
		n.nextHashDirty = true
		thisNodeWasReused = true
		manager.update(thisRef.Id(), this)
	} else {
		branch.children[n.path.Get(commonPrefixLength)] = n.next
		branch.markChildHashDirty(byte(n.path.Get(commonPrefixLength)))
	}

	// Build the extension covering the common prefix.
	if commonPrefixLength > 0 {
		// Reuse current node unless already taken.
		extension := n
		extensionRef := *thisRef
		extensionHandle := this
		if thisNodeWasReused {
			extensionRef, extensionHandle, err = manager.createExtension()
			if err != nil {
				return NodeReference{}, false, err
			}
			defer extensionHandle.Release()
			extension = extensionHandle.Get().(*ExtensionNode)
		} else {
			thisNodeWasReused = true
		}

		extension.path = CreatePathFromNibbles(path[0:commonPrefixLength])
		extension.next = branchRef
		extension.hashDirty = true
		extension.nextHashDirty = true
		manager.update(extensionRef.Id(), extensionHandle)
		newRoot = extensionRef
	}

	// Continue insertion of new account at new branch level.
	_, _, err = createSubTree(&branchRef, branchHandle, path[commonPrefixLength:])
	if err != nil {
		return NodeReference{}, false, err
	}

	// If this node was not needed any more, we can discard it.
	if !thisNodeWasReused {
		manager.release(thisRef.Id())
	}

	return newRoot, !isClone, nil
}

func (n *ExtensionNode) SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path, info.IsEmpty(),
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetAccount(manager, next, node, address, path, info)
		},
	)
}

func (n *ExtensionNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path, value == (common.Value{}),
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetValue(manager, next, node, key, path, value)
		},
	)
}

func (n *ExtensionNode) SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, this, path, true,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetSlot(manager, next, node, address, path, key, value)
		},
	)
}

func (n *ExtensionNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, hasChanged bool, err error) {
	return n.setNextNode(manager, thisRef, this, path, true,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().ClearStorage(manager, next, node, address, path)
		},
	)
}

func (n *ExtensionNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	handle, err := manager.getWriteAccess(&n.next)
	if err != nil {
		return err
	}
	defer handle.Release()
	err = handle.Get().Release(manager, &n.next, handle)
	if err != nil {
		return err
	}
	return manager.release(thisRef.Id())
}

func (n *ExtensionNode) GetHash() (common.Hash, bool) {
	return n.hash, n.hashDirty
}

func (n *ExtensionNode) SetHash(hash common.Hash) {
	n.hash = hash
	n.hashDirty = false
}

func (n *ExtensionNode) IsFrozen() bool {
	return n.frozen
}

func (n *ExtensionNode) MarkFrozen() {
	n.frozen = true
}

func (n *ExtensionNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	handle, err := manager.getWriteAccess(&n.next)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Freeze(manager, handle)
}

func (n *ExtensionNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble, ancestors []NodeId) error {
	// Checked invariants:
	//  - extension path have a length > 0
	//  - extension can only be followed by a branch
	//  - sub-trie is correct
	//  - hash of sub-tree is either dirty or correct
	if slices.Contains(ancestors, thisRef.Id()) {
		return fmt.Errorf("node cycle detected: %v in %v", thisRef.Id(), ancestors)
	}
	ancestors = append(ancestors, thisRef.Id())
	errs := []error{}
	if n.path.Length() <= 0 {
		errs = append(errs, fmt.Errorf("node %v - extension path must not be empty", thisRef.Id()))
	}
	if !n.next.Id().IsBranch() {
		errs = append(errs, fmt.Errorf("node %v - extension path must be followed by a branch", thisRef.Id()))
	}
	if handle, err := source.getViewAccess(&n.next); err == nil {
		defer handle.Release()
		extended := path
		for i := 0; i < n.path.Length(); i++ {
			extended = append(extended, n.path.Get(i))
		}
		if err := handle.Get().Check(source, &n.next, extended, ancestors); err != nil {
			errs = append(errs, err)
		}
	} else {
		errs = append(errs, err)
	}
	if !n.nextHashDirty && !n.nextIsEmbedded {
		want, err := source.getHashFor(&n.next)
		if err != nil {
			errs = append(errs, err)
		} else if want != n.nextHash {
			errs = append(errs, fmt.Errorf("node %v - next node hash invalid\nwant: %v\ngot: %v", thisRef.Id(), want, n.nextHash))
		}
	}
	return errors.Join(errs...)
}

func (n *ExtensionNode) Dump(source NodeSource, thisRef *NodeReference, indent string) {
	fmt.Printf("%sExtension (ID: %v/%t, dirtyHash: %t, Embedded: %t, Hash: %v, dirtyHash: %t): %v\n", indent, thisRef.Id(), n.frozen, n.nextHashDirty, n.nextIsEmbedded, formatHashForDump(n.hash), n.hashDirty, &n.path)
	if handle, err := source.getViewAccess(&n.next); err == nil {
		defer handle.Release()
		handle.Get().Dump(source, &n.next, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.next, err)
	}
}

func (n *ExtensionNode) Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (bool, error) {
	response := visitor.Visit(n, NodeInfo{Id: thisRef.Id(), Depth: &depth})
	switch response {
	case VisitResponseAbort:
		return true, nil
	case VisitResponsePrune:
		return false, nil
	}
	if handle, err := source.getViewAccess(&n.next); err == nil {
		defer handle.Release()
		return handle.Get().Visit(source, &n.next, depth+1, visitor)
	} else {
		return false, err
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
	address          common.Address
	info             AccountInfo
	storage          NodeReference
	storageHash      common.Hash
	storageHashDirty bool
	frozen           bool
	// pathLength is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
	hash       common.Hash // the hash of this node (may be dirty)
	hashDirty  bool        // indicating whether this node's hash is dirty
}

func (n *AccountNode) Address() common.Address {
	return n.address
}

func (n *AccountNode) Info() AccountInfo {
	return n.info
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
	subPath := KeyToNibblePath(key, source)
	root, err := source.getReadAccess(&n.storage)
	if err != nil {
		return common.Value{}, false, err
	}
	defer root.Release()
	return root.Get().GetValue(source, key, subPath[:])
}

func (n *AccountNode) SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (NodeReference, bool, error) {
	// Check whether this is the correct account.
	if n.address == address {
		if info == n.info {
			return *thisRef, false, nil
		}
		if info.IsEmpty() {
			// TODO: test this
			if n.frozen {
				return NewNodeReference(EmptyId()), false, nil
			}
			// Recursively release the entire state DB.
			// TODO: consider performing this asynchronously.
			root, err := manager.getWriteAccess(&n.storage)
			if err != nil {
				return NodeReference{}, false, err
			}
			defer root.Release()
			err = root.Get().Release(manager, &n.storage, root)
			if err != nil {
				return NodeReference{}, false, err
			}
			// Release this account node and remove it from the trie.
			manager.release(thisRef.Id())
			return NewNodeReference(EmptyId()), true, nil
		}

		// If this node is frozen, we need to write the result in
		// a new account node.
		if n.frozen {
			newRef, handle, err := manager.createAccount()
			if err != nil {
				return NodeReference{}, false, err
			}
			defer handle.Release()
			newNode := handle.Get().(*AccountNode)
			*newNode = *n
			newNode.frozen = false
			newNode.info = info
			newNode.hashDirty = true
			manager.update(newRef.Id(), handle)
			return newRef, false, nil
		}

		n.info = info
		n.hashDirty = true
		manager.update(thisRef.Id(), this)
		return *thisRef, true, nil
	}

	// Skip restructuring the tree if the new info is empty.
	if info.IsEmpty() {
		return *thisRef, false, nil
	}

	// Create a new node for the sibling to be added.
	siblingRef, handle, err := manager.createAccount()
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	sibling := handle.Get().(*AccountNode)
	sibling.address = address
	sibling.info = info
	sibling.hashDirty = true

	thisPath := AddressToNibblePath(n.address, manager)
	newRoot, err := splitLeafNode(manager, thisRef, thisPath[:], n, this, path, &siblingRef, sibling, handle)
	return newRoot, false, err
}

type leafNode interface {
	Node
	setPathLength(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], length byte) (newRoot NodeReference, changed bool, err error)
}

func splitLeafNode(
	manager NodeManager,
	thisRef *NodeReference,
	thisPath []Nibble,
	this leafNode,
	thisHandle shared.WriteHandle[Node],
	siblingPath []Nibble,
	siblingRef *NodeReference,
	sibling leafNode,
	siblingHandle shared.WriteHandle[Node],
) (NodeReference, error) {
	// This single node needs to be split into
	//  - an optional common prefix extension
	//  - a branch node linking this node and
	//  - a new sibling account node to be returned

	branchRef, branchHandle, err := manager.createBranch()
	if err != nil {
		return NodeReference{}, err
	}
	defer branchHandle.Release()
	branch := branchHandle.Get().(*BranchNode)
	newRoot := branchRef

	// Check whether there is a common prefix.
	partialPath := thisPath[len(thisPath)-len(siblingPath):]
	commonPrefixLength := GetCommonPrefixLength(partialPath, siblingPath)
	if commonPrefixLength > 0 {
		extensionRef, handle, err := manager.createExtension()
		if err != nil {
			return NodeReference{}, err
		}
		defer handle.Release()
		extension := handle.Get().(*ExtensionNode)
		newRoot = extensionRef

		extension.path = CreatePathFromNibbles(siblingPath[0:commonPrefixLength])
		extension.next = branchRef
		extension.hashDirty = true
		extension.nextHashDirty = true
		manager.update(extensionRef.Id(), handle)
	}

	// If enabled, keep track of the suffix length of leaf values.
	remainingPathLength := byte(len(partialPath)-commonPrefixLength) - 1
	if manager.getConfig().TrackSuffixLengthsInLeafNodes {
		sibling.setPathLength(manager, siblingRef, siblingHandle, remainingPathLength)
		ref, _, err := this.setPathLength(manager, thisRef, thisHandle, remainingPathLength)
		if err != nil {
			return NodeReference{}, err
		}
		thisRef = &ref
	} else {
		// Commit the changes to the sibling.
		manager.update(siblingRef.Id(), siblingHandle)
	}

	// Add this node and the new sibling node to the branch node.
	branch.children[partialPath[commonPrefixLength]] = *thisRef
	branch.children[siblingPath[commonPrefixLength]] = *siblingRef
	branch.markChildHashDirty(byte(partialPath[commonPrefixLength]))
	branch.markChildHashDirty(byte(siblingPath[commonPrefixLength]))
	branch.hashDirty = true

	// Commit the changes to the the branch node.
	manager.update(branchRef.Id(), branchHandle)

	return newRoot, nil
}

func (n *AccountNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	return NodeReference{}, false, fmt.Errorf("setValue call should not reach account nodes")
}

func (n *AccountNode) SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (NodeReference, bool, error) {
	// If this is not the correct account, the real account does not exist
	// and the insert can be skipped. The insertion of a slot value shall
	// not create an account.
	if n.address != address {
		return *thisRef, false, nil
	}

	// Continue from here with a value insertion.
	handle, err := manager.getWriteAccess(&n.storage)
	if err != nil {
		return NodeReference{}, false, err
	}
	defer handle.Release()
	subPath := KeyToNibblePath(key, manager)
	root, hasChanged, err := handle.Get().SetValue(manager, &n.storage, handle, key, subPath[:], value)
	if err != nil {
		return NodeReference{}, false, err
	}
	if root != n.storage {
		// If this node is frozen, we need to write the result in
		// a new account node.
		if n.frozen {
			newRef, newHandle, err := manager.createAccount()
			if err != nil {
				return NodeReference{}, false, err
			}
			defer newHandle.Release()
			newNode := newHandle.Get().(*AccountNode)
			*newNode = *n
			newNode.frozen = false
			newNode.storage = root
			newNode.storageHashDirty = true
			newNode.hashDirty = true
			manager.update(newRef.Id(), newHandle)
			return newRef, false, nil
		}
		n.storage = root
		n.storageHashDirty = true
		n.hashDirty = true
		hasChanged = true
		manager.update(thisRef.Id(), this)
	} else if hasChanged {
		n.hashDirty = true
		n.storageHashDirty = true
		manager.update(thisRef.Id(), this)
	}
	return *thisRef, hasChanged, nil
}

func (n *AccountNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, changed bool, err error) {
	if n.address != address || n.storage.Id().IsEmpty() {
		return *thisRef, false, nil
	}

	// If this node is frozen, we need to write the result in
	// a new account node.
	if n.frozen {
		newRef, newHandle, err := manager.createAccount()
		if err != nil {
			return *thisRef, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*AccountNode)
		*newNode = *n
		newNode.frozen = false
		newNode.storage = NewNodeReference(EmptyId())
		newNode.storageHashDirty = true
		newNode.hashDirty = true
		manager.update(newRef.Id(), newHandle)
		return newRef, false, nil
	}

	rootHandle, err := manager.getWriteAccess(&n.storage)
	if err != nil {
		return *thisRef, false, err
	}
	defer rootHandle.Release()

	err = rootHandle.Get().Release(manager, &n.storage, rootHandle)
	n.storage = NewNodeReference(EmptyId())
	n.storageHashDirty = true
	n.hashDirty = true
	return *thisRef, true, err
}

func (n *AccountNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	if !n.storage.Id().IsEmpty() {
		rootHandle, err := manager.getWriteAccess(&n.storage)
		if err != nil {
			return err
		}
		err = rootHandle.Get().Release(manager, &n.storage, rootHandle)
		rootHandle.Release()
		if err != nil {
			return err
		}
	}
	return manager.release(thisRef.Id())
}

func (n *AccountNode) GetHash() (common.Hash, bool) {
	return n.hash, n.hashDirty
}

func (n *AccountNode) SetHash(hash common.Hash) {
	n.hash = hash
	n.hashDirty = false
}

func (n *AccountNode) setPathLength(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], length byte) (NodeReference, bool, error) {
	if n.pathLength == length {
		return *thisRef, false, nil
	}
	if n.frozen {
		newRef, newHandle, err := manager.createAccount()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*AccountNode)
		*newNode = *n
		newNode.frozen = false
		newNode.pathLength = length
		newNode.hashDirty = true
		return newRef, false, manager.update(newRef.Id(), newHandle)
	}

	n.hashDirty = true
	n.pathLength = length
	return *thisRef, true, manager.update(thisRef.Id(), this)
}

func (n *AccountNode) IsFrozen() bool {
	return n.frozen
}

func (n *AccountNode) MarkFrozen() {
	n.frozen = true
}

func (n *AccountNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	n.frozen = true
	handle, err := manager.getWriteAccess(&n.storage)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Freeze(manager, handle)
}

func (n *AccountNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble, ancestors []NodeId) error {
	// Checked invariants:
	//  - account information must not be empty
	//  - the account is at a correct position in the trie
	//  - state sub-trie is correct
	//  - path length
	if slices.Contains(ancestors, thisRef.Id()) {
		return fmt.Errorf("node cycle detected: %v in %v", thisRef.Id(), ancestors)
	}
	ancestors = append(ancestors, thisRef.Id())
	errs := []error{}

	fullPath := AddressToNibblePath(n.address, source)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("node %v - account node %v located in wrong branch: %v", thisRef.Id(), n.address, path))
	}

	if n.info.IsEmpty() {
		errs = append(errs, fmt.Errorf("node %v - account information must not be empty", thisRef.Id()))
	}

	if !n.storage.Id().IsEmpty() {
		if node, err := source.getViewAccess(&n.storage); err == nil {
			defer node.Release()
			if err := node.Get().Check(source, &n.storage, make([]Nibble, 0, common.KeySize*2), ancestors); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, err)
		}
	}

	if source.getConfig().TrackSuffixLengthsInLeafNodes {
		maxPathLength := 40
		if source.getConfig().UseHashedPaths {
			maxPathLength = 64
		}
		if got, want := n.pathLength, byte(maxPathLength-len(path)); got != want {
			errs = append(errs, fmt.Errorf("node %v - invalid path length, wanted %d, got %d", thisRef.Id(), want, got))
		}
	}

	return errors.Join(errs...)
}

func (n *AccountNode) Dump(source NodeSource, thisRef *NodeReference, indent string) {
	fmt.Printf("%sAccount (ID: %v/%t/%v, Hash: %v, dirtyHash: %t): %v - %v\n", indent, thisRef.Id(), n.frozen, n.pathLength, formatHashForDump(n.hash), n.hashDirty, n.address, n.info)
	if n.storage.Id().IsEmpty() {
		return
	}
	if node, err := source.getViewAccess(&n.storage); err == nil {
		defer node.Release()
		node.Get().Dump(source, &n.storage, indent+"  ")
	} else {
		fmt.Printf("%s  ERROR: unable to load node %v: %v", indent, n.storage, err)
	}
}

func (n *AccountNode) Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (bool, error) {
	response := visitor.Visit(n, NodeInfo{Id: thisRef.Id(), Depth: &depth})
	switch response {
	case VisitResponseAbort:
		return true, nil
	case VisitResponsePrune:
		return false, nil
	}
	if n.storage.Id().IsEmpty() {
		return false, nil
	}
	if node, err := source.getViewAccess(&n.storage); err == nil {
		defer node.Release()
		return node.Get().Visit(source, &n.storage, depth+1, visitor)
	} else {
		return false, err
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
	// pathLength is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
	hash       common.Hash // the hash of this node (may be dirty)
	hashDirty  bool        // indicating whether this node's hash is dirty
}

func (n *ValueNode) Key() common.Key {
	return n.key
}

func (n *ValueNode) Value() common.Value {
	return n.value
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

func (n *ValueNode) SetAccount(NodeManager, *NodeReference, shared.WriteHandle[Node], common.Address, []Nibble, AccountInfo) (NodeReference, bool, error) {
	return NodeReference{}, false, fmt.Errorf("invalid request: account update should not reach values")
}

func (n *ValueNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	// Check whether this is the correct value node.
	if n.key == key {
		if value == n.value {
			return *thisRef, false, nil
		}
		if value == (common.Value{}) {
			if !n.frozen {
				manager.release(thisRef.Id())
			}
			return NewNodeReference(EmptyId()), !n.frozen, nil
		}
		if n.frozen {
			newRef, newHandle, err := manager.createValue()
			if err != nil {
				return NodeReference{}, false, nil
			}
			defer newHandle.Release()
			newNode := newHandle.Get().(*ValueNode)
			newNode.key = n.key
			newNode.value = value
			newNode.hashDirty = true
			newNode.pathLength = n.pathLength
			manager.update(newRef.Id(), newHandle)
			return newRef, false, nil
		}
		n.value = value
		n.hashDirty = true
		manager.update(thisRef.Id(), this)
		return *thisRef, true, nil
	}

	// Skip restructuring the tree if the new info is empty.
	if value == (common.Value{}) {
		return *thisRef, false, nil
	}

	// Create a new node for the sibling to be added.
	siblingRef, siblingHandle, err := manager.createValue()
	if err != nil {
		return NodeReference{}, false, err
	}
	defer siblingHandle.Release()
	sibling := siblingHandle.Get().(*ValueNode)
	sibling.key = key
	sibling.value = value
	sibling.hashDirty = true

	thisPath := KeyToNibblePath(n.key, manager)
	newRootId, err := splitLeafNode(manager, thisRef, thisPath[:], n, this, path, &siblingRef, sibling, siblingHandle)
	return newRootId, false, err
}

func (n *ValueNode) SetSlot(NodeManager, *NodeReference, shared.WriteHandle[Node], common.Address, []Nibble, common.Key, common.Value) (NodeReference, bool, error) {
	return NodeReference{}, false, fmt.Errorf("invalid request: slot update should not reach values")
}

func (n *ValueNode) ClearStorage(NodeManager, *NodeReference, shared.WriteHandle[Node], common.Address, []Nibble) (NodeReference, bool, error) {
	return NodeReference{}, false, fmt.Errorf("invalid request: clear storage should not reach values")
}

func (n *ValueNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.frozen {
		return nil
	}
	return manager.release(thisRef.Id())
}

func (n *ValueNode) GetHash() (common.Hash, bool) {
	return n.hash, n.hashDirty
}

func (n *ValueNode) SetHash(hash common.Hash) {
	n.hash = hash
	n.hashDirty = false
}

func (n *ValueNode) setPathLength(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], length byte) (NodeReference, bool, error) {
	if n.pathLength == length {
		return *thisRef, false, nil
	}
	if n.frozen {
		newRef, newHandle, err := manager.createValue()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*ValueNode)
		newNode.key = n.key
		newNode.value = n.value
		newNode.hashDirty = true
		newNode.pathLength = length
		return newRef, false, manager.update(newRef.Id(), newHandle)
	}

	n.hashDirty = true
	n.pathLength = length
	return *thisRef, true, manager.update(thisRef.Id(), this)
}

func (n *ValueNode) IsFrozen() bool {
	return n.frozen
}

func (n *ValueNode) MarkFrozen() {
	n.frozen = true
}

func (n *ValueNode) Freeze(NodeManager, shared.WriteHandle[Node]) error {
	n.frozen = true
	return nil
}

func (n *ValueNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble, ancestors []NodeId) error {
	// Checked invariants:
	//  - value must not be empty
	//  - values are in the right position of the trie
	//  - the path length is correct (if enabled to be tracked)
	if slices.Contains(ancestors, thisRef.Id()) {
		return fmt.Errorf("node cycle detected: %v in %v", thisRef.Id(), ancestors)
	}
	errs := []error{}

	fullPath := KeyToNibblePath(n.key, source)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("node %v - value node %v located in wrong branch: %v", thisRef.Id(), n.key, path))
	}

	if n.value == (common.Value{}) {
		errs = append(errs, fmt.Errorf("node %v - value slot must not be empty", thisRef.Id()))
	}

	if source.getConfig().TrackSuffixLengthsInLeafNodes {
		if got, want := n.pathLength, byte(64-len(path)); got != want {
			errs = append(errs, fmt.Errorf("node %v - invalid path length, wanted %d, got %d", thisRef.Id(), want, got))
		}
	}

	return errors.Join(errs...)
}

func (n *ValueNode) Dump(source NodeSource, thisRef *NodeReference, indent string) {
	fmt.Printf("%sValue (ID: %v/%t/%d, Hash: %v, dirtyHash: %t): %v - %v\n", indent, thisRef.Id(), n.frozen, n.pathLength, formatHashForDump(n.hash), n.hashDirty, n.key, n.value)
}

func formatHashForDump(hash common.Hash) string {
	return fmt.Sprintf("0x%x", hash)
}

func (n *ValueNode) Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (bool, error) {
	return visitor.Visit(n, NodeInfo{Id: thisRef.Id(), Depth: &depth}) == VisitResponseAbort, nil
}

// ----------------------------------------------------------------------------
//                               Node Encoders
// ----------------------------------------------------------------------------

// TODO: move encoder to extra file and clean-up definitions

type BranchNodeEncoderWithNodeHash struct{}

func (BranchNodeEncoderWithNodeHash) GetEncodedSize() int {
	encoder := NodeIdEncoder{}
	return encoder.GetEncodedSize()*16 + common.HashSize
}

func (BranchNodeEncoderWithNodeHash) Store(dst []byte, node *BranchNode) error {
	if node.hashDirty {
		panic("unable to store branch node with dirty hash")
	}
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		if err := encoder.Store(dst[i*step:], &node.children[i].id); err != nil {
			return err
		}
	}
	dst = dst[step*16:]
	copy(dst, node.hash[:])
	return nil
}

func (BranchNodeEncoderWithNodeHash) Load(src []byte, node *BranchNode) error {
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		var id NodeId
		if err := encoder.Load(src[i*step:], &id); err != nil {
			return err
		}
		node.children[i] = NewNodeReference(id)
	}
	src = src[step*16:]
	copy(node.hash[:], src)

	// The hashes of the child nodes are not stored with the node, so they are
	// marked as dirty to trigger a re-computation the next time they are used.
	for i := 0; i < 16; i++ {
		if !node.children[i].Id().IsEmpty() {
			node.markChildHashDirty(byte(i))
		}
	}

	return nil
}

type BranchNodeEncoderWithChildHashes struct{}

func (BranchNodeEncoderWithChildHashes) GetEncodedSize() int {
	encoder := NodeIdEncoder{}
	return encoder.GetEncodedSize()*16 + common.HashSize*16 + 2
}

func (BranchNodeEncoderWithChildHashes) Store(dst []byte, node *BranchNode) error {
	if node.dirtyHashes != 0 {
		panic("unable to store branch node with dirty hash")
	}
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		if err := encoder.Store(dst[i*step:], &node.children[i].id); err != nil {
			return err
		}
	}
	dst = dst[step*16:]
	for i := 0; i < 16; i++ {
		copy(dst, node.hashes[i][:])
		dst = dst[common.HashSize:]
	}
	binary.BigEndian.PutUint16(dst, node.embeddedChildren)
	return nil
}

func (BranchNodeEncoderWithChildHashes) Load(src []byte, node *BranchNode) error {
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		var id NodeId
		if err := encoder.Load(src[i*step:], &id); err != nil {
			return err
		}
		node.children[i] = NewNodeReference(id)
	}
	src = src[step*16:]
	for i := 0; i < 16; i++ {
		copy(node.hashes[i][:], src)
		src = src[common.HashSize:]
	}
	node.embeddedChildren = binary.BigEndian.Uint16(src)

	// The node's hash is not stored with the node, so it is marked dirty.
	node.hashDirty = true

	return nil
}

type ExtensionNodeEncoderWithNodeHash struct{}

func (ExtensionNodeEncoderWithNodeHash) GetEncodedSize() int {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	return pathEncoder.GetEncodedSize() + idEncoder.GetEncodedSize() + common.HashSize
}

func (ExtensionNodeEncoderWithNodeHash) Store(dst []byte, value *ExtensionNode) error {
	if value.hashDirty {
		panic("unable to store extension node with dirty hash")
	}
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Store(dst, &value.path); err != nil {
		return err
	}
	dst = dst[pathEncoder.GetEncodedSize():]
	if err := idEncoder.Store(dst, &value.next.id); err != nil {
		return err
	}
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst, value.hash[:])
	return nil
}

func (ExtensionNodeEncoderWithNodeHash) Load(src []byte, node *ExtensionNode) error {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Load(src, &node.path); err != nil {
		return err
	}
	src = src[pathEncoder.GetEncodedSize():]
	var id NodeId
	if err := idEncoder.Load(src, &id); err != nil {
		return err
	}
	node.next = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.hash[:], src)

	// The hash of the next node is not stored with the node, so it is marked
	// as dirty to trigger a re-computation the next time it is accessed.
	node.nextHashDirty = true

	return nil
}

type ExtensionNodeEncoderWithChildHash struct{}

func (ExtensionNodeEncoderWithChildHash) GetEncodedSize() int {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	return pathEncoder.GetEncodedSize() + idEncoder.GetEncodedSize() + common.HashSize + 1
}

func (ExtensionNodeEncoderWithChildHash) Store(dst []byte, value *ExtensionNode) error {
	if value.nextHashDirty {
		panic("unable to store extension node with dirty hash")
	}
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Store(dst, &value.path); err != nil {
		return err
	}
	dst = dst[pathEncoder.GetEncodedSize():]
	if err := idEncoder.Store(dst, &value.next.id); err != nil {
		return err
	}
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst, value.nextHash[:])
	dst = dst[common.HashSize:]
	if value.nextIsEmbedded {
		dst[0] = 1
	} else {
		dst[0] = 0
	}
	return nil
}

func (ExtensionNodeEncoderWithChildHash) Load(src []byte, node *ExtensionNode) error {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	if err := pathEncoder.Load(src, &node.path); err != nil {
		return err
	}
	src = src[pathEncoder.GetEncodedSize():]
	var id NodeId
	if err := idEncoder.Load(src, &id); err != nil {
		return err
	}
	node.next = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.nextHash[:], src)
	src = src[common.HashSize:]
	node.nextIsEmbedded = src[0] != 0

	// The node's hash is not stored with the node, so it is marked dirty.
	node.hashDirty = true

	return nil
}

type AccountNodeEncoderWithNodeHash struct{}

func (AccountNodeEncoderWithNodeHash) GetEncodedSize() int {
	return common.AddressSize +
		AccountInfoEncoder{}.GetEncodedSize() +
		NodeIdEncoder{}.GetEncodedSize() +
		common.HashSize
}

func (AccountNodeEncoderWithNodeHash) Store(dst []byte, node *AccountNode) error {
	if node.hashDirty {
		panic("unable to store account node with dirty hash")
	}
	copy(dst, node.address[:])
	dst = dst[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Store(dst, &node.info); err != nil {
		return err
	}
	dst = dst[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	if err := idEncoder.Store(dst, &node.storage.id); err != nil {
		return err
	}
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst[:], node.hash[:])
	return nil
}

func (AccountNodeEncoderWithNodeHash) Load(src []byte, node *AccountNode) error {
	copy(node.address[:], src)
	src = src[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Load(src, &node.info); err != nil {
		return err
	}
	src = src[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	var id NodeId
	if err := idEncoder.Load(src, &id); err != nil {
		return err
	}
	node.storage = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.hash[:], src)

	// The storage hash is not stored with the node, so it is marked as dirty to
	// trigger a re-computation the next time it is accessed.
	node.storageHashDirty = true

	return nil
}

type AccountNodeEncoderWithChildHash struct{}

func (AccountNodeEncoderWithChildHash) GetEncodedSize() int {
	return common.AddressSize +
		AccountInfoEncoder{}.GetEncodedSize() +
		NodeIdEncoder{}.GetEncodedSize() +
		common.HashSize
}

func (AccountNodeEncoderWithChildHash) Store(dst []byte, node *AccountNode) error {
	if node.storageHashDirty {
		panic("unable to store account node with dirty hash")
	}
	copy(dst, node.address[:])
	dst = dst[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Store(dst, &node.info); err != nil {
		return err
	}
	dst = dst[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	if err := idEncoder.Store(dst, &node.storage.id); err != nil {
		return err
	}
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst[:], node.storageHash[:])
	return nil
}

func (AccountNodeEncoderWithChildHash) Load(src []byte, node *AccountNode) error {
	copy(node.address[:], src)
	src = src[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	if err := infoEncoder.Load(src, &node.info); err != nil {
		return err
	}
	src = src[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	var id NodeId
	if err := idEncoder.Load(src, &id); err != nil {
		return err
	}
	node.storage = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.storageHash[:], src)

	// The node's hash is not stored with the node, so it is marked dirty.
	node.hashDirty = true

	return nil
}

type AccountNodeWithPathLengthEncoderWithNodeHash struct{}

func (AccountNodeWithPathLengthEncoderWithNodeHash) GetEncodedSize() int {
	return AccountNodeEncoderWithNodeHash{}.GetEncodedSize() + 1
}

func (AccountNodeWithPathLengthEncoderWithNodeHash) Store(dst []byte, node *AccountNode) error {
	AccountNodeEncoderWithNodeHash{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (AccountNodeWithPathLengthEncoderWithNodeHash) Load(src []byte, node *AccountNode) error {
	AccountNodeEncoderWithNodeHash{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}

type AccountNodeWithPathLengthEncoderWithChildHash struct{}

func (AccountNodeWithPathLengthEncoderWithChildHash) GetEncodedSize() int {
	return AccountNodeEncoderWithChildHash{}.GetEncodedSize() + 1
}

func (AccountNodeWithPathLengthEncoderWithChildHash) Store(dst []byte, node *AccountNode) error {
	AccountNodeEncoderWithChildHash{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (AccountNodeWithPathLengthEncoderWithChildHash) Load(src []byte, node *AccountNode) error {
	AccountNodeEncoderWithChildHash{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}

type ValueNodeEncoderWithoutNodeHash struct{}

func (ValueNodeEncoderWithoutNodeHash) GetEncodedSize() int {
	return common.KeySize + common.ValueSize
}

func (ValueNodeEncoderWithoutNodeHash) Store(dst []byte, node *ValueNode) error {
	copy(dst, node.key[:])
	dst = dst[len(node.key):]
	copy(dst, node.value[:])
	return nil
}

func (ValueNodeEncoderWithoutNodeHash) Load(src []byte, node *ValueNode) error {
	copy(node.key[:], src)
	src = src[len(node.key):]
	copy(node.value[:], src)

	// The node's hash is not stored with the node, so it is marked dirty.
	node.hashDirty = true

	return nil
}

type ValueNodeEncoderWithNodeHash struct{}

func (ValueNodeEncoderWithNodeHash) GetEncodedSize() int {
	return ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize() + common.HashSize
}

func (ValueNodeEncoderWithNodeHash) Store(dst []byte, node *ValueNode) error {
	ValueNodeEncoderWithoutNodeHash{}.Store(dst, node)
	dst = dst[ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize():]
	copy(dst, node.hash[:])
	return nil
}

func (ValueNodeEncoderWithNodeHash) Load(src []byte, node *ValueNode) error {
	ValueNodeEncoderWithoutNodeHash{}.Load(src, node)
	src = src[ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize():]
	copy(node.hash[:], src)
	node.hashDirty = false
	return nil
}

type ValueNodeWithPathLengthEncoderWithoutNodeHash struct{}

func (ValueNodeWithPathLengthEncoderWithoutNodeHash) GetEncodedSize() int {
	return ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize() + 1
}

func (ValueNodeWithPathLengthEncoderWithoutNodeHash) Store(dst []byte, node *ValueNode) error {
	ValueNodeEncoderWithoutNodeHash{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (ValueNodeWithPathLengthEncoderWithoutNodeHash) Load(src []byte, node *ValueNode) error {
	ValueNodeEncoderWithoutNodeHash{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}

type ValueNodeWithPathLengthEncoderWithNodeHash struct{}

func (ValueNodeWithPathLengthEncoderWithNodeHash) GetEncodedSize() int {
	return ValueNodeEncoderWithNodeHash{}.GetEncodedSize() + 1
}

func (ValueNodeWithPathLengthEncoderWithNodeHash) Store(dst []byte, node *ValueNode) error {
	ValueNodeEncoderWithNodeHash{}.Store(dst, node)
	dst[len(dst)-1] = node.pathLength
	return nil
}

func (ValueNodeWithPathLengthEncoderWithNodeHash) Load(src []byte, node *ValueNode) error {
	ValueNodeEncoderWithNodeHash{}.Load(src, node)
	node.pathLength = src[len(src)-1]
	return nil
}
