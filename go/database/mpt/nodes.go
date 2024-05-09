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

//go:generate mockgen -source nodes.go -destination nodes_mocks.go -package mpt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"io"
	"slices"
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

	// IsDirty returns whether this node's state is different in memory than it
	// is on disk. All nodes are created dirty and may only be cleaned by marking
	// them as such.
	IsDirty() bool

	// MarkClean marks this node as clean. This function should be called when an
	// in memory version of a node got synced with its on-disk copy.
	MarkClean()

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

	// Check verifies internal invariants of this node. It is mainly intended
	// to validate invariants in unit tests and for issue diagnostics.
	Check(source NodeSource, thisRef *NodeReference, path []Nibble) error

	// Dump dumps this node and its sub-trees to the console. It is mainly
	// intended for debugging and may be very costly for larger instances.
	Dump(dest io.Writer, source NodeSource, thisRef *NodeReference, indent string) error

	// Visit visits this and all nodes in the respective sub-tree. The
	// visitor is called by each encountered node, with the proper NodeInfo
	// set. Visiting aborts if the visitor returns or prune sub-tree as
	// requested by the visitor. The function returns whether the visiting
	// process has been aborted and/or an error occurred.
	Visit(source NodeSource, thisRef *NodeReference, depth int, visitor NodeVisitor) (abort bool, err error)
}

// NodeSource is an interface for any object capable of resolving NodeIds into
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

	release(*NodeReference) error
	releaseTrieAsynchronous(NodeReference)
}

// ----------------------------------------------------------------------------
//                               Utilities
// ----------------------------------------------------------------------------

// VisitPathToStorage visits all nodes from the input storage root following the input storage key.
// Each encountered node is passed to the visitor.
// If no more nodes are available on the path, the execution ends.
// If the key does not exist, the function returns false.
// The function returns an error if the path cannot be iterated due to error propagated from the node source.
// Nodes provided via the visitor are made available with the view privilege.
func VisitPathToStorage(source NodeSource, storageRoot *NodeReference, key common.Key, visitor NodeVisitor) (bool, error) {
	path := KeyToNibblePath(key, source)
	return visitPathTo(source, storageRoot, path, nil, &key, visitor)
}

// VisitPathToAccount visits all nodes from the input root following the input account address.
// Each encountered node is passed to the visitor.
// If no more nodes are available on the path, the execution ends.
// If the account address does not exist, the function returns false.
// The function returns an error if the path cannot be iterated due to error propagated from the node source.
// Nodes provided via the visitor are made available with the view privilege.
func VisitPathToAccount(source NodeSource, root *NodeReference, address common.Address, visitor NodeVisitor) (bool, error) {
	path := AddressToNibblePath(address, source)
	return visitPathTo(source, root, path, &address, nil, visitor)
}

// visitPathTo visits all nodes from the input root following the input path.
// Each encountered node is passed to the visitor.
// If no more nodes are available on the path, the execution ends.
// If the path does not exist, the function returns false.
// The function returns an error if the path cannot be iterated due to error propagated from the node source.
// When the function reaches either an account node or a value node it is compared to the input address or key.
// If either the address or key matches the node, this function terminates.
// It means this function can be used to find either an account node or a value node,
// but it cannot find both at the same time.
func visitPathTo(source NodeSource, root *NodeReference, path []Nibble, address *common.Address, key *common.Key, visitor NodeVisitor) (bool, error) {
	nodeId := root

	var last shared.ViewHandle[Node]
	var found, done bool
	var lastNodeId *NodeReference
	for !done {
		handle, err := source.getViewAccess(nodeId)
		if last.Valid() {
			last.Release()
		}
		if err != nil {
			return false, err
		}
		last = handle
		lastNodeId = nodeId
		node := handle.Get()

		switch n := node.(type) {
		case *ExtensionNode:
			if n.path.IsPrefixOf(path) {
				nodeId = &n.next
				path = path[n.path.Length():]
			} else {
				done = true
			}
		case *BranchNode:
			if len(path) == 0 {
				done = true
			} else {
				nodeId = &n.children[path[0]]
				path = path[1:]
			}
		case *AccountNode:
			if address != nil && n.address == *address {
				found = true
			}
			done = true
		case *ValueNode:
			if key != nil && n.key == *key {
				found = true
			}
			done = true
		default:
			done = true
		}

		// visit when we are in the middle of the path or when we found the result
		if !done || found {
			if res := visitor.Visit(last.Get(), NodeInfo{Id: lastNodeId.Id()}); res != VisitResponseContinue {
				done = true
			}
		}
	}

	last.Release()
	return found, nil
}

// CheckForest evaluates invariants throughout all nodes reachable from the
// given list of roots. Executed checks include node-specific checks like the
// minimum number of child nodes of a BranchNode, the correct placement of
// nodes within the forest, and the absence of zero values. The function also
// checks the proper sharing of nodes in multiple tries rooted by different
// nodes. A reuse is only valid if the node's position within the respective
// tries is compatible -- thus, the node is reachable through the same
// navigation path.
func CheckForest(source NodeSource, roots []*NodeReference) error {
	// The check algorithm is based on an iterative depth-first traversal
	// where information on encountered nodes is cached to avoid multiple
	// evaluations.
	workList := []NodeId{}
	contexts := map[NodeId]nodeCheckContext{}
	for _, ref := range roots {
		workList = append(workList, ref.Id())
		contexts[ref.Id()] = nodeCheckContext{
			root:           ref.Id(),
			hasSeenAccount: false,
			path:           nil,
		}
	}

	// scheduleNode verifies that the given node is reached consistently
	// with earlier encounters or schedules the node for future checks
	// if this is the first time a path to this node was discovered.
	scheduleNode := func(ref *NodeReference, root NodeId, accountSeen bool, path []Nibble) error {
		context := nodeCheckContext{
			root:           root,
			hasSeenAccount: accountSeen,
			path:           path,
		}
		previous, found := contexts[ref.Id()]
		if found {
			if !context.isCompatible(&previous) {
				return fmt.Errorf(
					"invalid reuse of node %v: reachable from %v through %v and from %v through %v",
					ref.Id(), previous.root, previous.path, context.root, context.path,
				)
			}
			return nil
		} else {
			contexts[ref.Id()] = context
		}
		workList = append(workList, ref.Id())
		return nil
	}

	count := 0
	for len(workList) > 0 {
		curId := workList[len(workList)-1]
		workList = workList[:len(workList)-1]

		// TODO [cleanup]: replace this by an observer
		count++
		if count%100000 == 0 {
			fmt.Printf("Checking %v (%d), |ws| = %d, |contexts| = %d\n", curId, count, len(workList), len(contexts))
		}

		context := contexts[curId]
		curNodeRef := NewNodeReference(curId)
		handle, err := source.getViewAccess(&curNodeRef)
		if err != nil {
			return err
		}
		node := handle.Get()
		err = node.Check(source, &curNodeRef, context.path)
		if err != nil {
			handle.Release()
			return err
		}

		// schedule child nodes to be checked
		switch cur := node.(type) {
		case EmptyNode:
			// terminal node without children
		case *AccountNode:
			storage := cur.storage
			if !storage.id.IsEmpty() {
				err = scheduleNode(&storage, context.root, true, nil)
			}
		case *BranchNode:
			for i := 0; i < 16; i++ {
				child := cur.children[i]
				if !child.id.IsEmpty() {
					path := make([]Nibble, len(context.path)+1)
					copy(path, context.path)
					path[len(context.path)] = Nibble(i)
					if err = scheduleNode(&child, context.root, context.hasSeenAccount, path); err != nil {
						break
					}
				}
			}
		case *ExtensionNode:
			next := cur.next
			if !next.id.IsEmpty() {
				path := make([]Nibble, len(context.path), len(context.path)+cur.path.Length())
				copy(path, context.path)
				for i := 0; i < cur.path.Length(); i++ {
					path = append(path, cur.path.Get(i))
				}
				err = scheduleNode(&next, context.root, context.hasSeenAccount, path)
			}
		case *ValueNode:
			// terminal node without children
			if !context.hasSeenAccount {
				err = fmt.Errorf("value node %v is reachable without passing an account", curNodeRef.Id())
			}
		}

		handle.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

type nodeCheckContext struct {
	root           NodeId
	path           []Nibble
	hasSeenAccount bool
}

func (c *nodeCheckContext) isCompatible(other *nodeCheckContext) bool {
	return c.hasSeenAccount == other.hasSeenAccount && slices.Equal(c.path, other.path)
}

// nodeBase is an optional common base type for nodes.
type nodeBase struct {
	hash       common.Hash // the hash of this node (may be dirty)
	hashStatus hashStatus  // indicating whether this node's hash is valid
	clean      bool        // by default nodes are dirty (clean == false)
	frozen     bool        // a flag marking the node as immutable (default: mutable)
}

type hashStatus byte

const (
	hashStatusClean   hashStatus = 0 // the hash is up-to-date, matching the nodes content
	hashStatusDirty   hashStatus = 1 // the hash is out-of-date and needs to be refreshed
	hashStatusUnknown hashStatus = 2 // the hash is missing / invalid
)

func (s hashStatus) String() string {
	switch s {
	case hashStatusClean:
		return "clean"
	case hashStatusDirty:
		return "dirty"
	default:
		return "unknown"
	}
}

func (n *nodeBase) GetHash() (common.Hash, bool) {
	return n.hash, n.hashStatus != hashStatusClean
}

func (n *nodeBase) SetHash(hash common.Hash) {
	n.hash = hash
	n.hashStatus = hashStatusClean
}

func (n *nodeBase) hasCleanHash() bool {
	return n.hashStatus == hashStatusClean
}

func (n *nodeBase) getHashStatus() hashStatus {
	return n.hashStatus
}

func (n *nodeBase) IsFrozen() bool {
	return n.frozen
}

func (n *nodeBase) MarkFrozen() {
	n.frozen = true
}

func (n *nodeBase) markMutable() {
	n.frozen = false
}

func (n *nodeBase) IsDirty() bool {
	return !n.clean
}

func (n *nodeBase) MarkClean() {
	n.clean = true
}

func (n *nodeBase) markDirty() {
	n.clean = false
	n.hashStatus = hashStatusDirty
}

func (n *nodeBase) Release() {
	// The node is disconnected from the disk version and thus clean.
	n.clean = true
	n.hashStatus = hashStatusClean
}

func (n *nodeBase) check(thisRef *NodeReference) error {
	var errs []error
	if !n.IsDirty() && n.hashStatus == hashStatusDirty {
		errs = append(errs, fmt.Errorf("node %v is marked clean but hash is dirty", thisRef.Id()))
	}
	if n.IsDirty() && n.hashStatus == hashStatusUnknown {
		errs = append(errs, fmt.Errorf("node %v is marked dirty but hash is marked unknown (should be dirty or clean)", thisRef.Id()))
	}
	return errors.Join(errs...)
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
	res.markDirty()
	res.address = address
	res.info = info
	res.pathLength = byte(len(path))
	return ref, false, nil
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
	res.markDirty()
	res.pathLength = byte(len(path))
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

func (e EmptyNode) IsDirty() bool {
	return false
}

func (e EmptyNode) MarkClean() {}

func (e EmptyNode) GetHash() (common.Hash, bool) {
	// The hash of an empty node should be defined by the hash algorithm as
	// a constant, and not stored in an empty node instance. Thus, the empty
	// node is not required to store a hash and if asked for it, it is always
	// appearing as to have a dirty hash.
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

func (EmptyNode) Check(NodeSource, *NodeReference, []Nibble) error {
	// No invariants to be checked.
	return nil
}

func (EmptyNode) Dump(out io.Writer, _ NodeSource, thisRef *NodeReference, indent string) error {
	fmt.Fprintf(out, "%s-empty- (ID: %v)\n", indent, thisRef.Id())
	return nil
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
	nodeBase
	children         [16]NodeReference // the ID of child nodes
	hashes           [16]common.Hash   // the hashes of child nodes
	dirtyHashes      uint16            // a bit mask marking hashes as dirty; 0 .. clean, 1 .. dirty
	embeddedChildren uint16            // a bit mask marking children as embedded; 0 .. not, 1 .. embedded
	frozenChildren   uint16            // a bit mask marking frozen children; not persisted
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
			n.markDirty()
			n.markChildHashDirty(byte(path[0]))
		}
		return *thisRef, hasChanged, nil
	}

	// If frozen, clone the current node and modify copy.
	isClone := false
	if n.IsFrozen() {
		newRef, handle, err := manager.createBranch()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer handle.Release()
		newNode := handle.Get().(*BranchNode)
		*newNode = *n
		newNode.markDirty()
		newNode.markMutable()
		n = newNode
		thisRef = &newRef
		isClone = true
	}

	wasEmpty := child.Id().IsEmpty()
	n.children[path[0]] = newRoot
	n.markChildHashDirty(byte(path[0]))
	n.setChildFrozen(byte(path[0]), false)

	// If a branch got removed, check that there are enough children left.
	if !wasEmpty && newRoot.Id().IsEmpty() {
		n.markChildHashClean(byte(path[0]))
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

				// If the extension is frozen, we need to modify a copy.
				if extensionNode.IsFrozen() {
					copyId, handle, err := manager.createExtension()
					if err != nil {
						return NodeReference{}, false, err
					}
					defer handle.Release()
					copy := handle.Get().(*ExtensionNode)
					*copy = *extensionNode
					copy.markMutable()
					extensionNode = copy
					remaining = copyId
					newRoot = copyId
				}

				extensionNode.path.Prepend(remainingPos)
				extensionNode.markDirty()
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
				extension.nextHashDirty = n.isChildHashDirty(byte(remainingPos))
				if !extension.nextHashDirty {
					extension.nextIsEmbedded = n.isEmbedded(byte(remainingPos))
					extension.nextHash = n.hashes[byte(remainingPos)]
				}
				extension.markDirty()
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
			n.nodeBase.Release()
			return newRoot, !isClone, manager.release(thisRef)
		}
	}

	n.markDirty()
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
	if n.IsFrozen() {
		return nil
	}
	n.nodeBase.Release()
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
	return manager.release(thisRef)
}

func (n *BranchNode) MarkFrozen() {
	n.nodeBase.MarkFrozen()
	n.frozenChildren = ^uint16(0)
}

func (n *BranchNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.IsFrozen() {
		return nil
	}
	n.nodeBase.MarkFrozen()
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

func (n *BranchNode) Check(source NodeSource, thisRef *NodeReference, _ []Nibble) error {
	// Checked invariants:
	//  - must have 2+ children
	//  - non-dirty hashes for child nodes are valid
	//  - mask of frozen children is consistent
	numChildren := 0
	var errs []error

	if err := n.nodeBase.check(thisRef); err != nil {
		errs = append(errs, err)
	}

	hashWithParent := source.getConfig().HashStorageLocation == HashStoredWithParent
	if hashWithParent && n.hasCleanHash() && n.dirtyHashes != 0 {
		errs = append(errs, fmt.Errorf("node %v is has clean hash but child hashes are dirty: %016b", thisRef.Id(), n.dirtyHashes))
	}

	for i, child := range n.children {
		if child.Id().IsEmpty() {
			continue
		}
		numChildren++
		if !n.isChildHashDirty(byte(i)) && !n.isEmbedded(byte(i)) {
			want, err := source.getHashFor(&child)
			if err != nil {
				errs = append(errs, err)
			} else if got := n.hashes[i]; want != got {
				errs = append(errs, fmt.Errorf("in node %v the hash for child %d is invalid\nwant: %v\ngot: %v", thisRef.Id(), i, want, got))
			}
		}
		handle, err := source.getViewAccess(&child)
		if err != nil {
			return err
		}

		childIsFrozen := handle.Get().IsFrozen()
		handle.Release()

		// rule: child is marked as frozen -> childIsFrozen (implication)
		if flag := n.isChildFrozen(byte(i)); flag && !childIsFrozen {
			errs = append(errs, fmt.Errorf("in node %v the frozen flag for child 0x%X is invalid, flag: %t, actual: %t", thisRef.Id(), i, flag, childIsFrozen))
		}

		// rule: if this node is frozen, all children must be frozen
		if n.IsFrozen() && !childIsFrozen {
			errs = append(errs, fmt.Errorf("the frozen node %v must not have a non-frozen child at position 0x%X", thisRef.Id(), i))
		}
	}
	if numChildren < 2 {
		errs = append(errs, fmt.Errorf("node %v has an insufficient number of child nodes: %d", thisRef.Id(), numChildren))
	}
	return errors.Join(errs...)
}

func (n *BranchNode) Dump(out io.Writer, source NodeSource, thisRef *NodeReference, indent string) error {
	errs := []error{}
	fmt.Fprintf(out, "%sBranch (ID: %v, dirty: %t, frozen: %t, Dirty: %016b, Embedded: %016b, Frozen: %016b, Hash: %v, hashState: %v):\n", indent, thisRef.Id(), n.IsDirty(), n.IsFrozen(), n.dirtyHashes, n.embeddedChildren, n.frozenChildren, formatHashForDump(n.hash), n.getHashStatus())
	for i, child := range n.children {
		if child.Id().IsEmpty() {
			continue
		}
		if handle, err := source.getViewAccess(&child); err == nil {
			defer handle.Release()
			if err := handle.Get().Dump(out, source, &child, fmt.Sprintf("%s  %v ", indent, Nibble(i))); err != nil {
				errs = append(errs, err)
			}
		} else {
			fmt.Fprintf(out, "%s  ERROR: unable to load node %v: %v", indent, child, err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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

func (n *BranchNode) markChildHashClean(index byte) {
	n.dirtyHashes = n.dirtyHashes & ^(1 << index)
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
	nodeBase
	path           Path
	next           NodeReference
	nextHash       common.Hash
	nextHashDirty  bool
	nextIsEmbedded bool
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

		// The modified sub-trie is either a branch, extension, account, or
		// value node. It can not be empty, since a single modification cannot
		// convert a branch node into an empty node.

		if newRoot != n.next {

			// If frozen, modify a clone.
			isClone := false
			if n.IsFrozen() {
				newRef, handle, err := manager.createExtension()
				if err != nil {
					return NodeReference{}, false, err
				}
				defer handle.Release()
				newNode := handle.Get().(*ExtensionNode)
				*newNode = *n
				newNode.markDirty()
				newNode.markMutable()
				thisRef, n = &newRef, newNode
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
				n.nextHashDirty = extension.nextHashDirty
				if !extension.nextHashDirty {
					n.nextHash = extension.nextHash
					n.nextIsEmbedded = extension.nextIsEmbedded
				}
				n.markDirty()
				extension.nodeBase.Release()
				if err := manager.release(&newRoot); err != nil {
					return NodeReference{}, false, err
				}
			} else if newRoot.Id().IsBranch() {
				n.next = newRoot
				n.nextHashDirty = true
				n.markDirty()
			} else {
				// If the next node is anything but a branch or extension, remove this extension.
				n.nodeBase.Release()
				if err := manager.release(thisRef); err != nil {
					return NodeReference{}, false, err
				}

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
			n.markDirty()
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
	if n.IsFrozen() {
		newRef, handle, err := manager.createExtension()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer handle.Release()
		newNode := handle.Get().(*ExtensionNode)
		*newNode = *n
		newNode.markDirty()
		newNode.markMutable()
		thisRef, n = &newRef, newNode
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
		n.markDirty()
		thisNodeWasReused = true
	} else {
		pos := byte(n.path.Get(commonPrefixLength))
		branch.children[pos] = n.next
		if n.nextHashDirty {
			branch.markChildHashDirty(pos)
		} else {
			branch.hashes[pos] = n.nextHash
			branch.setEmbedded(pos, n.nextIsEmbedded)
		}
		branch.setChildFrozen(pos, isClone)
	}

	// Build the extension covering the common prefix.
	if commonPrefixLength > 0 {
		// Reuse current node unless already taken.
		extension := n
		extensionRef := *thisRef
		if thisNodeWasReused {
			var extensionHandle shared.WriteHandle[Node]
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
		extension.nextHashDirty = true
		extension.markDirty()
		newRoot = extensionRef
	}

	// Continue insertion of new account at new branch level.
	_, _, err = createSubTree(&branchRef, branchHandle, path[commonPrefixLength:])
	if err != nil {
		return NodeReference{}, false, err
	}

	// If this node was not needed any more, we can discard it.
	if !thisNodeWasReused {
		n.nodeBase.Release()
		return newRoot, false, manager.release(thisRef)
	}

	return newRoot, !isClone, nil
}

func (n *ExtensionNode) SetAccount(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, info AccountInfo) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, path, info.IsEmpty(),
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetAccount(manager, next, node, address, path, info)
		},
	)
}

func (n *ExtensionNode) SetValue(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], key common.Key, path []Nibble, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, path, value == (common.Value{}),
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetValue(manager, next, node, key, path, value)
		},
	)
}

func (n *ExtensionNode) SetSlot(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble, key common.Key, value common.Value) (NodeReference, bool, error) {
	return n.setNextNode(manager, thisRef, path, true,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().SetSlot(manager, next, node, address, path, key, value)
		},
	)
}

func (n *ExtensionNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, hasChanged bool, err error) {
	return n.setNextNode(manager, thisRef, path, true,
		func(next *NodeReference, node shared.WriteHandle[Node], path []Nibble) (NodeReference, bool, error) {
			return node.Get().ClearStorage(manager, next, node, address, path)
		},
	)
}

func (n *ExtensionNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.IsFrozen() {
		return nil
	}
	n.nodeBase.Release()
	handle, err := manager.getWriteAccess(&n.next)
	if err != nil {
		return err
	}
	defer handle.Release()
	err = handle.Get().Release(manager, &n.next, handle)
	if err != nil {
		return err
	}
	return manager.release(thisRef)
}

func (n *ExtensionNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.IsFrozen() {
		return nil
	}
	n.MarkFrozen()
	handle, err := manager.getWriteAccess(&n.next)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Freeze(manager, handle)
}

func (n *ExtensionNode) Check(source NodeSource, thisRef *NodeReference, _ []Nibble) error {
	// Checked invariants:
	//  - extension path have a length > 0
	//  - extension can only be followed by a branch
	//  - hash of sub-tree is either dirty or correct
	//  - frozen flags are consistent
	var errs []error

	if err := n.nodeBase.check(thisRef); err != nil {
		errs = append(errs, err)
	}

	hashWithParent := source.getConfig().HashStorageLocation == HashStoredWithParent
	if hashWithParent && n.hasCleanHash() && n.nextHashDirty {
		errs = append(errs, fmt.Errorf("node %v is marked to have a clean hash but next hash is dirty", thisRef.Id()))
	}

	if n.path.Length() <= 0 {
		errs = append(errs, fmt.Errorf("node %v - extension path must not be empty", thisRef.Id()))
	}
	if !n.next.Id().IsBranch() {
		errs = append(errs, fmt.Errorf("node %v - extension path must be followed by a branch", thisRef.Id()))
	}
	if !n.nextHashDirty && !n.nextIsEmbedded {
		want, err := source.getHashFor(&n.next)
		if err != nil {
			errs = append(errs, err)
		} else if want != n.nextHash {
			errs = append(errs, fmt.Errorf("node %v - next node hash invalid\nwant: %v\ngot: %v", thisRef.Id(), want, n.nextHash))
		}
	}

	if !n.next.Id().IsEmpty() {
		handle, err := source.getViewAccess(&n.next)
		if err != nil {
			errs = append(errs, err)
		} else {
			nextIsFrozen := handle.Get().IsFrozen()
			handle.Release()
			if n.IsFrozen() && !nextIsFrozen {
				errs = append(errs, fmt.Errorf("the frozen node %v must have a frozen next", thisRef.Id()))
			}
		}
	}

	return errors.Join(errs...)
}

func (n *ExtensionNode) Dump(out io.Writer, source NodeSource, thisRef *NodeReference, indent string) error {
	errs := []error{}
	fmt.Fprintf(out, "%sExtension (ID: %v/%t, nextHashDirty: %t, Embedded: %t, Hash: %v, hashState: %v): %v\n", indent, thisRef.Id(), n.IsFrozen(), n.nextHashDirty, n.nextIsEmbedded, formatHashForDump(n.hash), n.getHashStatus(), &n.path)
	if handle, err := source.getViewAccess(&n.next); err == nil {
		defer handle.Release()
		if err := handle.Get().Dump(out, source, &n.next, indent+"  "); err != nil {
			errs = append(errs, err)
		}
	} else {
		fmt.Fprintf(out, "%s  ERROR: unable to load node %v: %v", indent, n.next, err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
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
	nodeBase
	address          common.Address
	info             AccountInfo
	storage          NodeReference
	storageHash      common.Hash
	storageHashDirty bool
	// pathLength is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
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
			if n.IsFrozen() {
				return NewNodeReference(EmptyId()), false, nil
			}
			// Recursively release the entire state DB.
			if !n.storage.Id().IsEmpty() {
				manager.releaseTrieAsynchronous(n.storage)
			}
			// Release this account node and remove it from the trie.
			n.nodeBase.Release()
			return NewNodeReference(EmptyId()), false, manager.release(thisRef)
		}

		// If this node is frozen, we need to write the result in
		// a new account node.
		if n.IsFrozen() {
			newRef, handle, err := manager.createAccount()
			if err != nil {
				return NodeReference{}, false, err
			}
			defer handle.Release()
			newNode := handle.Get().(*AccountNode)
			*newNode = *n
			newNode.markDirty()
			newNode.markMutable()
			newNode.info = info
			return newRef, false, nil
		}

		n.info = info
		n.markDirty()
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
	sibling.markDirty()

	thisPath := AddressToNibblePath(n.address, manager)
	newRoot, err := splitLeafNode(manager, thisRef, thisPath[:], n, this, path, &siblingRef, sibling, handle)
	return newRoot, !n.IsFrozen() && manager.getConfig().TrackSuffixLengthsInLeafNodes, err
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
		extension.nextHashDirty = true
		extension.markDirty()
	}

	// If enabled, keep track of the suffix length of leaf values.
	thisModified := false
	thisIsFrozen := this.IsFrozen()
	remainingPathLength := byte(len(partialPath)-commonPrefixLength) - 1
	if manager.getConfig().TrackSuffixLengthsInLeafNodes {
		sibling.setPathLength(manager, siblingRef, siblingHandle, remainingPathLength)
		ref, _, err := this.setPathLength(manager, thisRef, thisHandle, remainingPathLength)
		if err != nil {
			return NodeReference{}, err
		}
		thisModified = true
		thisRef = &ref
		thisIsFrozen = false
	}

	// Add this node and the new sibling node to the branch node.
	branch.children[partialPath[commonPrefixLength]] = *thisRef
	branch.children[siblingPath[commonPrefixLength]] = *siblingRef
	branch.markChildHashDirty(byte(siblingPath[commonPrefixLength]))
	branch.markDirty()

	// Update hash if present.
	if hash, dirty := this.GetHash(); thisModified || dirty {
		branch.markChildHashDirty(byte(partialPath[commonPrefixLength]))
	} else {
		branch.hashes[partialPath[commonPrefixLength]] = hash
		// The embedded flag can be ignored in this case as long as direct
		// hashing is used.
		if manager.getConfig().Hashing.Name != DirectHashing.Name {
			panic("unsupported mode: disabled TrackSuffixLengthsInLeafNodes is not (yet) supported with hash algorithms depending on embedded nodes.")
		}
	}

	// Track frozen state of split node.
	if thisIsFrozen {
		branch.setChildFrozen(byte(partialPath[commonPrefixLength]), true)
	}

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
		if n.IsFrozen() {
			newRef, newHandle, err := manager.createAccount()
			if err != nil {
				return NodeReference{}, false, err
			}
			defer newHandle.Release()
			newNode := newHandle.Get().(*AccountNode)
			*newNode = *n
			newNode.markDirty()
			newNode.markMutable()
			newNode.storage = root
			newNode.storageHashDirty = true
			return newRef, false, nil
		}
		n.storage = root
		n.storageHashDirty = true
		n.markDirty()
		hasChanged = true
	} else if hasChanged {
		n.storageHashDirty = true
		n.markDirty()
	}
	return *thisRef, hasChanged, nil
}

func (n *AccountNode) ClearStorage(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], address common.Address, path []Nibble) (newRoot NodeReference, changed bool, err error) {
	if n.address != address || n.storage.Id().IsEmpty() {
		return *thisRef, false, nil
	}

	// If this node is frozen, we need to write the result in
	// a new account node.
	if n.IsFrozen() {
		newRef, newHandle, err := manager.createAccount()
		if err != nil {
			return *thisRef, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*AccountNode)
		*newNode = *n
		newNode.markDirty()
		newNode.markMutable()
		newNode.storage = NewNodeReference(EmptyId())
		newNode.storageHashDirty = true
		return newRef, false, nil
	}

	if !n.storage.Id().IsEmpty() {
		manager.releaseTrieAsynchronous(n.storage)
	}

	n.storage = NewNodeReference(EmptyId())
	n.markDirty()
	n.storageHashDirty = true
	return *thisRef, true, err
}

func (n *AccountNode) Release(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node]) error {
	if n.IsFrozen() {
		return nil
	}
	n.nodeBase.Release()
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
	return manager.release(thisRef)
}

func (n *AccountNode) setPathLength(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], length byte) (NodeReference, bool, error) {
	if n.pathLength == length {
		return *thisRef, false, nil
	}
	if n.IsFrozen() {
		newRef, newHandle, err := manager.createAccount()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*AccountNode)
		*newNode = *n
		newNode.markDirty()
		newNode.markMutable()
		newNode.pathLength = length
		return newRef, false, nil
	}

	n.pathLength = length
	n.markDirty()
	return *thisRef, true, nil
}

func (n *AccountNode) Freeze(manager NodeManager, this shared.WriteHandle[Node]) error {
	if n.IsFrozen() {
		return nil
	}
	n.MarkFrozen()
	handle, err := manager.getWriteAccess(&n.storage)
	if err != nil {
		return err
	}
	defer handle.Release()
	return handle.Get().Freeze(manager, handle)
}

func (n *AccountNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble) error {
	// Checked invariants:
	//  - account information must not be empty
	//  - the account is at a correct position in the trie
	//  - frozen flags are consistent
	//  - path length
	var errs []error

	if err := n.nodeBase.check(thisRef); err != nil {
		errs = append(errs, err)
	}

	hashWithParent := source.getConfig().HashStorageLocation == HashStoredWithParent
	if hashWithParent && n.hasCleanHash() && n.storageHashDirty {
		errs = append(errs, fmt.Errorf("node %v is marked to have a clean hash but storage hash is dirty", thisRef.Id()))
	}

	fullPath := AddressToNibblePath(n.address, source)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("node %v - account node %v located in wrong branch: %v", thisRef.Id(), n.address, path))
	}

	if n.info.IsEmpty() {
		errs = append(errs, fmt.Errorf("node %v - account information must not be empty", thisRef.Id()))
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

	if !n.storage.Id().IsEmpty() {
		handle, err := source.getViewAccess(&n.storage)
		if err != nil {
			errs = append(errs, err)
		} else {
			storageIsFrozen := handle.Get().IsFrozen()
			handle.Release()
			if n.IsFrozen() && !storageIsFrozen {
				errs = append(errs, fmt.Errorf("the frozen node %v must not have a non-frozen storage", thisRef.Id()))
			}
		}
	}

	return errors.Join(errs...)
}

func (n *AccountNode) Dump(out io.Writer, source NodeSource, thisRef *NodeReference, indent string) error {
	errs := []error{}
	fmt.Fprintf(out, "%sAccount (ID: %v, dirty: %t, frozen: %t, path length: %v, Hash: %v, hashState: %v): %v - %v\n", indent, thisRef.Id(), n.IsDirty(), n.IsFrozen(), n.pathLength, formatHashForDump(n.hash), n.getHashStatus(), n.address, n.info)
	if n.storage.Id().IsEmpty() {
		return nil
	}
	if node, err := source.getViewAccess(&n.storage); err == nil {
		defer node.Release()
		if err := node.Get().Dump(out, source, &n.storage, indent+"  "); err != nil {
			errs = append(errs, err)
		}
	} else {
		fmt.Fprintf(out, "%s  ERROR: unable to load node %v: %v", indent, n.storage, err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
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
	nodeBase
	key   common.Key
	value common.Value
	// pathLength is the number of nibbles of the key (or its hash) not covered
	// by the navigation path to this node. It is only maintained if the
	// `TrackSuffixLengthsInLeafNodes` of the `MptConfig` is enabled.
	pathLength byte
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
			if !n.IsFrozen() {
				n.nodeBase.Release()
				if err := manager.release(thisRef); err != nil {
					return NodeReference{}, false, err
				}
			}
			return NewNodeReference(EmptyId()), !n.IsFrozen(), nil
		}
		if n.IsFrozen() {
			newRef, newHandle, err := manager.createValue()
			if err != nil {
				return NodeReference{}, false, nil
			}
			defer newHandle.Release()
			newNode := newHandle.Get().(*ValueNode)
			newNode.key = n.key
			newNode.value = value
			newNode.markDirty()
			newNode.pathLength = n.pathLength
			return newRef, false, nil
		}
		n.value = value
		n.markDirty()
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
	sibling.markDirty()

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
	if n.IsFrozen() {
		return nil
	}
	n.nodeBase.Release()
	return manager.release(thisRef)
}

func (n *ValueNode) setPathLength(manager NodeManager, thisRef *NodeReference, this shared.WriteHandle[Node], length byte) (NodeReference, bool, error) {
	if n.pathLength == length {
		return *thisRef, false, nil
	}
	if n.IsFrozen() {
		newRef, newHandle, err := manager.createValue()
		if err != nil {
			return NodeReference{}, false, err
		}
		defer newHandle.Release()
		newNode := newHandle.Get().(*ValueNode)
		newNode.key = n.key
		newNode.value = n.value
		newNode.markDirty()
		newNode.pathLength = length
		return newRef, false, nil
	}

	n.pathLength = length
	n.markDirty()
	return *thisRef, true, nil
}

func (n *ValueNode) Freeze(NodeManager, shared.WriteHandle[Node]) error {
	n.MarkFrozen()
	return nil
}

func (n *ValueNode) Check(source NodeSource, thisRef *NodeReference, path []Nibble) error {
	// Checked invariants:
	//  - value must not be empty
	//  - values are in the right position of the trie
	//  - the path length is correct (if enabled to be tracked)
	var errs []error

	if err := n.nodeBase.check(thisRef); err != nil {
		errs = append(errs, err)
	}

	fullPath := KeyToNibblePath(n.key, source)
	if !IsPrefixOf(path, fullPath[:]) {
		errs = append(errs, fmt.Errorf("node %v - value node %v [%v] located in wrong branch: %v", thisRef.Id(), n.key, fullPath, path))
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

func (n *ValueNode) Dump(out io.Writer, source NodeSource, thisRef *NodeReference, indent string) error {
	fmt.Fprintf(out, "%sValue (ID: %v/%t/%d, Hash: %v, hashState: %v): %v - %x\n", indent, thisRef.Id(), n.IsFrozen(), n.pathLength, formatHashForDump(n.hash), n.getHashStatus(), n.key, n.value)
	return nil
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

// TODO [cleanup]: move encoder to extra file and clean-up definitions

type BranchNodeEncoderWithNodeHash struct{}

func (BranchNodeEncoderWithNodeHash) GetEncodedSize() int {
	encoder := NodeIdEncoder{}
	return encoder.GetEncodedSize()*16 + common.HashSize
}

func (BranchNodeEncoderWithNodeHash) Store(dst []byte, node *BranchNode) error {
	if !node.hasCleanHash() {
		panic("unable to store branch node with dirty hash")
	}
	encoder := NodeIdEncoder{}
	step := encoder.GetEncodedSize()
	for i := 0; i < 16; i++ {
		encoder.Store(dst[i*step:], &node.children[i].id)
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
		encoder.Load(src[i*step:], &id)
		node.children[i] = NewNodeReference(id)
	}
	src = src[step*16:]
	copy(node.hash[:], src)
	node.hashStatus = hashStatusClean

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
		encoder.Store(dst[i*step:], &node.children[i].id)
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
		encoder.Load(src[i*step:], &id)
		node.children[i] = NewNodeReference(id)
	}
	src = src[step*16:]
	for i := 0; i < 16; i++ {
		copy(node.hashes[i][:], src)
		src = src[common.HashSize:]
	}
	node.embeddedChildren = binary.BigEndian.Uint16(src)

	// The node's hash is not stored with the node, so it is marked unknown.
	node.hashStatus = hashStatusUnknown

	return nil
}

type ExtensionNodeEncoderWithNodeHash struct{}

func (ExtensionNodeEncoderWithNodeHash) GetEncodedSize() int {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	return pathEncoder.GetEncodedSize() + idEncoder.GetEncodedSize() + common.HashSize
}

func (ExtensionNodeEncoderWithNodeHash) Store(dst []byte, value *ExtensionNode) error {
	if !value.hasCleanHash() {
		panic("unable to store extension node with dirty hash")
	}
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	pathEncoder.Store(dst, &value.path)
	dst = dst[pathEncoder.GetEncodedSize():]
	idEncoder.Store(dst, &value.next.id)
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst, value.hash[:])
	return nil
}

func (ExtensionNodeEncoderWithNodeHash) Load(src []byte, node *ExtensionNode) error {
	pathEncoder := PathEncoder{}
	idEncoder := NodeIdEncoder{}
	pathEncoder.Load(src, &node.path)
	src = src[pathEncoder.GetEncodedSize():]
	var id NodeId
	idEncoder.Load(src, &id)
	node.next = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.hash[:], src)
	node.hashStatus = hashStatusClean

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
	pathEncoder.Store(dst, &value.path)
	dst = dst[pathEncoder.GetEncodedSize():]
	idEncoder.Store(dst, &value.next.id)
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
	pathEncoder.Load(src, &node.path)
	src = src[pathEncoder.GetEncodedSize():]
	var id NodeId
	idEncoder.Load(src, &id)
	node.next = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.nextHash[:], src)
	src = src[common.HashSize:]
	node.nextIsEmbedded = src[0] != 0

	// The node's hash is not stored with the node, so it is marked unknown.
	node.hashStatus = hashStatusUnknown

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
	if !node.hasCleanHash() {
		panic("unable to store account node with dirty hash")
	}
	copy(dst, node.address[:])
	dst = dst[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	infoEncoder.Store(dst, &node.info)
	dst = dst[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	idEncoder.Store(dst, &node.storage.id)
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst[:], node.hash[:])
	return nil
}

func (AccountNodeEncoderWithNodeHash) Load(src []byte, node *AccountNode) error {
	copy(node.address[:], src)
	src = src[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	infoEncoder.Load(src, &node.info)
	src = src[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	var id NodeId
	idEncoder.Load(src, &id)
	node.storage = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.hash[:], src)
	node.hashStatus = hashStatusClean

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
	infoEncoder.Store(dst, &node.info)
	dst = dst[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	idEncoder.Store(dst, &node.storage.id)
	dst = dst[idEncoder.GetEncodedSize():]
	copy(dst[:], node.storageHash[:])
	return nil
}

func (AccountNodeEncoderWithChildHash) Load(src []byte, node *AccountNode) error {
	copy(node.address[:], src)
	src = src[len(node.address):]

	infoEncoder := AccountInfoEncoder{}
	infoEncoder.Load(src, &node.info)
	src = src[infoEncoder.GetEncodedSize():]

	idEncoder := NodeIdEncoder{}
	var id NodeId
	idEncoder.Load(src, &id)
	node.storage = NewNodeReference(id)
	src = src[idEncoder.GetEncodedSize():]
	copy(node.storageHash[:], src)

	// The node's hash is not stored with the node, so it is marked unknown.
	node.hashStatus = hashStatusUnknown

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

	// The node's hash is not stored with the node, so it is marked unknown.
	node.hashStatus = hashStatusUnknown

	return nil
}

type ValueNodeEncoderWithNodeHash struct{}

func (ValueNodeEncoderWithNodeHash) GetEncodedSize() int {
	return ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize() + common.HashSize
}

func (ValueNodeEncoderWithNodeHash) Store(dst []byte, node *ValueNode) error {
	if !node.hasCleanHash() {
		panic("unable to store value node with dirty hash")
	}
	ValueNodeEncoderWithoutNodeHash{}.Store(dst, node)
	dst = dst[ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize():]
	copy(dst, node.hash[:])
	return nil
}

func (ValueNodeEncoderWithNodeHash) Load(src []byte, node *ValueNode) error {
	ValueNodeEncoderWithoutNodeHash{}.Load(src, node)
	src = src[ValueNodeEncoderWithoutNodeHash{}.GetEncodedSize():]
	copy(node.hash[:], src)
	node.hashStatus = hashStatusClean
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
