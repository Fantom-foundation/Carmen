package mpt

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

//go:generate mockgen -source node_manager.go -destination node_manager_mocks.go -package mpt

type NodeSource interface {
	GetReadAccess(*NodeReference) (shared.ReadHandle[Node], error)
	Touch(NodeReference) error

	// Internal interface for NodeReferences.
	getOwner(NodeId) (*nodeOwner, error)
}

// NodeManager are responsible of managing the live-cycle of node instances of
// an MPT. Since the overall memory consumption of all nodes of an MPT exceeds
// the memory resources for any machine for real-chain data, only a share of
// those nodes can be held in memory at any given time. It is the job of a
// NodeManager to decide which nodes to retain and which nodes to flush.
type NodeManager interface {
	NodeSource

	// Provides the number of nodes currently retained by this manager.
	Size() int

	CreateAccount() (NodeReference, shared.WriteHandle[Node], error)
	CreateBranch() (NodeReference, shared.WriteHandle[Node], error)
	CreateExtension() (NodeReference, shared.WriteHandle[Node], error)
	CreateValue() (NodeReference, shared.WriteHandle[Node], error)

	GetWriteAccess(*NodeReference) (shared.WriteHandle[Node], error)
	MarkDirty(NodeReference, shared.WriteHandle[Node])
	Release(NodeReference) error

	Flush() error
	Close() error
}

type NodeStore interface {
	// Load requests the given node to be loaded from an underlying storage system.
	Load(NodeId) (Node, error)

	// Store requests the given node to be stored to the underlying storage system.
	Store(NodeId, Node) error

	GetFreshAccountId() (NodeId, error)
	GetFreshBranchId() (NodeId, error)
	GetFreshExtensionId() (NodeId, error)
	GetFreshValueId() (NodeId, error)

	Release(NodeId) error

	Flush() error
	Close() error
}

// Node Live Cycle:
//  non-existing .. not referenced by any valid owner
//  in-memory .. referenced by an owner which is part of the index and the LRU list
//  on-transit .. referenced by an owner which is part of the index
//  on-disk .. not referenced by any valid owner

type nodeManager struct {
	store       NodeStore
	writeBuffer WriteBuffer

	// The manager retains a mapping from node IDs to nodeOwners. A node
	// owner is an internal object that controls the presence of a given
	// node in memory. All node references with the same node ID within
	// the scope of a single manager are required to point to the master
	// nodeOwner indexed in this map or to an invalid or nil owner.
	index      map[NodeId]*nodeOwner
	indexMutex sync.Mutex

	// The following fields define a double-linked list of node owners
	// in their least-recently-used order. Each access to a node owner
	// triggers an update of this list to retain the LRU order. The
	// size of the list is limited to the capacity of the manager.
	capacity  int
	size      int
	head      *nodeOwner
	tail      *nodeOwner
	listMutex sync.Mutex
}

func NewNodeManager(capacity int, store NodeStore) NodeManager {
	// By default a node owner for the empty node is added to each manager.
	// This way, the LRU list is never empty, simplifying the code for
	// manipulating the LRU list of node owners.
	emptyNodeOwner := newNodeOwner(
		EmptyId(),
		shared.MakeShared[Node](EmptyNode{}),
	)

	// The index for all valid owners. Owners are valid while being listed
	// in the LRU list. Once evicted and moved to the output buffer, owners
	// are invalidated.
	index := make(map[NodeId]*nodeOwner, capacity)
	index[EmptyId()] = emptyNodeOwner

	bufferCapacity := 1024
	return &nodeManager{
		capacity: capacity,
		store:    store,
		writeBuffer: makeWriteBuffer(
			nodeStoreToSinkAdapter{store},
			bufferCapacity,
		),
		size:  1,
		index: index,
		head:  emptyNodeOwner,
		tail:  emptyNodeOwner,
	}
}

func (m *nodeManager) Size() int {
	m.listMutex.Lock()
	res := m.size
	m.listMutex.Unlock()
	return res
}

func (m *nodeManager) CreateAccount() (NodeReference, shared.WriteHandle[Node], error) {
	id, err := m.store.GetFreshAccountId()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	return m.createNode(id, &AccountNode{})
}

func (m *nodeManager) CreateBranch() (NodeReference, shared.WriteHandle[Node], error) {
	id, err := m.store.GetFreshBranchId()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	return m.createNode(id, &BranchNode{})
}

func (m *nodeManager) CreateExtension() (NodeReference, shared.WriteHandle[Node], error) {
	id, err := m.store.GetFreshExtensionId()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	return m.createNode(id, &ExtensionNode{})
}

func (m *nodeManager) CreateValue() (NodeReference, shared.WriteHandle[Node], error) {
	id, err := m.store.GetFreshValueId()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	return m.createNode(id, &ValueNode{})
}

func (m *nodeManager) createNode(id NodeId, node Node) (NodeReference, shared.WriteHandle[Node], error) {
	shared := shared.MakeShared[Node](node)
	owner := newNodeOwner(id, shared)
	handle := shared.GetWriteHandle()
	m.indexMutex.Lock()
	m.index[id] = owner
	m.indexMutex.Unlock()
	m.addToLruList(owner)
	return NodeReference{
		NodeId: id,
		owner:  owner,
	}, handle, nil
}

// TODO: remove error result
func (m *nodeManager) Touch(ref NodeReference) error {

	owner := ref.owner
	/*
		owner, err := ref.getOwner(m)
		if err != nil {
			return err
		}
	*/

	/*
		// TODO: remove before commit.
		if !owner.isValid() {
			panic("should never touch invalid owner!")
		}
	*/

	m.listMutex.Lock()

	// If the node is the current head there is nothing to do.
	if m.head == owner {
		m.listMutex.Unlock()
		return nil
	}

	// Remove node from current position.
	if m.tail == owner {
		m.tail = owner.pred
		m.tail.succ = nil // TODO: consider removing this line for efficiency
	} else {
		owner.succ.pred = owner.pred
		owner.pred.succ = owner.succ
	}

	// Insert node at head position.
	owner.pred = nil // TODO: consider removing this line for efficiency
	owner.succ = m.head
	m.head.pred = owner
	m.head = owner

	m.listMutex.Unlock()
	return nil
}

func (m *nodeManager) GetReadAccess(ref *NodeReference) (shared.ReadHandle[Node], error) {
	for {
		owner, err := ref.getOwner(m)
		if err != nil {
			return shared.ReadHandle[Node]{}, err
		}
		if handle, success := owner.getReadAccess(m); success {
			return handle, nil
		}
	}
}

func (m *nodeManager) GetWriteAccess(ref *NodeReference) (shared.WriteHandle[Node], error) {
	for {
		owner, err := ref.getOwner(m)
		if err != nil {
			return shared.WriteHandle[Node]{}, err
		}
		if handle, success := owner.getWriteAccess(m); success {
			return handle, nil
		}
	}
}

func (m *nodeManager) MarkDirty(ref NodeReference, _ shared.WriteHandle[Node]) {
	// The reference owner should always be set if a write handle on the node is valid.
	ref.owner.markDirty()
}

func (m *nodeManager) Release(ref NodeReference) error {
	// TODO: think through the locking in this function.
	id := ref.Id()
	if id.IsEmpty() {
		return nil
	}
	m.writeBuffer.Cancel(id)

	m.indexMutex.Lock()
	owner, found := m.index[id]
	if found {
		delete(m.index, id)
	}
	m.indexMutex.Unlock()

	owner.dropNode()

	// Remove the owner from the LRU list.
	m.listMutex.Lock()
	m.size--
	if m.size == 0 {
		panic("LRU should never get empty")
	}
	if m.head == owner {
		m.head = owner.succ
		m.head.pred = nil
	} else if m.tail == owner {
		m.tail = m.tail.pred
		m.tail.succ = nil
	} else {
		owner.pred.succ = owner.succ
		owner.succ.pred = owner.pred
	}
	m.listMutex.Unlock()

	return m.store.Release(id)
}

func (m *nodeManager) Flush() error {
	type entry struct {
		id    NodeId
		owner *nodeOwner
	}

	// Get a list of all nodes retained in memory.
	m.indexMutex.Lock()
	entries := make([]entry, 0, len(m.index))
	for id, owner := range m.index {
		if owner.isDirty() {
			entries = append(entries, entry{id, owner})
		}
	}
	m.indexMutex.Unlock()

	// Sort IDs to improve the write order to disk.
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	// Get exclusive access to each individual owner and send dirty instances
	// to the write buffer to be written to the store.
	for _, entry := range entries {
		handle, success := entry.owner.getReadAccess(m)
		if !success {
			continue
		}
		m.writeBuffer.Add(entry.owner.id, entry.owner.node.Load())
		entry.owner.markClean()
		handle.Release()
	}

	return errors.Join(
		m.writeBuffer.Flush(),
		m.store.Flush(),
	)
}
func (m *nodeManager) Close() error {
	return errors.Join(
		m.Flush(),
		m.writeBuffer.Close(),
		m.store.Close(),
	)
}

type nodeStoreToSinkAdapter struct {
	NodeStore
}

func (a nodeStoreToSinkAdapter) Write(id NodeId, handle shared.ReadHandle[Node]) error {
	return a.Store(id, handle.Get())
}

func (m *nodeManager) addToLruList(owner *nodeOwner) {
	m.listMutex.Lock()
	defer m.listMutex.Unlock()

	// If needed, remove an element from the pool.
	if m.size >= m.capacity {

		last := m.tail
		if !last.isValid() {
			fmt.Printf("found invalid owner for id %v in LRU list\n", last.id)
		}

		// Dirty nodes need to be moved from the LRU list to the write buffer.
		// FIXME: another thread may currently modify the node and mark it dirty
		// at the end of the modification; this would be missed, corrupting the
		// database by missing a flush;
		if last.isDirty() {
			m.writeBuffer.Add(last.id, last.node.Load())
		}

		// To evict the node, the reference in the owner is dropped and the owner
		// is marked to be invalid. References to the owner will refresh the owner
		// as need.
		m.indexMutex.Lock()
		delete(m.index, last.id)
		m.indexMutex.Unlock()
		last.dropNode()

		m.tail = m.tail.pred
		m.tail.succ = nil // TODO: consider removing this line for efficiency
	} else {
		m.size++
	}

	owner.pred = nil // TODO: consider removing this line for efficiency
	owner.succ = m.head
	m.head.pred = owner
	m.head = owner
}

func (m *nodeManager) load(id NodeId) (*shared.Shared[Node], error) {
	// If the requested node is not in-transit it is in the store.
	node, err := m.store.Load(id)
	if err != nil {
		return nil, err
	}
	return shared.MakeShared[Node](node), nil
}

func (m *nodeManager) getOwner(id NodeId) (*nodeOwner, error) {
	// start by checking the index
	m.indexMutex.Lock()
	if owner, found := m.index[id]; found {
		m.indexMutex.Unlock()
		return owner, nil
	}

	// If not in the index, check the write buffer. If it is there, cancel
	// the write and bring it back to be owned by a new nodeOwner instance.
	if node, found := m.writeBuffer.Cancel(id); found {
		owner := newNodeOwner(id, node)
		owner.markDirty()
		m.index[id] = owner
		// TODO: think hard whether the index lock needs to be hold while adding to the LRU list
		m.indexMutex.Unlock()
		m.addToLruList(owner)
		return owner, nil
	}
	// Do not block the index while doing IO.
	m.indexMutex.Unlock()

	// Load the node data and create a new owner.
	node, err := m.load(id)
	if err != nil {
		return nil, err
	}
	owner := newNodeOwner(id, node)

	// Check that no concurrent process has loaded the same node in the meanwhile.
	m.indexMutex.Lock()
	if other, found := m.index[id]; found {
		owner.dropNode()
		m.indexMutex.Unlock()
		return other, nil
	}
	m.index[id] = owner
	// TODO: think hard whether the index should be locked while modifying the LRU list
	m.indexMutex.Unlock()
	m.addToLruList(owner)
	return owner, nil
}

type NodeReference struct {
	NodeId            // The ID of the referenced node.
	owner  *nodeOwner // a cached pointer to the node owner
}

func NewNodeReference(id NodeId) NodeReference {
	return NodeReference{NodeId: id}
}

func (r *NodeReference) Id() NodeId {
	if r == nil {
		return EmptyId()
	}
	return r.NodeId
}

func (r *NodeReference) getOwner(source NodeSource) (*nodeOwner, error) {
	// check cached owner first
	if r.owner != nil && r.owner.isValid() {
		return r.owner, nil
	}

	// Get the owner for this node from the manager and cache it in the reference.
	res, err := source.getOwner(r.NodeId)
	if err != nil {
		return nil, err
	}
	r.owner = res
	return res, nil
}

// ------------------------------ Node Owner ----------------------------------

// nodeOwner is managing the presence of a single node in main memory. A node owner
// has a 3-step live cycle:
//
//	TODO: update this documentation:
//
//	- empty: the initial state, where an ID is set, the owner is the canonical
//	  owner of the node associated to this ID, but the node is not in
//	  memory yet (the node reference is nil)
//	- set: the owner is the canonical owner of the node associated to the
//	  contained ID and the node is indeed in memory and referenced by
//	  this node
//	- dropped: the owned node was dropped and the owner is marked invalid. It is
//	  no longer the canonical owner of the associated node and should be
//	  ignored and forgotten by node references
//
// Owners can only progress to successor phases, never back. Each transition must
// happen while the owner node is locked.
type nodeOwner struct {
	id    NodeId                              // the ID of the owned node, never to be changed
	node  atomic.Pointer[shared.Shared[Node]] // the owned node, may be initially set and updated once to nil
	dirty atomic.Bool                         // the owned node's state is different to the on-disk version

	// synchronized by the pool's list mutex
	pred *nodeOwner
	succ *nodeOwner
}

func newNodeOwner(id NodeId, node *shared.Shared[Node]) *nodeOwner {
	res := &nodeOwner{id: id}
	res.node.Store(node)
	return res
}

func (o *nodeOwner) isValid() bool {
	return o.node.Load() != nil
}

func (o *nodeOwner) isDirty() bool {
	return o.dirty.Load()
}

func (o *nodeOwner) markDirty() {
	o.dirty.Store(true)
}

func (o *nodeOwner) markClean() {
	o.dirty.Store(false)
}

func (o *nodeOwner) getReadAccess(source NodeSource) (handle shared.ReadHandle[Node], success bool) {
	// Get the reference to the node.
	shared := o.node.Load()
	if shared == nil {
		return handle, false
	}

	// Acquire read access to the shared node.
	res := shared.GetReadHandle()

	// Check that this owner has not become invalid in the meanwhile.
	if o.node.Load() != shared {
		res.Release()
		return handle, false
	}
	return res, true
}

func (o *nodeOwner) getWriteAccess(manager NodeManager) (handle shared.WriteHandle[Node], success bool) {
	// Get the reference to the node.
	shared := o.node.Load()
	if shared == nil {
		return handle, false
	}

	// Acquire write access to the shared node.
	res := shared.GetWriteHandle()

	// Check that this owner has not become invalid in the meanwhile.
	if o.node.Load() != shared {
		res.Release()
		return handle, false
	}
	return res, true
}

func (o *nodeOwner) dropNode() {
	o.node.Store(nil)
}
