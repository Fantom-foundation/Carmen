package mpt

import (
	"errors"
	"sort"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

//go:generate mockgen -source node_manager.go -destination node_manager_mocks.go -package mpt

type NodeSource interface {
	// Internal interfaces for NodeReferences to interact with the manager.
	load(NodeId) (*shared.Shared[Node], error)
	getOwner(NodeId) lockedOwner
	touch(lockedOwner)
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
	emptyNodeOwner := &nodeOwner{
		valid: true,
		id:    EmptyId(),
		node:  shared.MakeShared[Node](EmptyNode{}),
	}

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
	owner := &nodeOwner{
		valid: true,
		id:    id,
		node:  shared.MakeShared[Node](node),
	}
	handle := owner.node.GetWriteHandle()
	lockedOwner := owner.lock()
	m.indexMutex.Lock()
	m.index[id] = owner
	m.indexMutex.Unlock()
	m.addToLruList(lockedOwner)
	lockedOwner.unlock()
	return NodeReference{
		NodeId: id,
		owner:  owner,
	}, handle, nil
}

func (m *nodeManager) MarkDirty(ref NodeReference, _ shared.WriteHandle[Node]) {
	locked := ref.owner.lock()
	locked.dirty = true
	locked.unlock()
}

func (m *nodeManager) Release(ref NodeReference) error {
	id := ref.Id()
	m.writeBuffer.Cancel(id)
	m.indexMutex.Lock()
	if owner, found := m.index[id]; found {
		locked := owner.lock()
		locked.dropNode()
		locked.unlock()
		delete(m.index, id)
	}
	m.indexMutex.Unlock()
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
		entries = append(entries, entry{id, owner})
	}
	m.indexMutex.Unlock()

	// Sort IDs to improve the write order to disk.
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	// Get exclusive access to each individual owner and send dirty instances
	// to the write buffer to be written to the store.
	for _, entry := range entries {
		locked := entry.owner.lock()
		if locked.valid && locked.dirty {
			m.writeBuffer.Add(locked.id, locked.node)
		}
		locked.unlock()
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

func (m *nodeManager) addToLruList(owner lockedOwner) {
	m.listMutex.Lock()

	// If needed, remove an element from the pool.
	if m.size >= m.capacity {

		locked := m.tail.lock()
		if locked.dirty {
			// Dirty nodes need to be moved from the LRU list to the write buffer.
			m.writeBuffer.Add(locked.id, locked.node)
		}
		// To evict the node, the reference in the owner is dropped and the owner
		// is marked to be invalid. References to the owner will refresh the owner
		// as need.
		locked.dropNode()
		delete(m.index, locked.id)
		locked.unlock()

		m.tail = m.tail.pred
		m.tail.succ = nil // TODO: consider removing this line for efficiency
	} else {
		m.size++
	}

	owner.pred = nil // TODO: consider removing this line for efficiency
	owner.succ = m.head
	m.head.pred = owner.nodeOwner
	m.head = owner.nodeOwner
	m.listMutex.Unlock()
}

func (m *nodeManager) load(id NodeId) (*shared.Shared[Node], error) {

	// If the node is in the write buffer, cancel the write and bring it back
	// to be owned by an nodeOwner in the LRU list.
	if node, found := m.writeBuffer.Cancel(id); found {
		return node, nil
	}

	// If the requested node is not in-transit it is in the store.
	node, err := m.store.Load(id)
	if err != nil {
		return nil, err
	}
	return shared.MakeShared[Node](node), nil
}

func (m *nodeManager) getOwner(id NodeId) lockedOwner {
	m.indexMutex.Lock()
	if owner, found := m.index[id]; found {
		res := owner.lock()
		m.touch(res)
		m.indexMutex.Unlock()
		return res
	}
	owner := &nodeOwner{
		valid: true,
		id:    id,
	}
	m.index[id] = owner
	res := owner.lock()
	m.indexMutex.Unlock()
	m.addToLruList(res)
	return res
}

func (m *nodeManager) touch(owner lockedOwner) {
	// TODO: remove before commit.
	if !owner.valid {
		panic("should never touch invalid owner!")
	}

	m.listMutex.Lock()

	// If the node is the current head there is nothing to do.
	if m.head == owner.nodeOwner {
		m.listMutex.Unlock()
		return
	}

	// Remove node from current position.
	if m.tail == owner.nodeOwner {
		m.tail = owner.pred
		m.tail.succ = nil // TODO: consider removing this line for efficiency
	} else {
		owner.succ.pred = owner.pred
		owner.pred.succ = owner.succ
	}

	// Insert node at head position.
	owner.pred = nil // TODO: consider removing this line for efficiency
	owner.succ = m.head
	m.head.pred = owner.nodeOwner
	m.head = owner.nodeOwner

	m.listMutex.Unlock()
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

// TODO: introduce two different types for the NodeManager -- one to gain read access, another to get write access

func (r *NodeReference) GetReadAccess(source NodeSource) (shared.ReadHandle[Node], error) {
	owner := r.getLockedOwner(source)
	res, err := owner.getReadAccess(r.NodeId, source)
	owner.unlock()
	return res, err
}

func (r *NodeReference) GetWriteAccess(manager NodeManager) (shared.WriteHandle[Node], error) {
	owner := r.getLockedOwner(manager)
	res, err := owner.getWriteAccess(r.NodeId, manager)
	owner.unlock()
	return res, err
}

func (r *NodeReference) getLockedOwner(source NodeSource) lockedOwner {
	// check cached owner first
	if r.owner != nil {
		locked := r.owner.lock()

		// Only consider cached owner if it is still valid.
		if locked.isValid() {
			return locked
		}

		// Otherwise unlock the owner and refresh it with the code below.
		locked.unlock()
	}

	// Get the owner for this node from the manager and cache it in the reference.
	owner := source.getOwner(r.NodeId)
	r.owner = owner.nodeOwner
	return owner
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
	// mutex is to synchronize concurrent access to nodeOwners. Since owners may be
	// accessed concurrently, this lock is used to synchronize between those. It is
	// also used to provide exclusive access to the pool when it is necessary to
	// discard a node.
	mutex sync.Mutex

	id    NodeId               // the ID of the owned node
	node  *shared.Shared[Node] // the owned node, may be nil if it was not yet loaded from the disk.
	valid bool                 // if not valid, this owner should be ignored since it is no longer in its pool's registry.
	dirty bool                 // the owned node's state is different to the on-disk version

	// synchronized by the pool's list mutex
	pred *nodeOwner
	succ *nodeOwner
}

func (o *nodeOwner) lock() lockedOwner {
	o.mutex.Lock()
	return lockedOwner{o}
}

// lockedOwner is a reference to a owner for which the current thread is holding
// the respective lock. Since the lock needs to be acquired, held, and released
// in a non-trivial way in the NodePool implementation, this type is added to
// to make sure that operations on nodeOwners that depend on holding the lock are
// only conducted when the lock is actually held.
type lockedOwner struct {
	*nodeOwner
}

func (o *lockedOwner) unlock() *nodeOwner {
	o.mutex.Unlock()
	return o.nodeOwner
}

func (o *lockedOwner) isValid() bool {
	return o.valid
}

func (o *lockedOwner) isDirty() bool {
	return o.dirty
}

func (o *lockedOwner) getSharedNode(id NodeId, source NodeSource) (*shared.Shared[Node], error) {
	source.touch(*o)
	// If the node is present, it can be returned directly.
	res := o.node
	if res != nil {
		return res, nil
	}
	// Otherwise it needs to be fetched.
	node, err := source.load(id)
	if err != nil {
		return nil, err
	}
	o.node = node
	return node, err
}

func (o *lockedOwner) getReadAccess(id NodeId, source NodeSource) (shared.ReadHandle[Node], error) {
	node, err := o.getSharedNode(id, source)
	if err != nil {
		return shared.ReadHandle[Node]{}, err
	}
	return node.GetReadHandle(), nil
}

func (o *lockedOwner) getWriteAccess(id NodeId, manager NodeManager) (shared.WriteHandle[Node], error) {
	node, err := o.getSharedNode(id, manager)
	if err != nil {
		return shared.WriteHandle[Node]{}, err
	}
	return node.GetWriteHandle(), nil
}

func (o *lockedOwner) dropNode() {
	if o.node == nil {
		return
	}
	o.node = nil
	o.valid = false
}
