package mpt

import (
	"errors"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

//go:generate mockgen -source node_manager.go -destination node_manager_mocks.go -package mpt

type NodeStore interface {
	// Load requests the given node to be loaded from an underlying storage system.
	Load(NodeId) (Node, error)

	// Store requests the given node to be stored to the underlying storage system.
	Store(NodeId, Node) error

	GetFreshAccountId() (NodeId, error)
	GetFreshBranchId() (NodeId, error)
	GetFreshExtensionId() (NodeId, error)
	GetFreshValueId() (NodeId, error)

	Flush() error

	Close() error
}

type NodePool interface {
	// Provides the number of nodes currently retained by this pool.
	Size() int

	CreateAccount() (NodeReference, error)
	CreateBranch() (NodeReference, error)
	CreateExtension() (NodeReference, error)
	CreateValue() (NodeReference, error)

	Flush() error
	Close() error

	// Internal interfaces for NodeReferences to interact with a pool.
	touch(lockedOwner)
	getOwner(NodeId) lockedOwner
	load(NodeId) (*shared.Shared[Node], error)
}

type nodePool struct {
	capacity    int
	store       NodeStore
	writeBuffer WriteBuffer

	// The pool retains a mapping from node IDs to nodeOwners. A node
	// owner is an internal object that controls the presence of a given
	// node in memory. All node references with the same node ID within
	// a single forest are required to point to the master nodeOwner
	// indexed in this map.
	index      map[NodeId]*nodeOwner
	indexMutex sync.Mutex

	// The following fields define a double-linked list of node owners
	// in their least-recently-used order. Each access to a node owner
	// triggers an update of this list to retain the LRU order. The
	// size of the list is limited to the capacity of the pool.
	size      int
	head      *nodeOwner
	tail      *nodeOwner
	listMutex sync.Mutex
}

// TODO: add support for write buffers

func NewNodePool(capacity int, store NodeStore) NodePool {
	// By default a node owner for the empty node is added to each pool.
	// This way, the pool is never empty, simplifying the code for manipulating
	// the LRU list of node owners.
	emptyNodeOwner := &nodeOwner{
		valid: true,
		id:    EmptyId(),
		node:  shared.MakeShared[Node](EmptyNode{}),
	}

	index := make(map[NodeId]*nodeOwner, capacity)
	index[EmptyId()] = emptyNodeOwner

	writeBuffer := MakeWriteBuffer(nodeStoreToSinkAdapter{store})

	return &nodePool{
		capacity:    capacity,
		store:       store,
		writeBuffer: writeBuffer,
		size:        1,
		index:       index,
		head:        emptyNodeOwner,
		tail:        emptyNodeOwner,
	}
}

func (p *nodePool) Size() int {
	p.listMutex.Lock()
	res := p.size
	p.listMutex.Unlock()
	return res
}

func (p *nodePool) CreateAccount() (NodeReference, error) {
	id, err := p.store.GetFreshAccountId()
	if err != nil {
		return NodeReference{}, err
	}
	return p.createNode(id, &AccountNode{})
}

func (p *nodePool) CreateBranch() (NodeReference, error) {
	id, err := p.store.GetFreshBranchId()
	if err != nil {
		return NodeReference{}, err
	}
	return p.createNode(id, &BranchNode{})
}

func (p *nodePool) CreateExtension() (NodeReference, error) {
	id, err := p.store.GetFreshExtensionId()
	if err != nil {
		return NodeReference{}, err
	}
	return p.createNode(id, &ExtensionNode{})
}

func (p *nodePool) CreateValue() (NodeReference, error) {
	id, err := p.store.GetFreshValueId()
	if err != nil {
		return NodeReference{}, err
	}
	return p.createNode(id, &ValueNode{})
}

func (p *nodePool) createNode(id NodeId, node Node) (NodeReference, error) {
	owner := &nodeOwner{
		valid: true,
		id:    id,
		node:  shared.MakeShared[Node](node),
	}
	lockedOwner := owner.lock()
	p.indexMutex.Lock()
	p.addWithHeldIndexMutex(lockedOwner)
	p.indexMutex.Unlock()
	return NodeReference{
		NodeId: id,
		owner:  owner,
	}, nil
}

func (p *nodePool) Flush() error {

	// TODO: flush out all dirty nodes!

	return errors.Join(
		p.writeBuffer.Flush(),
		p.store.Flush(),
	)
}
func (p *nodePool) Close() error {
	return errors.Join(
		p.Flush(),
		p.writeBuffer.Close(),
		p.store.Close(),
	)
}

func (p *nodePool) addWithHeldIndexMutex(owner lockedOwner) {
	p.listMutex.Lock()

	// If needed, remove an element from the pool.
	if p.size >= p.capacity {

		if p.tail.dirty {
			// Dirty nodes need to be moved from the LRU list to the write buffer.
			p.writeBuffer.Add(p.tail.id, p.tail.node)

			// TODO: we need a callback from the write buffer when the write is completed to invalidate the owner.

		} else {
			// If the tail node is not dirty, it can be dropped instantly.
			p.dropNode(p.tail)
		}

		p.tail = p.tail.pred
		p.tail.succ = nil // TODO: consider removing this line
	} else {
		p.size++
	}

	owner.pred = nil
	owner.succ = p.head
	p.head.pred = owner.nodeOwner
	p.head = owner.nodeOwner
	p.listMutex.Unlock()
}

func (p *nodePool) dropNode(owner *nodeOwner) {
	// TODO:
	//  - remove the node from its owner
	//  - set the owner invalid
	//  - remove the owner from the owner index

	// TODO: check required locking ...
	p.indexMutex.Lock()
	delete(p.index, owner.id)
	p.indexMutex.Unlock()
}

func (p *nodePool) getOwner(id NodeId) lockedOwner {
	p.indexMutex.Lock()
	if owner, found := p.index[id]; found {
		res := owner.lock()
		p.touch(res)
		p.indexMutex.Unlock()
		return res
	}
	owner := &nodeOwner{
		valid: true,
		id:    id,
	}
	p.index[id] = owner
	res := owner.lock()
	p.addWithHeldIndexMutex(res)
	p.indexMutex.Unlock()
	return res
}

func (p *nodePool) load(id NodeId) (*shared.Shared[Node], error) {
	node, err := p.store.Load(id)
	if err != nil {
		return nil, err
	}
	return shared.MakeShared[Node](node), nil
}

func (p *nodePool) touch(owner lockedOwner) {

	p.listMutex.Lock()

	// If the node is the current head there is nothing to do.
	if p.head == owner.nodeOwner {
		p.listMutex.Unlock()
		return
	}

	// Remove node from current position.
	if p.tail == owner.nodeOwner {
		p.tail = owner.pred
		p.tail.succ = nil // TODO: consider removing this line
	} else {
		owner.succ.pred = owner.pred
		owner.pred.succ = owner.succ
	}

	// Insert node at head position.
	owner.pred = nil // TODO: consider removing this line
	owner.succ = p.head
	p.head.pred = owner.nodeOwner
	p.head = owner.nodeOwner

	p.listMutex.Unlock()
}

type nodeStoreToSinkAdapter struct {
	store NodeStore
}

func (a nodeStoreToSinkAdapter) Write(id NodeId, handle shared.ReadHandle[Node]) error {
	return a.store.Store(id, handle.Get())
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

// TODO: introduce two different types for the NodePool -- one to gain read access, another to get write access

func (r *NodeReference) GetReadAccess(pool NodePool) (shared.ReadHandle[Node], error) {
	owner := r.getLockedOwner(pool)
	res, err := owner.getReadAccess(r.NodeId, pool)
	owner.unlock()
	return res, err
}

func (r *NodeReference) GetWriteAccess(pool NodePool) (shared.WriteHandle[Node], error) {
	owner := r.getLockedOwner(pool)
	res, err := owner.getWriteAccess(r.NodeId, pool)
	owner.unlock()
	return res, err
}

func (r *NodeReference) getLockedOwner(pool NodePool) lockedOwner {
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

	// Get the owner for this node from the pool and cache it in the reference.
	owner := pool.getOwner(r.NodeId)
	r.owner = owner.nodeOwner
	return owner
}

// ------------------------------ Node Owner ----------------------------------

// nodeOwner is managing the presence of a single node in main memory. A node owner
// has a 3-step live cycle:
//   - empty: the initial state, where an ID is set, the owner is the canonical
//     owner of the node associated to this ID, but the node is not in
//     memory yet (the node reference is nil)
//   - set: the owner is the canonical owner of the node associated to the
//     contained ID and the node is indeed in memory and referenced by
//     this node
//   - dropped: the owned node was dropped and the owner is marked invalid. It is
//     no longer the canonical owner of the associated node and should be
//     ignored and forgotten by node references
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

func (o *lockedOwner) getSharedNode(id NodeId, pool NodePool) (*shared.Shared[Node], error) {
	pool.touch(*o)
	// If the node is present, it can be returned directly.
	res := o.node
	if res != nil {
		return res, nil
	}
	// Otherwise it needs to be fetched.
	node, err := pool.load(id)
	if err != nil {
		return nil, err
	}
	o.node = node
	return node, err
}

func (o *lockedOwner) getReadAccess(id NodeId, pool NodePool) (shared.ReadHandle[Node], error) {
	node, err := o.getSharedNode(id, pool)
	if err != nil {
		return shared.ReadHandle[Node]{}, err
	}
	return node.GetReadHandle(), nil
}

func (o *lockedOwner) getWriteAccess(id NodeId, pool NodePool) (shared.WriteHandle[Node], error) {
	node, err := o.getSharedNode(id, pool)
	if err != nil {
		return shared.WriteHandle[Node]{}, err
	}
	return node.GetWriteHandle(), nil
}

func (o *lockedOwner) dropNode() {
	if o.node == nil {
		return
	}

	// Acquire a read handle making sure nobody is writing to it anymore.
	// TODO: consider making this a write handle
	_ = o.node.GetReadHandle()
	o.node = nil
	o.valid = false
}
