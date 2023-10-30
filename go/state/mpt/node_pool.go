package mpt

import (
	"sync"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

//go:generate mockgen -source node_pool.go -destination node_pool_mocks.go -package mpt

type NodePoolSource interface {
	getSharedNode(NodeId) (*shared.Shared[Node], error)
}

type NodePool interface {
	// Provides the number of nodes currently retained by this pool.
	Size() int

	// Internal interfaces for NodeReferences to interact with a pool.
	touch(lockedOwner)
	get(NodeId) lockedOwner
	load(NodeId) (*shared.Shared[Node], error)
}

type nodePool struct {
	capacity int
	source   NodePoolSource

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

func NewNodePool(capacity int, source NodePoolSource) NodePool {
	// By default a node owner for the empty node is added to each pool.
	// This way, the pool is never empty, simplifying the code for manipulating
	// the LRU list of node owners.
	emptyNodeOwner := &nodeOwner{
		id:   EmptyId(),
		node: shared.MakeShared[Node](EmptyNode{}),
	}

	index := make(map[NodeId]*nodeOwner, capacity)
	index[EmptyId()] = emptyNodeOwner

	return &nodePool{
		capacity: capacity,
		size:     1,
		source:   source,
		index:    index,
		head:     emptyNodeOwner,
		tail:     emptyNodeOwner,
	}
}

func (p *nodePool) Size() int {
	p.listMutex.Lock()
	res := p.size
	p.listMutex.Unlock()
	return res
}

func (p *nodePool) get(id NodeId) lockedOwner {
	p.indexMutex.Lock()
	if owner, found := p.index[id]; found {
		res := owner.lock()
		p.touch(res)
		p.indexMutex.Unlock()
		return res
	}
	owner := &nodeOwner{id: id}
	p.index[id] = owner
	res := owner.lock()
	p.addWithHeldIndexMutex(res)
	p.indexMutex.Unlock()
	return res
}

func (p *nodePool) load(id NodeId) (*shared.Shared[Node], error) {
	node, err := p.source.getSharedNode(id)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (p *nodePool) addWithHeldIndexMutex(owner lockedOwner) {
	p.listMutex.Lock()

	// If needed, remove an element from the pool.
	if p.size >= p.capacity {
		lockedTail := p.tail.lock()
		lockedTail.dropNode()
		tail := lockedTail.unlock()
		delete(p.index, tail.id)
		p.tail = tail.pred
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

type NodeReference struct {
	id    NodeId // The ID of the referenced node.
	owner *nodeOwner
}

func NewNodeReference(id NodeId) *NodeReference {
	return &NodeReference{id: id}
}

func (r *NodeReference) Id() NodeId {
	if r == nil {
		return EmptyId()
	}
	return r.id
}

func (r *NodeReference) GetReadAccess(pool NodePool) (shared.ReadHandle[Node], error) {
	owner := r.getLockedOwner(pool)
	res, err := owner.getReadAccess(r.id, pool)
	owner.unlock()
	return res, err
}

func (r *NodeReference) GetWriteAccess(pool NodePool) (shared.WriteHandle[Node], error) {
	owner := r.getLockedOwner(pool)
	res, err := owner.getWriteAccess(r.id, pool)
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
	owner := pool.get(r.id)
	r.owner = owner.nodeOwner
	return owner
}

// nodeOwner is managing the presence of a single node in main memory.
type nodeOwner struct {
	// mutex is to synchronize concurrent access to nodeOwners. Since owners may be
	// accessed concurrently, this lock is used to synchronize between those. It is
	// also used to provide exclusive access to the pool when it is necessary to
	// discard a node.
	mutex sync.Mutex

	valid bool                 // if not valid, this owner should be ignored since it is no longer in its pool's registry.
	id    NodeId               // the ID of the owned node
	node  *shared.Shared[Node] // the owned node, may be nil if it was not yet loaded from the disk.

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

func (o *lockedOwner) getSharedNode(id NodeId, pool NodePool) (*shared.Shared[Node], error) {
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
	pool.touch(*o)
	node, err := o.getSharedNode(id, pool)
	if err != nil {
		return shared.ReadHandle[Node]{}, err
	}
	handle := node.GetReadHandle()
	return handle, err
}

func (o *lockedOwner) getWriteAccess(id NodeId, pool NodePool) (shared.WriteHandle[Node], error) {
	pool.touch(*o)
	node, err := o.getSharedNode(id, pool)
	if err != nil {
		return shared.WriteHandle[Node]{}, err
	}
	handle := node.GetWriteHandle()
	return handle, err
}

func (o *lockedOwner) dropNode() {
	if o.node == nil {
		return
	}

	// Acquire a read handle making sure nobody is writing to it anymore.
	_ = o.node.GetReadHandle()
	o.node = nil
	o.valid = false
}
