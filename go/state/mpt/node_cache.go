package mpt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

type NodeReference struct {
	id  NodeId
	pos uint32
	tag uint64
}

func NewNodeReference(id NodeId) NodeReference {
	return NodeReference{id: id, pos: uint32(unknownPosition)}
}

func (r *NodeReference) Id() NodeId {
	return r.id
}

func (r *NodeReference) String() string {
	return r.id.String()
}

type NodeCache interface {
	Get(r *NodeReference) (*shared.Shared[Node], bool)
	GetOrSet(NodeId, *shared.Shared[Node]) (*shared.Shared[Node], bool, NodeId, *shared.Shared[Node], bool)
	Touch(r *NodeReference)
	GetMemoryFootprint() *common.MemoryFootprint
}

type nodeCache struct {
	owners     []nodeOwner
	capacity   ownerPosition
	index      map[NodeId]ownerPosition
	tagCounter uint64
	head       ownerPosition
	tail       ownerPosition
	mutex      sync.Mutex // for everything in the cache
}

func NewNodeCache(capacity int) NodeCache {
	return newNodeCache(capacity)
	/*
		return &shadowNodeCache{
			prime:  newNodeCache(capacity),
			shadow: &simpleNodeCache{index: map[NodeId]*shared.Shared[Node]{}},
		}
	*/
}

func newNodeCache(capacity int) NodeCache {
	if capacity < 1 {
		capacity = 1
	}
	return &nodeCache{
		owners:   make([]nodeOwner, capacity),
		capacity: ownerPosition(capacity),
		index:    make(map[NodeId]ownerPosition, capacity),
	}
}

func (c *nodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	pos := atomic.LoadUint32(&r.pos)
	tag := atomic.LoadUint64(&r.tag)
	for {
		// Resolve the owner position if needed.
		if ownerPosition(pos) >= c.capacity {
			c.mutex.Lock()
			position, found := c.index[r.id]
			if !found {
				c.mutex.Unlock()
				return nil, false
			}
			pos = uint32(position)
			tag = c.owners[pos].tag.Load()
			atomic.StoreUint32(&r.pos, uint32(position))
			atomic.StoreUint64(&r.tag, tag)
			c.mutex.Unlock()
		}
		// Fetch the owner and check the tag.
		owner := &c.owners[pos]
		res := owner.node
		id := owner.id
		if owner.tag.Load() == tag {
			// TODO: remove this
			if id != r.id {
				panic(fmt.Sprintf("reference resolution for node %v failed, got %v", r.id, id))
			}
			return res, true
		}
		pos = uint32(unknownPosition)
	}
}

func (c *nodeCache) GetOrSet(
	id NodeId,
	node *shared.Shared[Node],
) (
	current *shared.Shared[Node],
	present bool,
	evictedId NodeId,
	evictedNode *shared.Shared[Node],
	evicted bool,
) {
	c.mutex.Lock()
	// Lookup element - if present, we are done.
	if pos, found := c.index[id]; found {
		c.mutex.Unlock()
		return c.owners[pos].node, true, NodeId(0), nil, false
	}

	// If not present, the capacity needs to be checked.
	var pos ownerPosition
	var target *nodeOwner
	if len(c.index) >= int(c.capacity) {
		// an element needs to be evicted
		pos = c.tail

		target = &c.owners[pos]
		delete(c.index, target.id)
		c.tail = target.priv

		// remember the evicted node
		evictedId = target.id
		evictedNode = target.node
		evicted = true

	} else {
		// start using a new node from the owner list
		pos = ownerPosition(len(c.index))
		target = &c.owners[pos]
	}

	// update the owner to own the new ID and node
	c.tagCounter++
	target.tag.Store(c.tagCounter)
	target.id = id
	target.node = node

	// Move new owner to head of the LRU list.
	target.next = c.head
	c.owners[c.head].priv = pos
	c.head = pos

	c.index[id] = pos
	c.mutex.Unlock()
	return node, false, evictedId, evictedNode, evicted
}

func (c *nodeCache) Touch(r *NodeReference) {
	pos := ownerPosition(atomic.LoadUint32(&r.pos))
	if pos >= c.capacity {
		return
	}
	target := &c.owners[pos]
	c.mutex.Lock()
	if c.head == pos {
		c.mutex.Unlock()
		return
	}
	if c.tail == pos {
		c.tail = target.priv
	}

	c.owners[target.priv].next = target.next
	c.owners[target.next].priv = target.priv

	c.owners[c.head].priv = pos
	target.next = c.head
	c.head = pos
	c.mutex.Unlock()
}

func (c *nodeCache) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*c))
	mf.AddChild("owners", common.NewMemoryFootprint(unsafe.Sizeof(nodeOwner{})*uintptr(len(c.owners))))
	mf.AddChild("index", common.NewMemoryFootprint((unsafe.Sizeof(ownerPosition(0))+unsafe.Sizeof(NodeId(0)))*uintptr(len(c.index))))
	// TODO: add size of owned nodes
	return mf
}

func (c *nodeCache) getIdsInReverseEvictionOrder() []NodeId {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	res := make([]NodeId, 0, c.capacity)
	for cur := c.head; cur != c.tail; cur = c.owners[cur].next {
		res = append(res, c.owners[cur].id)
	}
	if c.owners[c.tail].tag.Load() > 0 {
		res = append(res, c.owners[c.tail].id)
	}
	return res
}

type nodeOwner struct {
	tag  atomic.Uint64
	id   NodeId
	node *shared.Shared[Node]
	priv ownerPosition
	next ownerPosition
}

type ownerPosition uint32

const unknownPosition = ownerPosition(0xFFFFFFFF)

type shadowNodeCache struct {
	prime, shadow NodeCache
}

func (s *shadowNodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	av, af := s.prime.Get(r)
	bv, bf := s.shadow.Get(r)
	if av != bv || af != bf {
		panic(fmt.Sprintf("inconsistent cache, wanted %p,%t, got %p,%t", bv, bf, av, af))
	}
	return av, af
}

func (s *shadowNodeCache) GetOrSet(id NodeId, value *shared.Shared[Node]) (*shared.Shared[Node], bool, NodeId, *shared.Shared[Node], bool) {
	av, af, ei, ev, e := s.prime.GetOrSet(id, value)
	bv, bf, _, _, _ := s.shadow.GetOrSet(id, value)
	if av != bv || af != bf {
		panic(fmt.Sprintf("inconsistent cache, wanted %p,%t, got %p,%t", bv, bf, av, af))
	}
	return av, af, ei, ev, e
}

func (s *shadowNodeCache) Touch(r *NodeReference) {
	s.prime.Touch(r)
	s.shadow.Touch(r)
}

func (s *shadowNodeCache) GetMemoryFootprint() *common.MemoryFootprint {
	return nil
}

type simpleNodeCache struct {
	index map[NodeId]*shared.Shared[Node]
}

func (s *simpleNodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	res, found := s.index[r.Id()]
	return res, found
}

func (s *simpleNodeCache) GetOrSet(id NodeId, value *shared.Shared[Node]) (*shared.Shared[Node], bool, NodeId, *shared.Shared[Node], bool) {
	if cur, found := s.index[id]; found {
		return cur, true, NodeId(0), nil, false
	}
	s.index[id] = value
	return value, false, NodeId(0), nil, false
}

func (s *simpleNodeCache) Touch(r *NodeReference) {
}
func (s *simpleNodeCache) GetMemoryFootprint() *common.MemoryFootprint {
	return nil
}
