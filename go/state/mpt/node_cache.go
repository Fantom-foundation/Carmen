package mpt

import (
	"sync"
	"sync/atomic"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

type NodeReference struct {
	id  NodeId
	pos ownerPosition
	tag uint64
}

func NewNodeReference(id NodeId) NodeReference {
	return NodeReference{id: id, pos: unknownPosition}
}

func (r *NodeReference) Id() NodeId {
	return r.id
}

type NodeCache struct {
	owners     []nodeOwner
	capacity   ownerPosition
	index      map[NodeId]ownerPosition
	tagCounter uint64
	head       ownerPosition
	tail       ownerPosition
	mutex      sync.Mutex // for everything in the cache
}

func NewNodeCache(capacity int) *NodeCache {
	if capacity < 1 {
		capacity = 1
	}
	return &NodeCache{
		owners:   make([]nodeOwner, capacity),
		capacity: ownerPosition(capacity),
		index:    make(map[NodeId]ownerPosition, capacity),
	}
}

func (c *NodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	for {
		// Resolve the owner position if needed.
		if r.pos >= c.capacity {
			c.mutex.Lock()
			pos, found := c.index[r.id]
			if !found {
				c.mutex.Unlock()
				return nil, false
			}
			r.pos = pos
			r.tag = c.owners[pos].tag.Load()
			c.mutex.Unlock()
		}
		// Fetch the owner and check the tag.
		owner := &c.owners[r.pos]
		res := owner.node
		if owner.tag.Load() == r.tag {
			return res, true
		}
		r.pos = unknownPosition
	}
}

func (c *NodeCache) GetOrSet(
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

func (c *NodeCache) Touch(r *NodeReference) {
	if r.pos >= c.capacity {
		return
	}
	target := &c.owners[r.pos]
	c.mutex.Lock()
	if c.head == r.pos {
		c.mutex.Unlock()
		return
	}
	if c.tail == r.pos {
		c.tail = target.priv
	}

	c.owners[target.priv].next = target.next
	c.owners[target.next].priv = target.priv

	c.owners[c.head].priv = r.pos
	target.next = c.head
	c.head = r.pos
	c.mutex.Unlock()
}

func (c *NodeCache) getIdsInReverseEvictionOrder() []NodeId {
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
