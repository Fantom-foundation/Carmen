package mpt

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

// NodeReference is used to address a node within an MPT. The node is
// identified by a NodeId and may either be in memory or on disk. In
// combination with a NodeCache, a reference can be used to resolve and
// access nodes while navigating an MPT instance. Internally, references
// cache information enabling efficient access to nodes.
// NOTE: NodeReferences must be consistently used with a single NodeCache
// instance. Mixing references to nodes in different caches can lead to
// failures and corrupted content.
type NodeReference struct {
	id  NodeId // the ID of the referenced node
	pos uint32 // the position of the node within the cache
	tag uint64 // a tag used to invalidate references on cache changes
}

// NewNodeReference creates a new node reference pointing to the addressed
// Node.
func NewNodeReference(id NodeId) NodeReference {
	return NodeReference{id: id, pos: uint32(unknownPosition)}
}

func (r *NodeReference) Id() NodeId {
	return r.id
}

func (r *NodeReference) String() string {
	return r.id.String()
}

// NodeCache is managing the life cycle of nodes in memory and limits the
// overall memory usage of nodes retained. Nodes can be accessed through
// NodeReferences. All accesses are thread safe.
type NodeCache interface {
	// Get tries to resolve the given node reference, returning the
	// corresponding node or nil if not found.
	Get(r *NodeReference) (node *shared.Shared[Node], found bool)

	// GetOrSet attempts to bind a new node to a given reference. If a node is
	// already bound to the referenced ID, the present value is returned.
	// Otherwise the provided node is registered in the cache and returned.
	// If the insertion causes a node to be evicted, the evicted node's ID,
	// the node itself, and a boolean flag indicating the eviction is returned.
	GetOrSet(*NodeReference, *shared.Shared[Node]) (
		after *shared.Shared[Node],
		present bool,
		evictedId NodeId,
		evictedNode *shared.Shared[Node],
		evicted bool,
	)

	// Touch signals the cache that the given node has been used. This signals
	// are used by implementation to manage the eviction order of elements.
	Touch(r *NodeReference)

	// ForEach iterates through all elements in this cache.
	ForEach(func(NodeId, *shared.Shared[Node]))

	// MemoryFootprintProvider is embedded to require implementations to
	// produces a summary of the overall memory usage of this cache, including
	// the size of all owned node instances.
	common.MemoryFootprintProvider
}

// nodeCache implements the NodeCache interface using a fixed capacity cache
// of nodes and an LRU policy for evicting nodes.
//
// Internally, this implementation maintains a list of node-owners, each
// equipped with a tag to indicate mutations. Node references retain the
// position of referenced nodes in the list of owners and the tag. When
// resolving a node through a reference, the position and tag enable a direct,
// lock free lookup of the targeted node.
type nodeCache struct {
	owners     []nodeOwner              // fixed length list of all owned nodes
	index      map[NodeId]ownerPosition // an index on the owned nodes
	tagCounter uint64                   // a counter to generate fresh tags
	head       ownerPosition            // head of the LRU list of owners
	tail       ownerPosition            // tail of the LRU list of owners
	mutex      sync.Mutex               // for everything except the owner list
}

func NewNodeCache(capacity int) NodeCache {
	return newNodeCache(capacity)
}

func newNodeCache(capacity int) *nodeCache {
	if capacity < 1 {
		capacity = 1
	}
	return &nodeCache{
		owners: make([]nodeOwner, capacity),
		index:  make(map[NodeId]ownerPosition, capacity),
	}
}

func (c *nodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	// Node references cache the position of the owner retaining the referenced
	// node such that lookups are reduced to simple array lookups. However, at
	// any time the cache may chose to evict an element and replace it with
	// another. To do so, the owner at the corresponding position is simply
	// updated. To allow node references to identify situations in which the
	// referenced node got evicted, an additional tag is stored. This tag is
	// incremented every time an owner is recycled, allowing references to
	// identify modifications.
	pos := atomic.LoadUint32(&r.pos)
	tag := atomic.LoadUint64(&r.tag)
	for {
		// Resolve the owner position if needed.
		if pos >= uint32(len(c.owners)) {
			c.mutex.Lock()
			position, found := c.index[r.id]
			if !found {
				c.mutex.Unlock()
				return nil, false
			}
			pos = uint32(position)
			tag = c.owners[pos].tag.Load()
			atomic.StoreUint32(&r.pos, pos)
			atomic.StoreUint64(&r.tag, tag)
			c.mutex.Unlock()
		}
		// Fetch the owner and check the tag.
		owner := &c.owners[pos]
		res := owner.Node()
		// Check that the tag is still correct and the fetched result is valid.
		if owner.tag.Load() == tag {
			return res, true
		}
		// If the tag has changed the position is out-dated and the true owner
		// needs to be resolved through the index.
		pos = uint32(unknownPosition)
	}
}

func (c *nodeCache) GetOrSet(
	ref *NodeReference,
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
	if pos, found := c.index[ref.id]; found {
		current := c.owners[pos].Node()
		c.mutex.Unlock()
		atomic.StoreUint32(&ref.pos, uint32(pos))
		atomic.StoreUint64(&ref.tag, c.owners[pos].tag.Load())
		return current, true, NodeId(0), nil, false
	}

	// If not present, the capacity needs to be checked.
	var pos ownerPosition
	var target *nodeOwner
	if len(c.index) >= len(c.owners) {
		// an element needs to be evicted
		pos = c.tail

		target = &c.owners[pos]
		delete(c.index, target.Id())
		c.tail = target.prev

		// remember the evicted node
		evictedId = target.Id()
		evictedNode = target.Node()
		evicted = true

	} else {
		// start using a new node from the owner list
		pos = ownerPosition(len(c.index))
		target = &c.owners[pos]
	}

	// update the owner to own the new ID and node
	c.tagCounter++
	target.tag.Store(c.tagCounter)
	target.id.Store(uint64(ref.Id()))
	target.node.Store(node)

	// Move new owner to head of the LRU list.
	target.next = c.head
	c.owners[c.head].prev = pos
	c.head = pos

	c.index[ref.Id()] = pos
	c.mutex.Unlock()
	atomic.StoreUint32(&ref.pos, uint32(pos))
	atomic.StoreUint64(&ref.tag, c.owners[pos].tag.Load())
	return node, false, evictedId, evictedNode, evicted
}

func (c *nodeCache) Touch(r *NodeReference) {
	// During a touch we need to update the double-linked list
	// formed by owners such that the referenced node is at the
	// head position.
	pos := ownerPosition(atomic.LoadUint32(&r.pos))
	if uint32(pos) >= uint32(len(c.owners)) {
		// In this reference does not point to a valid owner; the
		// reference is not extra resolved to perform a touch, and
		// thus the operation can stop here.
		return
	}
	target := &c.owners[pos]
	c.mutex.Lock()
	if c.head == pos {
		c.mutex.Unlock()
		return
	}
	if c.tail == pos {
		c.tail = target.prev
	} else {
		c.owners[target.next].prev = target.prev
	}
	c.owners[target.prev].next = target.next

	c.owners[c.head].prev = pos
	target.next = c.head
	c.head = pos
	c.mutex.Unlock()
}

func (c *nodeCache) ForEach(consume func(NodeId, *shared.Shared[Node])) {
	for i := 0; i < len(c.owners); i++ {
		cur := &c.owners[i]
		for {
			tag := cur.tag.Load()
			if tag == 0 {
				break
			}
			id := cur.Id()
			node := cur.Node()
			if tag != cur.tag.Load() {
				// The owner was updated in the meantime, repeat.
				continue
			}
			consume(id, node)
			break
		}
	}
}

func (c *nodeCache) GetMemoryFootprint() *common.MemoryFootprint {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*c))
	mf.AddChild("owners", common.NewMemoryFootprint(unsafe.Sizeof(nodeOwner{})*uintptr(len(c.owners))))
	mf.AddChild("index", common.NewMemoryFootprint((unsafe.Sizeof(ownerPosition(0))+unsafe.Sizeof(NodeId(0)))*uintptr(len(c.index))))

	emptySize := unsafe.Sizeof(EmptyNode{})
	branchSize := unsafe.Sizeof(BranchNode{})
	extensionSize := unsafe.Sizeof(ExtensionNode{})
	accountSize := unsafe.Sizeof(AccountNode{})
	valueSize := unsafe.Sizeof(ValueNode{})

	size := uintptr(0)
	for i := 0; i < len(c.owners); i++ {
		cur := &c.owners[i]
		if cur.Node() == nil {
			continue
		}
		id := cur.Id()
		if id.IsEmpty() {
			size += emptySize
		} else if id.IsBranch() {
			size += branchSize
		} else if id.IsValue() {
			size += valueSize
		} else if id.IsAccount() {
			size += accountSize
		} else if id.IsExtension() {
			size += extensionSize
		}

	}
	mf.AddChild("nodes", common.NewMemoryFootprint(size))
	return mf
}

func (c *nodeCache) getIdsInReverseEvictionOrder() []NodeId {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	res := make([]NodeId, 0, len(c.owners))
	for cur := c.head; cur != c.tail; cur = c.owners[cur].next {
		res = append(res, c.owners[cur].Id())
	}
	if c.owners[c.tail].tag.Load() > 0 {
		res = append(res, c.owners[c.tail].Id())
	}
	return res
}

// nodeOwner is a single entry of the node cache. It servers two roles:
// - provide synchronized access to an owned node
// - be an element of a LRU list to manage eviction order
type nodeOwner struct {
	tag  atomic.Uint64                       // a tag vor versioning the owned node
	id   atomic.Uint64                       // the ID of the owned node (protected by seq lock, but atomic for race detection check)
	node atomic.Pointer[shared.Shared[Node]] // the owned node (protected by seq lock, but atomic for race detection check)
	prev ownerPosition                       // predecessor in the LRU list
	next ownerPosition                       // successor in the LRU list
}

func (o *nodeOwner) Id() NodeId {
	return NodeId(o.id.Load())
}

func (o *nodeOwner) Node() *shared.Shared[Node] {
	return o.node.Load()
}

type ownerPosition uint32

const unknownPosition = ownerPosition(0xFFFFFFFF)
