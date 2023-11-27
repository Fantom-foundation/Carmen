package mpt

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

func TestNodeCache_ElementsCanBeStoredAndRetrieved(t *testing.T) {
	cache := NewNodeCache(10)

	ref := NewNodeReference(EmptyId())
	if _, found := cache.Get(&ref); found {
		t.Errorf("empty cache should not contain any element, found %v", ref)
	}

	node := shared.MakeShared[Node](EmptyNode{})
	if _, present, _, _, evicted := cache.GetOrSet(&ref, node); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if got, found := cache.Get(&ref); !found || got != node {
		t.Errorf("failed to retrieve element for %v, found %t, want %p, got %p", ref, found, node, got)
	}
	if ref.tag == 0 {
		t.Errorf("reference has not been tagged during lookup")
	}

	// Lookup also works for a reference not caching the owner
	ref2 := NewNodeReference(EmptyId())
	if got, found := cache.Get(&ref2); !found || got != node {
		t.Errorf("failed to retrieve element for %v, found %t, want %p, got %p", ref, found, node, got)
	}
}

func TestNodeCache_GetOrSetReturnsCurrent(t *testing.T) {
	cache := NewNodeCache(10)

	ref := NewNodeReference(EmptyId())
	if _, found := cache.Get(&ref); found {
		t.Errorf("empty cache should not contain any element, found %v", ref)
	}

	node1 := shared.MakeShared[Node](EmptyNode{})
	if res, present, _, _, evicted := cache.GetOrSet(&ref, node1); present || evicted || res != node1 {
		t.Errorf("insertion failed, present %t, evicted %t, wanted %p, got %p", present, evicted, node1, res)
	}

	node2 := shared.MakeShared[Node](EmptyNode{})
	if res, present, _, _, evicted := cache.GetOrSet(&ref, node2); !present || evicted || res != node1 {
		t.Errorf("insertion failed, present %t, evicted %t, wanted %p, got %p", present, evicted, node1, res)
	}
}

func TestNodeCache_ElementsAreRetainedInLruOrder(t *testing.T) {
	cache := NewNodeCache(3).(*nodeCache)

	ref1 := NewNodeReference(ValueId(1))
	ref2 := NewNodeReference(ValueId(2))
	ref3 := NewNodeReference(ValueId(3))
	ref4 := NewNodeReference(ValueId(4))
	ref5 := NewNodeReference(ValueId(5))

	node1 := shared.MakeShared[Node](EmptyNode{})
	node2 := shared.MakeShared[Node](EmptyNode{})
	node3 := shared.MakeShared[Node](EmptyNode{})
	node4 := shared.MakeShared[Node](EmptyNode{})
	node5 := shared.MakeShared[Node](EmptyNode{})

	if want, got := "[]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, _, _, evicted := cache.GetOrSet(&ref1, node1); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, _, _, evicted := cache.GetOrSet(&ref2, node2); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, _, _, evicted := cache.GetOrSet(&ref3, node3); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(&ref4, node4); present || !evicted || evictedId != ref1.Id() || evictedNode != node1 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-4 V-3 V-2]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(&ref5, node5); present || !evicted || evictedId != ref2.Id() || evictedNode != node2 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-5 V-4 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(&ref1, node1); present || !evicted || evictedId != ref3.Id() || evictedNode != node3 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-1 V-5 V-4]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(&ref2, node2); present || !evicted || evictedId != ref4.Id() || evictedNode != node4 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-2 V-1 V-5]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}
}

func TestNodeCache_TouchChangesOrder(t *testing.T) {
	cache := NewNodeCache(3).(*nodeCache)

	ref1 := NewNodeReference(ValueId(1))
	ref2 := NewNodeReference(ValueId(2))
	ref3 := NewNodeReference(ValueId(3))

	if want, got := "[]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.GetOrSet(&ref1, nil)
	cache.GetOrSet(&ref2, nil)
	cache.GetOrSet(&ref3, nil)
	cache.Get(&ref1)
	cache.Get(&ref2)
	cache.Get(&ref3)

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Touch(&ref1)

	if want, got := "[V-1 V-3 V-2]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Touch(&ref3)

	if want, got := "[V-3 V-1 V-2]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Touch(&ref3)

	if want, got := "[V-3 V-1 V-2]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}
}

func TestNodeCache_StressTestLruList(t *testing.T) {
	const Capacity = 10
	cache := newNodeCache(Capacity)

	should := []NodeId{}

	// fill cache with elements
	refs := []NodeReference{}
	for i := 0; i < Capacity; i++ {
		id := ValueId(uint64(i))
		ref := NewNodeReference(id)
		cache.GetOrSet(&ref, nil)
		refs = append(refs, ref)
	}

	// touch elements in cache and check LRU consistency
	r := rand.New(rand.NewSource(123))
	for i := 0; i < 1000; i++ {
		pos := int(r.Int31n(Capacity))
		ref := refs[pos]
		cache.Touch(&ref)

		refs = append(refs, ref)
		refs = slices.Delete(refs, pos, pos+1)

		should = should[0:0]
		for _, ref := range refs {
			should = append(should, ref.Id())
		}
		slices.Reverse(should)
		fmt.Printf("%3d - %v - %v\n", i, ref.Id(), should)

		forward, err := getForwardLruList(cache)
		if err != nil {
			t.Fatalf("failed to get forward list order: %v", err)
		}

		backward, err := getBackwardLruList(cache)
		if err != nil {
			t.Fatalf("failed to get backward list order: %v", err)
		}

		slices.Reverse(backward)
		if !slices.Equal(forward, backward) {
			t.Fatalf("inconsistent list order: %v vs %v", forward, backward)
		}

		if !slices.Equal(should, forward) {
			t.Fatalf("unexpected list, wanted %v, got %v", should, forward)
		}
	}
}

func getForwardLruList(c *nodeCache) ([]NodeId, error) {
	res := make([]NodeId, 0, len(c.owners))
	seen := map[ownerPosition]struct{}{}
	for cur := c.head; cur != c.tail; cur = c.owners[cur].next {
		if _, contains := seen[cur]; contains {
			return nil, fmt.Errorf("detected loop in LRU list after %v followed by %v", res, c.owners[cur].id)
		}
		seen[cur] = struct{}{}
		res = append(res, c.owners[cur].id)
	}
	if c.owners[c.tail].tag.Load() > 0 {
		res = append(res, c.owners[c.tail].id)
	}
	return res, nil
}

func getBackwardLruList(c *nodeCache) ([]NodeId, error) {
	res := make([]NodeId, 0, len(c.owners))
	seen := map[ownerPosition]struct{}{}
	for cur := c.tail; cur != c.head; cur = c.owners[cur].prev {
		if _, contains := seen[cur]; contains {
			return nil, fmt.Errorf("detected loop in LRU list after %v followed by %v", res, c.owners[cur].id)
		}
		seen[cur] = struct{}{}
		res = append(res, c.owners[cur].id)
	}
	if c.owners[c.head].tag.Load() > 0 {
		res = append(res, c.owners[c.head].id)
	}
	return res, nil
}
