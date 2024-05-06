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

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
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

func TestNodeCache_ReleaseChangesOrder(t *testing.T) {
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

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Release(&ref3)

	if want, got := "[V-2 V-1 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Release(&ref3)

	if want, got := "[V-2 V-1 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Release(&ref1)

	if want, got := "[V-2 V-3 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}
}

func TestNodeCache_Release_NoChange(t *testing.T) {
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

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	for i := 0; i < 3; i++ {
		cache.Release(&ref1)
		if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
			t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
		}
	}
}

func TestNodeCache_ReleaseAndTouch_ChangesOrder(t *testing.T) {
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

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Release(&ref3)

	if want, got := "[V-2 V-1 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Touch(&ref3)

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.Release(&ref3)

	if want, got := "[V-2 V-1 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
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
			return nil, fmt.Errorf("detected loop in LRU list after %v followed by %v", res, c.owners[cur].Id())
		}
		seen[cur] = struct{}{}
		res = append(res, c.owners[cur].Id())
	}
	if c.owners[c.tail].tag.Load() > 0 {
		res = append(res, c.owners[c.tail].Id())
	}
	return res, nil
}

func getBackwardLruList(c *nodeCache) ([]NodeId, error) {
	res := make([]NodeId, 0, len(c.owners))
	seen := map[ownerPosition]struct{}{}
	for cur := c.tail; cur != c.head; cur = c.owners[cur].prev {
		if _, contains := seen[cur]; contains {
			return nil, fmt.Errorf("detected loop in LRU list after %v followed by %v", res, c.owners[cur].Id())
		}
		seen[cur] = struct{}{}
		res = append(res, c.owners[cur].Id())
	}
	if c.owners[c.head].tag.Load() > 0 {
		res = append(res, c.owners[c.head].Id())
	}
	return res, nil
}

func TestNodeCache_ForEachEnumeratesAllEntries(t *testing.T) {
	const Capacity = 10
	cache := newNodeCache(Capacity)

	getAll := func() map[NodeId]*shared.Shared[Node] {
		res := map[NodeId]*shared.Shared[Node]{}
		cache.ForEach(func(id NodeId, node *shared.Shared[Node]) {
			res[id] = node
		})
		return res
	}

	want := map[NodeId]*shared.Shared[Node]{}
	if got := getAll(); !maps.Equal(want, got) {
		t.Errorf("invalid content, wanted %v, got %v", want, got)
	}

	for i := 0; i < 2*Capacity; i++ {
		ref := NewNodeReference(ValueId(uint64(i)))
		node := shared.MakeShared[Node](&ValueNode{})

		if _, _, evictedId, _, evicted := cache.GetOrSet(&ref, node); evicted {
			delete(want, evictedId)
		}

		want[ref.Id()] = node
		if got := getAll(); !maps.Equal(want, got) {
			t.Errorf("invalid content, wanted %v, got %v", want, got)
		}
	}
}

func TestNodeCache_GetAndSetThreadSafety(t *testing.T) {
	cache := newNodeCache(2)
	N := 100
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			id := ValueId(uint64(i))
			node := shared.MakeShared[Node](EmptyNode{})
			for j := 0; j < 1000; j++ {
				ref := NewNodeReference(id)
				got, _, _, _, _ := cache.GetOrSet(&ref, node)
				if got != node {
					t.Errorf("Invalid element in cache, wanted %p, got %p for ID %v", node, got, id)
				}
			}

		}(i)
	}
	wg.Wait()
}
