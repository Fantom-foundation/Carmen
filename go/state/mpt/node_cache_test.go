package mpt

import (
	"fmt"
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
	if _, present, _, _, evicted := cache.GetOrSet(ref.Id(), node); present || evicted {
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

func TestNodeCache_ElementsAreRetainedInLruOrder(t *testing.T) {
	cache := NewNodeCache(3)

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

	if _, present, _, _, evicted := cache.GetOrSet(ref1.Id(), node1); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, _, _, evicted := cache.GetOrSet(ref2.Id(), node2); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, _, _, evicted := cache.GetOrSet(ref3.Id(), node3); present || evicted {
		t.Errorf("insertion failed, present %t, evicted %t", present, evicted)
	}

	if want, got := "[V-3 V-2 V-1]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(ref4.Id(), node4); present || !evicted || evictedId != ref1.Id() || evictedNode != node1 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-4 V-3 V-2]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(ref5.Id(), node5); present || !evicted || evictedId != ref2.Id() || evictedNode != node2 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-5 V-4 V-3]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(ref1.Id(), node1); present || !evicted || evictedId != ref3.Id() || evictedNode != node3 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-1 V-5 V-4]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	if _, present, evictedId, evictedNode, evicted := cache.GetOrSet(ref2.Id(), node2); present || !evicted || evictedId != ref4.Id() || evictedNode != node4 {
		t.Errorf("insertion failed, present %t, evicted %t, evictedId %v, evicted node %p", present, evicted, evictedId, evictedNode)
	}

	if want, got := "[V-2 V-1 V-5]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}
}

func TestNodeCache_TouchChangesOrder(t *testing.T) {
	cache := NewNodeCache(3)

	ref1 := NewNodeReference(ValueId(1))
	ref2 := NewNodeReference(ValueId(2))
	ref3 := NewNodeReference(ValueId(3))

	if want, got := "[]", fmt.Sprintf("%v", cache.getIdsInReverseEvictionOrder()); want != got {
		t.Errorf("unexpected eviction order, wanted %s, got %s", want, got)
	}

	cache.GetOrSet(ref1.Id(), nil)
	cache.GetOrSet(ref2.Id(), nil)
	cache.GetOrSet(ref3.Id(), nil)
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
