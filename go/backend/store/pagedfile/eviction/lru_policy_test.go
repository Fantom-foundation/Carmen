package eviction

import "testing"

func TestLruInOrderReadsAreEvictedInOrder(t *testing.T) {
	p := NewLeastRecentlyUsedEvictionPolicy(5)
	if p.GetPageToEvict() != -1 {
		t.Errorf("initial page to evict not nil")
	}

	for i := 0; i < 5; i++ {
		p.Read(i)
	}

	// pages should be evicted in same order as added
	for i := 0; i < 5; i++ {
		if p.GetPageToEvict() != i {
			t.Errorf("pages not evicted in same order as added (%d != %d)", p.GetPageToEvict(), i)
		}
		p.Removed(i)
	}
}

func TestLruLeastRecentlyAreEvicted(t *testing.T) {
	p := NewLeastRecentlyUsedEvictionPolicy(5)
	if p.GetPageToEvict() != -1 {
		t.Errorf("not evicted correctly")
	}
	p.Read(1) // now: 1
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Read(2) // now: 2, 1
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Read(3) // now: 3, 2, 1
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}

	// Access last
	p.Read(1) // now: 1, 3, 2
	if p.GetPageToEvict() != 2 {
		t.Errorf("not evicted correctly")
	}

	// Access middle
	p.Read(3) // now 3, 1, 2
	if p.GetPageToEvict() != 2 {
		t.Errorf("not evicted correctly")
	}

	p.Read(3) // now 3, 1, 2
	if p.GetPageToEvict() != 2 {
		t.Errorf("not evicted correctly")
	}

	// Check order
	p.Read(2) // now 2, 3, 1
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Read(1) // now 1, 2, 3
	if p.GetPageToEvict() != 3 {
		t.Errorf("not evicted correctly")
	}
}

func TestLruLastElementCanBeRemoved(t *testing.T) {
	p := NewLeastRecentlyUsedEvictionPolicy(5)
	p.Read(1)
	p.Read(2)
	p.Read(3)

	p.Removed(1)
	if p.GetPageToEvict() != 2 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(2)
	if p.GetPageToEvict() != 3 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(3)
	if p.GetPageToEvict() != -1 {
		t.Errorf("not evicted correctly")
	}
}

func TestLruFirstElementCanBeRemoved(t *testing.T) {
	p := NewLeastRecentlyUsedEvictionPolicy(5)
	p.Read(1)
	p.Read(2)
	p.Read(3)

	p.Removed(3)
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(2)
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(1)
	if p.GetPageToEvict() != -1 {
		t.Errorf("not evicted correctly")
	}
}

func TestLruMiddleElementCanBeRemoved(t *testing.T) {
	p := NewLeastRecentlyUsedEvictionPolicy(5)
	p.Read(1)
	p.Read(2)
	p.Read(3)

	p.Removed(2)
	if p.GetPageToEvict() != 1 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(1)
	if p.GetPageToEvict() != 3 {
		t.Errorf("not evicted correctly")
	}
	p.Removed(3)
	if p.GetPageToEvict() != -1 {
		t.Errorf("not evicted correctly")
	}
}
