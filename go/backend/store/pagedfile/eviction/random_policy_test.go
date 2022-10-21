package eviction

import "testing"

func TestRandomEvictionPolicy(t *testing.T) {
	p := NewRandomPolicy(5)
	if p.GetPageToEvict() != -1 {
		t.Errorf("initial page to evict not nil")
	}
	p.Read(10)
	p.Written(11)
	// clean pages should be prioritized for the eviction over dirty pages
	if p.GetPageToEvict() != 10 {
		t.Errorf("page to evict not 10 - clean pages are not prioritized")
	}
	p.Removed(10)
	if p.GetPageToEvict() != 11 {
		t.Errorf("page to evict not 11")
	}
	p.Removed(11)
	if p.GetPageToEvict() != -1 {
		t.Errorf("page to evict not nil")
	}
}
