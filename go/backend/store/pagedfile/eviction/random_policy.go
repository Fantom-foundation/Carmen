package eviction

// RandomPolicy implements a random eviction policy. Pages are grouped into two categories:
// dirty pages and clean pages. When picking a page to be evicted, the clean
// pages are considered first. If there are clean pages, a random entry is
// selected. If there are non, a random entry from the dirty pages is selected.
type RandomPolicy struct {
	clean FlatSet
	dirty FlatSet
}

func NewRandomPolicy(capacity int) *RandomPolicy {
	return &RandomPolicy{
		clean: NewFlatSet(capacity),
		dirty: NewFlatSet(capacity),
	}
}

// Read informs the policy that a page slot has been read.
func (re *RandomPolicy) Read(pageId int) {
	if !re.dirty.Contains(pageId) {
		re.clean.Add(pageId)
	}
}

// Written informs the policy that a page slot has been updated.
func (re *RandomPolicy) Written(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Add(pageId)
}

// Removed informs the policy that a page slot has been removed from the cache.
func (re *RandomPolicy) Removed(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Remove(pageId)
}

// GetPageToEvict requests a slot to be evicted.
func (re *RandomPolicy) GetPageToEvict() int {
	if !re.clean.IsEmpty() {
		return re.clean.PickRandom()
	}
	if !re.dirty.IsEmpty() {
		return re.dirty.PickRandom()
	}
	return -1
}
