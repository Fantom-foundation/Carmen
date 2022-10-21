package eviction

type Policy interface {
	// Read informs the policy that a page slot has been read.
	Read(pageId int)
	// Written informs the policy that a page slot has been updated.
	Written(pageId int)
	// Removed informs the policy that a page slot has been removed from the cache.
	Removed(pageId int)
	// GetPageToEvict requests a slot to be evicted.
	GetPageToEvict() int
}
