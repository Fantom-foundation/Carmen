package eviction

type LeastRecentlyUsedEvictionPolicy struct {
	entries map[int]*LruEntry
	head    *LruEntry
	tail    *LruEntry
}

func NewLeastRecentlyUsedEvictionPolicy(capacity int) Policy {
	return &LeastRecentlyUsedEvictionPolicy{
		entries: make(map[int]*LruEntry, capacity),
		head:    nil,
		tail:    nil,
	}
}

type LruEntry struct {
	pageId int
	succ   *LruEntry
	pred   *LruEntry
}

func (lru *LeastRecentlyUsedEvictionPolicy) Read(pageId int) {
	entry, exist := lru.entries[pageId]
	if !exist {
		entry = &LruEntry{
			pageId: pageId,
		}
		lru.entries[pageId] = entry
	} else {
		if lru.head == entry {
			return // already on head
		}

		// remove the entry from the current position
		entry.pred.succ = entry.succ
		if entry.succ != nil {
			entry.succ.pred = entry.pred
		} else {
			lru.tail = entry.pred
		}
	}

	// add the entry at the top of the list
	entry.pred = nil
	entry.succ = lru.head
	if lru.head != nil {
		lru.head.pred = entry
	} else {
		lru.tail = entry
	}
	lru.head = entry
}

func (lru *LeastRecentlyUsedEvictionPolicy) Written(pageId int) {
	// this policy does not distinguish between reads and writes
	lru.Read(pageId)
}

func (lru *LeastRecentlyUsedEvictionPolicy) Removed(pageId int) {
	entry, exist := lru.entries[pageId]
	if exist {
		if entry.pred != nil {
			entry.pred.succ = entry.succ
		} else {
			lru.head = entry.succ
		}

		if entry.succ != nil {
			entry.succ.pred = entry.pred
		} else {
			lru.tail = entry.pred
		}

		delete(lru.entries, pageId)
	}
}

func (lru *LeastRecentlyUsedEvictionPolicy) GetPageToEvict() int {
	if lru.tail != nil {
		return lru.tail.pageId
	}
	return -1
}
