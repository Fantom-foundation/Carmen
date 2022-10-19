package eviction

type LRUPolicy struct {
	entries map[int]*lruEntry
	head    *lruEntry
	tail    *lruEntry
}

func NewLRUPolicy(capacity int) Policy {
	return &LRUPolicy{
		entries: make(map[int]*lruEntry, capacity),
		head:    nil,
		tail:    nil,
	}
}

type lruEntry struct {
	pageId int
	succ   *lruEntry
	pred   *lruEntry
}

func (lru *LRUPolicy) Read(pageId int) {
	entry, exist := lru.entries[pageId]
	if !exist {
		entry = &lruEntry{
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

func (lru *LRUPolicy) Written(pageId int) {
	// this policy does not distinguish between reads and writes
	lru.Read(pageId)
}

func (lru *LRUPolicy) Removed(pageId int) {
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

func (lru *LRUPolicy) GetPageToEvict() int {
	if lru.tail != nil {
		return lru.tail.pageId
	}
	return -1
}
