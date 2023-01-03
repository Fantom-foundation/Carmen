package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	pageA = PageId{0, 0}
	pageB = PageId{1, 0}
	pageC = PageId{2, 0}
	pageD = PageId{3, 0}
)

func TestEmptyPagePool(t *testing.T) {
	pagePool := initPagePool()

	if page, _ := pagePool.Get(pageA); page.size() != 0 {
		t.Errorf("Pool should be empty")
	}
}

func TestPageGetSet(t *testing.T) {
	pagePool := initPagePool()

	// create pages
	newPage1, _ := pagePool.Get(pageA)
	newPage2, _ := pagePool.Get(pageD)

	if actPage, _ := pagePool.Get(pageD); actPage != newPage2 {
		t.Errorf("Wrong page returned")
	}

	if actPage, _ := pagePool.Get(pageA); actPage != newPage1 {
		t.Errorf("Wrong page returned")
	}
}

func TestPageOverflows(t *testing.T) {
	pagePool := initPagePool()

	evictedPage := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	evictedPage.put(common.Address{}, 123) // to track a non-empty page

	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	// 3 pages with 4 items each
	_ = pagePool.put(pageA, evictedPage)
	_ = pagePool.put(pageB, page)
	_ = pagePool.put(pageC, page)

	_ = pagePool.put(pageD, page)

	// Here a page is loaded from the persistent storage.
	// If the page exists there, it verifies the page was evicted from the page pool
	testPage := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := pagePool.pageStore.Load(pageA, testPage); testPage.size() == 0 || err != nil {
		t.Errorf("One page should be evicted")
	}
}

func TestRemovedPageDoesNotExist(t *testing.T) {
	pagePool := initPagePool()

	_, _ = pagePool.Get(pageA) // create the page
	_, _ = pagePool.Remove(pageA)

	if actualPage, _ := pagePool.Get(pageA); actualPage.size() != 0 {
		t.Errorf("KVPage was not deleted: %v", actualPage)
	}
}

func TestPageRemoveOverflow(t *testing.T) {
	evictedPage := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	evictedPage.put(common.Address{}, 123) // to track a non-empty page
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	pagePool := initPagePool()

	_ = pagePool.put(pageA, evictedPage)
	_ = pagePool.put(pageB, page)
	_ = pagePool.put(pageC, page)
	_ = pagePool.put(pageD, page)

	// Here a page is loaded from the persistent storage.
	// If the page exists there, it verifies the page was evicted from the page pool
	testPage := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := pagePool.pageStore.Load(pageA, testPage); testPage.size() == 0 || err != nil {
		t.Errorf("KVPage is not evicted. ")
	}

	_, _ = pagePool.Remove(pageA)

	// removed from the page pool
	if removed, _ := pagePool.Get(pageA); removed.size() != 0 {
		t.Errorf("KVPage is not removed: %v", removed)
	}

	// and from the store
	testPage = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := pagePool.pageStore.Load(pageA, testPage); testPage.size() != 0 || err != nil {
		t.Errorf("KVPage is not removed. ")
	}

	// remove from the pool only
	if exists, _ := pagePool.Remove(pageB); exists {
		t.Errorf("KVPage is not removed")
	}

	// removed from the page pool - repeat
	if removed, _ := pagePool.Get(pageB); removed.size() != 0 {
		t.Errorf("KVPage should not exist: %v", removed)
	}
}

func TestPageClose(t *testing.T) {
	pagePool := initPagePool()

	newPage, _ := pagePool.Get(pageA)
	newPage.put(common.Address{}, 123) // to track a non-empty page

	if actualPage, _ := pagePool.Get(pageA); actualPage.size() == 0 || actualPage != newPage {
		t.Errorf("KVPage was not created, %v != %v", actualPage, newPage)
	}

	_ = pagePool.Close()

	// close must persist the page
	// try to getVal the page from the storage, and it must exist there
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := pagePool.pageStore.Load(pageA, page); err != nil || page.size() == 0 {
		t.Errorf("KVPage is not persisted, %v ", page)
	}
}

func initPagePool() *PagePool[*KVPage[common.Address, uint32]] {
	poolSize := 3
	pageItems := 4

	pageFactory := pageFactory(pageItems)
	return NewPagePool[*KVPage[common.Address, uint32]](poolSize, nil, NewMemoryPageStore(), pageFactory)
}
