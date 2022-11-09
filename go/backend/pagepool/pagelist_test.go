package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
)

const (
	pagePoolSize = 2
	maxItems     = 3
)

func TestPageListIsMap(t *testing.T) {
	var instance PageList[common.Address, uint32]
	var _ common.BulkInsertMap[common.Address, uint32] = &instance
}

func TestPageListPutGet(t *testing.T) {
	p := initPageList()

	if actual, exists, _ := p.Get(A); exists || actual != 0 {
		t.Errorf("PageList should be empty: %d", actual)
	}

	_ = p.Put(A, 10)
	if actual, exists, _ := p.Get(A); !exists || actual != 10 {
		t.Errorf("PageList key should exist: %d", actual)
	}

	_ = p.Put(A, 20)
	if actual, exists, _ := p.Get(A); !exists || actual != 20 {
		t.Errorf("PageList key should be replaced: %d", actual)

	}

	if actual, exists, _ := p.Get(A); !exists || actual != 20 {
		t.Errorf("PageList key should exist: %d", actual)
	}

	_ = p.Put(B, 30)
	if actual, exists, _ := p.Get(B); !exists || actual != 30 {
		t.Errorf("PageList key should exist: %d", actual)
	}

	actualData := make(map[common.Address]uint32, 123)
	_ = p.ForEach(func(k common.Address, v uint32) {
		actualData[k] = v

		if k != A && k != B {
			t.Errorf("Unexpected key: %v", k)
		}
		if k == A && v != 20 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
		if k == B && v != 30 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
	})

	if len(actualData) != p.Size() {
		t.Errorf("Wrong number of items received from for-each")
	}
}

func TestPageListBulk(t *testing.T) {
	p := initPageList()

	if _, exists, _ := p.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(2*maxItems + 2) // three pages
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	_ = p.BulkInsert(data)

	if size := p.Size(); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	if size := len(p.pageList); size != 3 {
		t.Errorf("Number of pages does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	entries, _ := p.GetAll()
	for i, entry := range entries {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(entries); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

}

func TestPageListBulkInsertNonEmptyList(t *testing.T) {
	p := initPageList()

	if _, exists, _ := p.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	// input some initial items
	_ = p.Put(A, 3000)
	_ = p.Put(B, 4000)

	max := uint32(2 * maxItems) // two pages + 2 already existing items will make three pages
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	_ = p.BulkInsert(data)

	expectedData := append(make([]common.MapEntry[common.Address, uint32], 0, 3*max), common.MapEntry[common.Address, uint32]{A, 3000})
	expectedData = append(expectedData, common.MapEntry[common.Address, uint32]{B, 4000})
	expectedData = append(expectedData, data...)

	if size := p.Size(); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, len(expectedData))
	}

	if size := len(p.pageList); size != 3 {
		t.Errorf("Number of pages does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	entries, _ := p.GetAll()
	for i, entry := range entries {
		if entry.Key != expectedData[i].Key || entry.Val != expectedData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, expectedData[i].Key, expectedData[i].Val)
		}
	}

	if size := len(entries); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	// insert one more dataset to the tail with values after already inserted ones
	nextMax := uint32(2 * maxItems) // two more pages will make five pages
	data = make([]common.MapEntry[common.Address, uint32], nextMax)
	for i := uint32(0); i < nextMax; i++ {
		address := common.Address{byte(max + i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, max + i + 1}
	}

	_ = p.BulkInsert(data)

	expectedData = append(expectedData, data...)

	if size := p.Size(); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	if size := len(p.pageList); size != 5 {
		t.Errorf("Number of pages does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	entries, _ = p.GetAll()
	for i, entry := range entries {
		if entry.Key != expectedData[i].Key || entry.Val != expectedData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, expectedData[i].Key, expectedData[i].Val)
		}
	}

	if size := len(entries); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}
}

func TestPageListRemove(t *testing.T) {
	p := initPageList()

	_ = p.Put(A, 10)
	_ = p.Put(B, 20)
	_ = p.Put(C, 30)

	if size := p.Size(); size != 3 {
		t.Errorf("Size is not correct: %d", size)
	}

	if exists, _ := p.Remove(B); !exists {
		t.Errorf("Cannot remove a value")
	}
	if size := p.Size(); size != 2 {
		t.Errorf("Size is not correct: %d", size)
	}

	if exists, _ := p.Remove(A); !exists {
		t.Errorf("Cannot remove a value")
	}
	if size := p.Size(); size != 1 {
		t.Errorf("Size is not correct: %d", size)
	}

	if exists, _ := p.Remove(C); !exists {
		t.Errorf("Cannot remove a value")
	}
	if size := p.Size(); size != 0 {
		t.Errorf("Size is not correct: %d", size)
	}

	_ = p.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageListClear(t *testing.T) {
	p := initPageList()

	_ = p.Put(A, 10)
	_ = p.Put(B, 20)
	_ = p.Put(C, 30)

	if size := p.Size(); size != 3 {
		t.Errorf("Size is not correct: %d", size)
	}

	_ = p.Clear()

	if size := p.Size(); size != 0 {
		t.Errorf("Size is not correct: %d", size)
	}

	_ = p.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageListOverflow(t *testing.T) {
	p := initPageList()
	randomBucket := 33

	// fill-in all pages we have
	for i := uint32(0); i < maxItems; i++ {
		address := common.Address{byte(i + 1)}
		_ = p.Put(address, i+1)
	}

	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(PageId{randomBucket, p.pageList[0]}); page.Size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.Size(), maxItems)
	}

	// add overflow page
	_ = p.Put(B, 199)

	if len(p.pageList) != 2 {
		t.Errorf("PageList should have two pages")
	}
	if page, _ := p.pagePool.Get(PageId{randomBucket, 0}); page.Size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.Size(), maxItems)
	}
	if page, _ := p.pagePool.Get(PageId{randomBucket, p.pageList[1]}); page.Size() != 1 {
		t.Errorf("Wrong page size: %d != %d", page.Size(), 1)
	}

	// remove from the first page
	if exists, _ := p.Remove(common.Address{byte(1)}); !exists {
		t.Errorf("Item not removed")
	}

	// we should have back one full page
	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(PageId{randomBucket, 0}); page.Size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.Size(), maxItems)
	}

	// remove yet one item
	// remove from the first page
	if exists, _ := p.Remove(common.Address{byte(2)}); !exists {
		t.Errorf("Item not removed")
	}

	// we should have back one full page
	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(PageId{randomBucket, 0}); page.Size() != maxItems-1 {
		t.Errorf("Wrong page size: %d != %d", page.Size(), maxItems-1)
	}
}

func initPageList() *PageList[common.Address, uint32] {
	// two pages in the pool, two items each
	pagePool := NewPagePool[common.Address, uint32](pagePoolSize, maxItems, nil, NewMemoryPageStore[common.Address, uint32](), common.AddressComparator{})
	return NewPageList[common.Address, uint32](33, maxItems, pagePool)
}
