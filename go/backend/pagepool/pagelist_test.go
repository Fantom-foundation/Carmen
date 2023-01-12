package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
	D = common.Address{0xDD}
)

const (
	pagePoolSize = 2
	maxItems     = 3
)

func TestPageListIsMap(t *testing.T) {
	var instance PageList[common.Address, uint32]
	var _ common.BulkInsertMap[common.Address, uint32] = &instance
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
	entries, _ := p.GetEntries()
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
	entries, _ = p.GetEntries()
	for i, entry := range entries {
		if entry.Key != expectedData[i].Key || entry.Val != expectedData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, expectedData[i].Key, expectedData[i].Val)
		}
	}

	if size := len(entries); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}
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
