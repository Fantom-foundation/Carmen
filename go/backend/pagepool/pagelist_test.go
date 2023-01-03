package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

const (
	pagePoolSize = 2
	maxItems     = 3
)

func TestPageListInsertGetValues(t *testing.T) {
	p := initPageList()

	if _, exists, _ := p.get(A); exists {
		t.Errorf("Value is not correct")
	}

	_, _, _ = p.addOrPut(A, 10, put)
	if val, exists, _ := p.get(A); !exists || val != 10 {
		t.Errorf("Value not found: %d", val)
	}

	// replace: 10 -> 20
	_, _, _ = p.addOrPut(A, 20, put)
	if val, exists, _ := p.get(A); !exists || val != 20 {
		t.Errorf("Value not found: %d", val)
	}

	// add: 20, 30
	_, _, _ = p.addOrPut(A, 30, add)
	vals, _ := p.getAll(A)
	common.AssertEqualArrays(t, vals, []uint32{20, 30})

	// attempt to add existing val just returns
	if val, _, _ := p.addOrPut(A, 40, getOrAdd); val != 30 {
		t.Errorf("Value not found: %d", val)
	}

	if size := p.size(); size != 2 {
		t.Errorf("Wrong size: %d", size)
	}

	entries, _ := p.getEntries()
	if size := p.size(); size != len(entries) {
		t.Errorf("Wrong size: %d", size)
	}

	var i int
	_ = p.forEach(func(k common.Address, v uint32) {
		if v != entries[i].Val {
			t.Errorf("Values do not match: %d != %d", v, entries[i].Val)
		}
		i += 1
	})
}

func TestPageListRemoveValues(t *testing.T) {
	p := initPageList()

	// will add two items
	_, _, _ = p.addOrPut(A, 10, add)
	_, _, _ = p.addOrPut(A, 20, add)
	_, _, _ = p.addOrPut(A, 20, add)
	_, _, _ = p.addOrPut(A, 20, add)

	_, _, _ = p.addOrPut(B, 11, add)
	_, _, _ = p.addOrPut(B, 12, add)

	// one more A
	_, _, _ = p.addOrPut(A, 30, add)

	_, _, _ = p.addOrPut(C, 31, add)

	// remove single key-value pair
	var val uint32 = 10
	_, _ = p.remove(A, &val, removeVal)

	vals, _ := p.getAll(A)
	common.AssertEqualArrays(t, vals, []uint32{20, 30})

	// remove the whole key
	_, _ = p.remove(A, nil, removeAll)
	if vals, _ := p.getAll(A); len(vals) > 0 {
		t.Errorf("Vals should be empty: %v", vals)
	}

	// remove single key
	_, _ = p.remove(C, nil, remove)
	if val, exists, _ := p.get(C); exists {
		t.Errorf("Val should not exist: %d", val)
	}
}

func TestPageListBulkInsertNonEmptyList(t *testing.T) {
	p := initPageList()

	if _, exists, _ := p.get(A); exists {
		t.Errorf("Value is not correct")
	}

	// input some initial items
	_, _, _ = p.addOrPut(A, 3000, put)
	_, _, _ = p.addOrPut(B, 4000, put)

	max := uint32(2 * maxItems) // two pages + 2 already existing items will make three pages
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	_ = p.bulkInsert(data)

	expectedData := append(make([]common.MapEntry[common.Address, uint32], 0, 3*max), common.MapEntry[common.Address, uint32]{A, 3000})
	expectedData = append(expectedData, common.MapEntry[common.Address, uint32]{B, 4000})
	expectedData = append(expectedData, data...)

	if size := p.size(); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, len(expectedData))
	}

	if size := len(p.pageList); size != 3 {
		t.Errorf("Number of pages does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	entries, _ := p.getEntries()
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

	_ = p.bulkInsert(data)

	expectedData = append(expectedData, data...)

	if size := p.size(); size != len(expectedData) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	if size := len(p.pageList); size != 5 {
		t.Errorf("Number of pages does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	entries, _ = p.getEntries()
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
		_, _, _ = p.addOrPut(address, i+1, put)
	}

	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, p.pageList[0])); page.size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.size(), maxItems)
	}

	// add overflow page
	_, _, _ = p.addOrPut(B, 199, put)

	if len(p.pageList) != 2 {
		t.Errorf("PageList should have two pages")
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.size(), maxItems)
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, p.pageList[1])); page.size() != 1 {
		t.Errorf("Wrong page size: %d != %d", page.size(), 1)
	}

	// remove from the first page
	if exists, _ := p.remove(common.Address{byte(1)}, nil, remove); !exists {
		t.Errorf("Item not removed")
	}

	// we should have back one full page
	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.size() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.size(), maxItems)
	}

	// remove yet one item
	// remove from the first page
	if exists, _ := p.remove(common.Address{byte(2)}, nil, remove); !exists {
		t.Errorf("Item not removed")
	}

	// we should have back one full page
	if len(p.pageList) != 1 {
		t.Errorf("PageList should have one page")
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.size() != maxItems-1 {
		t.Errorf("Wrong page size: %d != %d", page.size(), maxItems-1)
	}
}

func initPageList() *PageList[common.Address, uint32] {
	// two pages in the pool, two items each
	pagePool := NewPagePool[common.Address, uint32](pagePoolSize, maxItems, nil, NewMemoryPageStore[common.Address, uint32](), common.AddressComparator{})
	return NewPageList[common.Address, uint32](33, maxItems, pagePool)
}
