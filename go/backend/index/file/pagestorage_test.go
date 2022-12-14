package file

import (
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

const (
	pageSize      = 1 << 12
	metadata      = 2 + 4
	pageSizeItems = (pageSize - metadata) / (20 + 4)
)

func TestPageStorageImplements(t *testing.T) {
	var inst Storage[common.Address, uint32]
	var _ pagepool.PageStorage[common.Address, uint32] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageGetPut(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewStorage[common.Address, uint32](tempDir, pageSize, pageSizeItems, 0, 0, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	idA := pagepool.NewPageId(0, 0)
	if err := s.Load(idA, loadPageA); loadPageA.Size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(idA, initPageA())

	loadPageA = pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	if err := s.Load(idA, loadPageA); loadPageA.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	if hasNext := loadPageA.HasNext(); hasNext {
		t.Errorf("Has next is wrong")
	}
	emptyNext := pagepool.NewPageId(0, 0)
	if next := loadPageA.NextPage(); next != emptyNext {
		t.Errorf("Wront link to next: %v != %v", next, emptyNext)
	}

	idB := pagepool.NewPageId(0, 1)
	_ = s.Store(idB, initPageB())

	loadPageB := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	if err := s.Load(idB, loadPageB); loadPageB.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 4, 1, loadPageB)

	if hasNext := loadPageB.HasNext(); !hasNext {
		t.Errorf("Has next is wrong")
	}
	expectedNext := pagepool.NewPageId(0, 4)
	if next := loadPageB.NextPage(); next != expectedNext {
		t.Errorf("Wront link to next: %v != %v", next, expectedNext)
	}

	idC := pagepool.NewPageId(1, 0)
	_ = s.Store(idC, initPageC())

	loadPageC := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	if err := s.Load(idC, loadPageC); loadPageC.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 5, 2, loadPageC)
}

func testPageContent(t *testing.T, start, expectedSize int, page *pagepool.Page[common.Address, uint32]) {
	size := page.Size()
	if size != expectedSize {
		t.Errorf("Page size does not match: %d != %d", size, expectedSize)
	}

	for i := start; i < start+size; i++ {
		key := common.Address{byte(i)}
		if item, exists := page.Get(key); !exists || item != uint32(i) {
			t.Errorf("Missing value: key %v -> %d != %d ", key, item, uint32(i))
		}
	}
}

func initPageA() *pagepool.Page[common.Address, uint32] {
	page := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	page.Put(common.Address{1}, 1)
	page.Put(common.Address{2}, 2)
	page.Put(common.Address{3}, 3)

	return page
}

func initPageB() *pagepool.Page[common.Address, uint32] {
	page := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	page.Put(common.Address{4}, 4)

	page.SetNext(pagepool.NewPageId(3, 4))
	return page
}

func initPageC() *pagepool.Page[common.Address, uint32] {
	page := pagepool.NewPage[common.Address, uint32](pageSizeItems, common.AddressComparator{})
	page.Put(common.Address{5}, 5)
	page.Put(common.Address{6}, 6)

	return page
}
