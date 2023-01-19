package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestPageStorageTwoFilesImplements(t *testing.T) {
	var inst TwoFilesPageStorage
	var _ PageStorage[PageId] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageTwoFilesStoreLoad(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	idA := NewPageId(5, 0)
	if err := s.Load(idA, loadPageA); loadPageA.size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(idA, initPageA())

	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idA, loadPageA); loadPageA.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	if hasNext := loadPageA.hasNext; hasNext {
		t.Errorf("Has next is wrong")
	}
	if next := loadPageA.next; next != 0 {
		t.Errorf("Wront link to next: %v != %v", next, 0)
	}

	idB := NewPageId(0, 1)
	_ = s.Store(idB, initPageB())

	loadPageB := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idB, loadPageB); loadPageB.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 4, 1, loadPageB)

	if hasNext := loadPageB.hasNext; !hasNext {
		t.Errorf("Has next is wrong")
	}
	if next := loadPageB.next; next != 4 {
		t.Errorf("Wront link to next: %v != %v", next, 4)
	}

	idC := NewPageId(5, 7)
	_ = s.Store(idC, initPageC())

	loadPageC := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idC, loadPageC); loadPageC.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	if lastId := s.primaryFile.lastID; lastId != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}

	if lastId := s.overflowFile.lastID; lastId != 6 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 6)
	}

	testPageContent(t, 5, 2, loadPageC)
}

func TestPageStorageTwoFilesRemovePage(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = s.Store(NewPageId(2, 0), initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = s.Store(NewPageId(2, 4), initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if lastId := s.primaryFile.lastID; lastId != 2 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 2)
	}

	if lastId := s.overflowFile.lastID; lastId != 3 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 3)
	}

	// remove both pages
	err = s.Remove(NewPageId(2, 0))
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = s.Remove(NewPageId(2, 3))
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	// last IDs have not changed
	if lastId := s.primaryFile.lastID; lastId != 2 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 1)
	}

	if lastId := s.overflowFile.lastID; lastId != 3 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 1)
	}

	// removed IDs
	if removed, exists := s.primaryFile.removedIDs[2]; !exists || !removed {
		t.Errorf("Id not in remoevd list")
	}

	if removed, exists := s.overflowFile.removedIDs[2]; !exists || !removed {
		t.Errorf("Id not in remoevd list")
	}

	// try to load removed pages
	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(1, 0), loadPageA); loadPageA.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
	if err := s.Load(NewPageId(2, 3), loadPageA); loadPageA.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}

func TestPageStorageTwoFilesDataPersisted(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	idA := NewPageId(5, 0)
	if err := s.Load(idA, loadPageA); loadPageA.size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(idA, initPageA())

	idB := NewPageId(5, 3)
	_ = s.Store(idB, initPageA())

	// create and remove a few items
	_ = s.Store(NewPageId(4, 0), initPageA())
	_ = s.Store(NewPageId(5, 0), initPageA())
	_ = s.Store(NewPageId(5, 1), initPageA())
	_ = s.Store(NewPageId(5, 2), initPageA())

	_ = s.Remove(NewPageId(4, 0))
	_ = s.Remove(NewPageId(5, 1))
	_ = s.Remove(NewPageId(5, 2))

	// reopen
	err = s.Close()
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	s, err = NewTwoFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idA, loadPageA); loadPageA.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	loadPageB := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idB, loadPageB); loadPageB.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageB)

	// removed pages cannot exist
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(4, 0), page); page.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	page = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(5, 1), page); page.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	page = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(5, 2), page); page.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

}

func testPageContent(t *testing.T, start, expectedSize int, page *KVPage[common.Address, uint32]) {
	size := page.size()
	if size != expectedSize {
		t.Errorf("Page size does not match: %d != %d", size, expectedSize)
	}

	for i := start; i < start+size; i++ {
		key := common.Address{byte(i)}
		if item, exists := page.get(key); !exists || item != uint32(i) {
			t.Errorf("Missing value: key %v -> %d != %d ", key, item, uint32(i))
		}
	}
}

func initPageA() *KVPage[common.Address, uint32] {
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{1}, 1)
	page.put(common.Address{2}, 2)
	page.put(common.Address{3}, 3)

	return page
}

func initPageB() *KVPage[common.Address, uint32] {
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{4}, 4)

	page.setNext(4)
	return page
}

func initPageC() *KVPage[common.Address, uint32] {
	page := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{5}, 5)
	page.put(common.Address{6}, 6)

	return page
}
