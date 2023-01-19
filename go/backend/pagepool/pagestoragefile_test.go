package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

const (
	pageSize = 1 << 12
)

func TestPageStorageSingleFileImplements(t *testing.T) {
	var inst FilesPageStorage
	var _ PageStorage[int] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageSingleFileStoreLoad(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(5, loadPageA); loadPageA.size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(5, initPageA())

	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(5, loadPageA); loadPageA.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	if hasNext := loadPageA.hasNext; hasNext {
		t.Errorf("Has next is wrong")
	}
	if next := loadPageA.next; next != 0 {
		t.Errorf("Wront link to next: %v != %v", next, 0)
	}

	if lastId := s.lastID; lastId != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}
}

func TestPageStorageSingleFilesDataPersisted(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(5, loadPageA); loadPageA.size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(1, initPageA())
	_ = s.Store(2, initPageA())
	_ = s.Store(3, initPageA())
	_ = s.Store(4, initPageA())
	_ = s.Store(5, initPageA())

	// remove a page
	_ = s.Remove(1)
	_ = s.Remove(3)

	// reopen
	err = s.Close()
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	s, err = NewFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(5, loadPageA); loadPageA.size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	// removed pages should not exist
	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(3, loadPageA); loadPageA.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
	loadPageA = NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(1, loadPageA); loadPageA.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}

func TestPageStorageSingleFileRemovePage(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilesPageStorage(tempDir, pageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = s.Store(5, initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if lastId := s.lastID; lastId != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}

	// remove the page
	err = s.Remove(5)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	// last IDs have not changed
	if lastId := s.lastID; lastId != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}

	// removed IDs
	if removed, exists := s.removedIDs[5]; !exists || !removed {
		t.Errorf("Id not in remoevd list")
	}

	// try to load removed pages
	loadPageA := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(5, loadPageA); loadPageA.size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}
