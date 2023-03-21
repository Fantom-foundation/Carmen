package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestPageStorageSingleFileImplements(t *testing.T) {
	var inst FilePageStorage
	var _ PageStorage[int] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageSingleFileStoreLoad(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if count, err := s.GetPagesCount(); err != nil || count != 0 {
		t.Errorf("Unexpected pages count for empty file: %d, %s", count, err)
	}

	loadPageA := NewRawPage(common.PageSize)
	if err := s.Load(5, loadPageA); loadPageA.Size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(5, initPageA())

	loadPageA = NewRawPage(common.PageSize)
	if err := s.Load(5, loadPageA); loadPageA.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, loadPageA, []byte{0xAA})

	if count, err := s.GetPagesCount(); err != nil || count != 6 {
		t.Errorf("Unexpected pages count for file with last item id 5: %d, %s", count, err)
	}

	if lastId := s.NextId(); lastId != 6 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}
}

func TestPageStorageSingleFilesDataPersisted(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	loadPageA := NewRawPage(common.PageSize)
	if err := s.Load(5, loadPageA); loadPageA.Size() != 0 || err != nil {
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

	s, err = NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA = NewRawPage(common.PageSize)
	if err := s.Load(5, loadPageA); loadPageA.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, loadPageA, []byte{0xAA})

	// removed pages should not exist
	loadPageA = NewRawPage(common.PageSize)
	if err := s.Load(3, loadPageA); loadPageA.Size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
	loadPageA = NewRawPage(common.PageSize)
	if err := s.Load(1, loadPageA); loadPageA.Size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}

func TestPageStorageSingleFileRemovePage(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	err = s.Store(5, initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if lastId := s.NextId(); lastId != 6 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}

	// remove the page
	err = s.Remove(5)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	// last IDs have not changed
	if lastId := s.NextId(); lastId != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 5)
	}

	// removed IDs
	if removed, exists := s.freeIdsMap[5]; !exists || !removed {
		t.Errorf("Id not in remoevd list")
	}

	// try to load removed pages
	loadPageA := NewRawPage(common.PageSize)
	if err := s.Load(5, loadPageA); loadPageA.Size() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}

func TestPageDirtyFlag(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	page := initPageA()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("new page should be dirty")
	}

	_ = s.Store(1, page)

	if dirty := page.IsDirty(); dirty {
		t.Errorf("persisted page should not be dirty")
	}

	page.Clear()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("cleared page should be dirty")
	}

	_ = s.Load(1, page)

	if dirty := page.IsDirty(); dirty {
		t.Errorf("freshly loaded page should not be dirty")
	}

	dump := make([]byte, page.Size())
	page.ToBytes(dump)

	restoredPage := NewRawPage(common.PageSize)
	restoredPage.FromBytes(dump)
	if dirty := restoredPage.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}
}

func initPageA() Page {
	page := NewRawPage(common.PageSize)
	page.FromBytes([]byte{0xAA})
	return page
}

func testPageContent(t *testing.T, a Page, expected []byte) {
	actual := make([]byte, a.Size())
	a.ToBytes(actual)
	// check only first byte, not the whole 4kB!
	if actual[0] != expected[0] {
		t.Errorf("content does not match: %v != %v", expected, actual)
	}
}
