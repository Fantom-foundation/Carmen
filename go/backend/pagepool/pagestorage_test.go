package pagepool

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestPageStorageImplements(t *testing.T) {
	var inst FilePageStorage
	var _ PageStorage[PageId] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageGetPut(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewFilePageStorage(tempDir, 1, 0, 0)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA := NewRawPage(common.PageSize)
	idA := NewPageId(0, 0)
	if err := s.Load(idA, loadPageA); loadPageA.Size() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(idA, initPageA())

	loadPageA = NewRawPage(common.PageSize)
	if err := s.Load(idA, loadPageA); loadPageA.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, loadPageA, []byte{0xAA})

	idB := NewPageId(0, 1)
	_ = s.Store(idB, initPageB())

	loadPageB := NewRawPage(common.PageSize)
	if err := s.Load(idB, loadPageB); loadPageB.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, loadPageB, []byte{0xBB})

	idC := NewPageId(1, 0)
	_ = s.Store(idC, initPageC())

	loadPageC := NewRawPage(common.PageSize)
	if err := s.Load(idC, loadPageC); loadPageC.Size() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, loadPageC, []byte{0xCC})
}

func initPageA() Page {
	page := NewRawPage(common.PageSize)
	page.FromBytes([]byte{0xAA})
	return page
}

func initPageB() Page {
	page := NewRawPage(common.PageSize)
	page.FromBytes([]byte{0xBB})
	return page
}

func initPageC() Page {
	page := NewRawPage(common.PageSize)
	page.FromBytes([]byte{0xCC})
	return page
}

func testPageContent(t *testing.T, a Page, expected []byte) {
	actual := make([]byte, a.Size())
	a.ToBytes(actual)
	if bytes.Compare(actual, expected) != 0 {
		t.Errorf("content does not match: %v != %v", expected, actual)
	}
}
