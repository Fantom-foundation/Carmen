//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package file

import (
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestPageStorageTwoFilesImplements(t *testing.T) {
	var inst TwoFilesPageStorage
	var _ pagepool.PageStorage[PageId] = &inst
	var _ common.FlushAndCloser = &inst
}

func TestPageStorageTwoFilesStoreLoad(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	loadPageA := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	idA := NewPageId(5, 0)
	if err := s.Load(idA, loadPageA); loadPageA.sizeKeys() != 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	_ = s.Store(idA, initPageA())

	loadPageA = NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idA, loadPageA); loadPageA.sizeKeys() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	if hasNext := loadPageA.hasNext; hasNext {
		t.Errorf("Has getNextPage is wrong")
	}
	if next := loadPageA.next; next != 0 {
		t.Errorf("Wront link to getNextPage: %v != %v", next, 0)
	}

	idB := NewPageId(0, 1)
	_ = s.Store(idB, initPageB())

	loadPageB := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idB, loadPageB); loadPageB.sizeKeys() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 4, 1, loadPageB)

	if hasNext := loadPageB.hasNext; !hasNext {
		t.Errorf("Has getNextPage is wrong")
	}
	if next := loadPageB.next; next != 4 {
		t.Errorf("Wront link to getNextPage: %v != %v", next, 4)
	}

	idC := NewPageId(5, 7)
	_ = s.Store(idC, initPageC())

	loadPageC := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idC, loadPageC); loadPageC.sizeKeys() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 5, 2, loadPageC)

	if lastId := s.GenerateNextId(); lastId.Overflow() != 8 {
		t.Errorf("Last ID does not match: %d != %d", lastId, 8)
	}
}

func TestPageStorageTwoFilesRemovePage(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	err = s.Store(NewPageId(2, 0), initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = s.Store(NewPageId(2, 4), initPageA())
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if lastId := s.GenerateNextId(); lastId.Overflow() != 5 {
		t.Errorf("Last ID does not match: %d != %d", lastId.Overflow(), 5)
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

	// Last ID is the last removed one (three minus one)
	if lastId := s.GenerateNextId(); lastId.Overflow() != 3 {
		t.Errorf("Last ID does not match: %d != %d", lastId.Overflow(), 3)
	}

	// try to load removed pages
	loadPageA := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(1, 0), loadPageA); loadPageA.sizeKeys() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
	if err := s.Load(NewPageId(2, 3), loadPageA); loadPageA.sizeKeys() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}
}

func TestPageStorageTwoFilesDataPersisted(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	loadPageA := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	idA := NewPageId(5, 0)
	if err := s.Load(idA, loadPageA); loadPageA.sizeKeys() != 0 || err != nil {
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

	s, err = NewTwoFilesPageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	loadPageA = NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idA, loadPageA); loadPageA.sizeKeys() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageA)

	loadPageB := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(idB, loadPageB); loadPageB.sizeKeys() == 0 || err != nil {
		t.Errorf("Page should exist")
	}

	testPageContent(t, 1, 3, loadPageB)

	// removed pages cannot exist
	page := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(4, 0), page); page.sizeKeys() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	page = NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(5, 1), page); page.sizeKeys() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

	page = NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	if err := s.Load(NewPageId(5, 2), page); page.sizeKeys() > 0 || err != nil {
		t.Errorf("Page should not exist")
	}

}

func testPageContent(t *testing.T, start, expectedSize int, page *IndexPage[common.Address, uint32]) {
	size := page.sizeKeys()
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

func TestPageDirtyFlag(t *testing.T) {
	tempDir := t.TempDir()
	s, err := NewTwoFilesPageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	idA := NewPageId(5, 0)
	page := initPageA()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("new page should be dirty")
	}

	_ = s.Store(idA, page)

	if dirty := page.IsDirty(); dirty {
		t.Errorf("persisted page should not be dirty")
	}

	page.Clear()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("cleared page should be dirty")
	}

	_ = s.Load(idA, page)
	if dirty := page.IsDirty(); dirty {
		t.Errorf("freshly loaded page should not be dirty")
	}

	// test each modification
	page.put(common.Address{10}, 10)
	if dirty := page.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}

	_ = s.Load(idA, page) // make non-dirty
	page.remove(common.Address{1})
	if dirty := page.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}

	_ = s.Load(idA, page) // make non-dirty
	page.removeNext()
	if dirty := page.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}

	_ = s.Load(idA, page) // make non-dirty
	page.setNext(999)
	if dirty := page.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}

	_ = s.Load(idA, page) // make non-dirty
	page.setNumKeys(5)
	if dirty := page.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}

	_ = s.Load(idA, page) // make non-dirty
	dump := make([]byte, common.PageSize)
	page.ToBytes(dump)

	restoredPage := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	restoredPage.FromBytes(dump)
	if dirty := restoredPage.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}
}

func initPageA() *IndexPage[common.Address, uint32] {
	page := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{1}, 1)
	page.put(common.Address{2}, 2)
	page.put(common.Address{3}, 3)

	return page
}

func initPageB() *IndexPage[common.Address, uint32] {
	page := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{4}, 4)

	page.setNext(4)
	return page
}

func initPageC() *IndexPage[common.Address, uint32] {
	page := NewIndexPage[common.Address, uint32](common.PageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	page.put(common.Address{5}, 5)
	page.put(common.Address{6}, 6)

	return page
}
