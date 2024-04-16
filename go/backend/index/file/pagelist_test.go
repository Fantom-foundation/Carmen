//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package file

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const (
	maxItems = 3
)

func TestPageListIsMap(t *testing.T) {
	var instance PageList[common.Address, uint32]
	var _ common.ErrMap[common.Address, uint32] = &instance
}

func TestPageListBulkInsert(t *testing.T) {
	p := initPageList()

	if _, exists, _ := p.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(2 * maxItems) // two pages + 2 already existing items will make three pages
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{Key: address, Val: i + 1}
	}

	_ = p.bulkInsert(data)

	if size := p.Size(); size != len(data) {
		t.Errorf("Size does not match: %d != %d", size, len(data))
	}

	// inserted data must much returned data
	entries, _ := p.GetEntries()
	for i, entry := range entries {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(entries); size != len(data) {
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

	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.sizeKeys() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.sizeKeys(), maxItems)
	}

	// add overflow page
	_ = p.Put(B, 199)

	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.sizeKeys() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.sizeKeys(), maxItems)
	}
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); !page.hasNext || page.next == 0 {
		t.Errorf("Wrong has getNextPage link: %d ", page.next)
	}
	// since we have a fresh page pool, next page ID will be one
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 1)); page.sizeKeys() != 1 {
		t.Errorf("Wrong page size: %d != %d", page.sizeKeys(), 1)
	}

	// remove from the first page
	if exists, _ := p.Remove(common.Address{byte(1)}); !exists {
		t.Errorf("Item not removed")
	}

	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.sizeKeys() != maxItems {
		t.Errorf("Wrong page size: %d != %d", page.sizeKeys(), maxItems)
	}
	// link to next page must be removed
	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.hasNext || page.next != 0 {
		t.Errorf("Wrong has getNextPage link: %d ", page.next)
	}

	// remove yet one item
	// remove from the first page
	if exists, _ := p.Remove(common.Address{byte(2)}); !exists {
		t.Errorf("Item not removed")
	}

	if page, _ := p.pagePool.Get(NewPageId(randomBucket, 0)); page.sizeKeys() != maxItems-1 {
		t.Errorf("Wrong page size: %d != %d", page.sizeKeys(), maxItems-1)
	}
}

func initPageList() PageList[common.Address, uint32] {
	sizeBytes := byteSizePage[common.Address, uint32](maxItems, common.AddressSerializer{}, common.Identifier32Serializer{})
	pageFactory := PageFactory[common.Address, uint32](sizeBytes, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
	pagePool := pagepool.NewPagePool[PageId, *IndexPage[common.Address, uint32]](pagePoolSize, pagepool.NewMemoryPageStore[PageId](NextPageIdGenerator()), pageFactory)
	return NewPageList[common.Address, uint32](33, maxItems, pagePool)
}
