// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// PagePool maintains memory pages and handles their evictions, persistence and loadings.
// It uses an LRU cache to determine if the page has been recently used or not. The least used page
// is evicted when the capacity exceeds, and stored using PageStorage. When the pool is asked for
// a page that does not exist in this pool, it tries to load it from the PageStorage.
type PagePool[ID comparable, T Page] struct {
	pagePool    *common.LruCache[ID, T]
	pageStore   PageStorage[ID] // store where the overflown pages will be stored and where they are read from
	pageFactory func() T

	freePages []T // list of free pages reused between a page eviction and a page load, an instance is re-used no to allocate memory again and again.
}

// PageStorage is an interface to be implemented to persistent pages.
// A page is sent to the persistent storage by this page pool when it is evicted due to the pool exceeding its capacity.
// In opposite, a page is loaded from the storage when it is requested and not present in the page pool.
type PageStorage[ID any] interface {
	common.MemoryFootprintProvider

	// Load retrieves a page for the input ID from the storage and fills-in the input page
	Load(pageId ID, page Page) error

	// Store stores the input Page content under input ID
	Store(pageId ID, page Page) error

	// Remove deletes the page for the input ID from the storage
	Remove(pageId ID) error

	// GenerateNextId returns next free ID
	GenerateNextId() ID
}

// NewPagePool creates a new instance. It sets the capacity, i.e. the number of pages hold in-memory by this pool.
// freeIds are used for allocating IDs for new pages, when they are all used, this pool starts to allocate new IDs.
func NewPagePool[ID comparable, T Page](capacity int, pageStore PageStorage[ID], pageFactory func() T) *PagePool[ID, T] {
	return &PagePool[ID, T]{
		pagePool:    common.NewLruCache[ID, T](capacity),
		pageStore:   pageStore,
		pageFactory: pageFactory,
		freePages:   make([]T, 0, capacity),
	}
}

// GenerateNextId generates next unique ID.
func (p *PagePool[ID, T]) GenerateNextId() (id ID) {
	return p.pageStore.GenerateNextId()
}

// Get returns a Page from the pool, or load it from the storage if the page is not in the pool.
// Another Page may be potentially evicted.
func (p *PagePool[ID, T]) Get(id ID) (page T, err error) {
	page, exists := p.pagePool.Get(id)
	if !exists {
		page, err = p.loadPage(id)
		if err == nil {
			err = p.put(id, page)
		}
	}

	return
}

// put associates a new Page with this pool. Another Page may be potentially evicted,
// when the pool exceeds its capacity.
func (p *PagePool[ID, T]) put(pageId ID, page T) (err error) {
	evictedId, evictedPage, evicted := p.pagePool.Set(pageId, page)
	if evicted {
		err = p.storePage(evictedId, evictedPage)
		p.freePages = append(p.freePages, evictedPage)
	}

	return
}

// Remove deletes a page from this pool, which may cause deletion of the page
// from the storage.
func (p *PagePool[ID, T]) Remove(id ID) (bool, error) {
	original, exists := p.pagePool.Remove(id)
	if exists {
		p.freePages = append(p.freePages, original)
	}
	// perhaps it is persisted, remove it from the storage
	if err := p.pageStore.Remove(id); err != nil {
		return exists, err
	}

	return exists, nil
}

func (p *PagePool[ID, T]) Flush() (err error) {
	p.pagePool.Iterate(func(k ID, v T) bool {
		err = p.storePage(k, v)
		return err == nil
	})

	return nil
}

func (p *PagePool[ID, T]) Close() error {
	err := p.Flush()
	p.pagePool.Clear()
	return err
}

// storePage persist the Page to the disk
func (p *PagePool[ID, T]) storePage(pageId ID, page T) error {
	if page.IsDirty() {
		if err := p.pageStore.Store(pageId, page); err != nil {
			return err
		}
	}

	return nil
}

// loadPage loads a page from the disk.
func (p *PagePool[ID, T]) loadPage(pageId ID) (page T, err error) {
	page = p.createPage() // it creates a new page instance or re-use from the freelist
	err = p.pageStore.Load(pageId, page)
	return
}

// createPage returns a new page either from the free list or it creates a new Page.
func (p *PagePool[ID, T]) createPage() (page T) {
	if len(p.freePages) > 0 {
		page = p.freePages[len(p.freePages)-1]
		p.freePages = p.freePages[0 : len(p.freePages)-1]
		page.Clear()
	} else {
		page = p.pageFactory()
	}

	return
}

func (p *PagePool[ID, T]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*p)
	pageSize := p.pageFactory().GetMemoryFootprint()
	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("freeList", common.NewMemoryFootprint(uintptr(len(p.freePages))*pageSize.Value()))
	footprint.AddChild("pagePool", p.pagePool.GetMemoryFootprint(pageSize.Value()))
	footprint.AddChild("pageStore", p.pageStore.GetMemoryFootprint())
	return footprint
}
