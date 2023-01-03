package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

const (
	releasedIdsCap = 1000
)

// PagePool maintains memory pages and handles their evictions, persistence and loadings.
// It uses an LRU cache to determine if the page has been recently used or not. The least used page
// is evicted when the capacity exceeds, and stored using PageStorage. When the pool is asked for
// a page that does not exist in this pool, it tries to load it from the PageStorage.
// The pool also creates and deletes pages when asked, and it generates new IDs for new pages.
// It maintains a list of released ID of the removed pages, which are re-claimed for newly created pages.
// It prevents empty space on the disk, when the pages are stored via PageStorage.
type PagePool[T Page] struct {
	pagePool    *common.Cache[PageId, T]
	pageStore   PageStorage[PageId] // store where the overflown pages will be stored and where they are read from
	pageFactory func() T

	currentId int   // current ID which is incremented with every new page
	freePages []T   // list of free pages reused between a page eviction and a page load, an instance is re-used no to allocate memory again and again.
	freeIds   []int // freeIDs are released IDs used for re-allocating disk space for deleted pages
}

// PageStorage is an interface to be implemented to persistent pages.
// A page is sent to the persistent storage by this page pool when it is evicted due to the pool exceeding its capacity.
// In opposite, a page is loaded from the storage when it is requested and not present in the page pool.
type PageStorage[ID any] interface {
	common.MemoryFootprintProvider

	Load(pageId ID, page Page) error
	Store(pageId ID, page Page) error
	Remove(pageId ID) error
}

// NewPagePool creates a new instance. It sets the capacity, i.e. the number of pages hold in-memory by this pool.
// freeIds are used for allocating IDs for new pages, when they are all used, this pool starts to allocate new IDs.
func NewPagePool[T Page](capacity int, freeIds []int, pageStore PageStorage[PageId], pageFactory func() T) *PagePool[T] {
	var freeIdsCopy []int
	if len(freeIds) == 0 {
		freeIdsCopy = make([]int, 0, releasedIdsCap)
	} else {
		freeIdsCopy = make([]int, len(freeIds))
		copy(freeIdsCopy, freeIds)
	}
	return &PagePool[T]{
		pagePool:    common.NewCache[PageId, T](capacity),
		pageStore:   pageStore,
		pageFactory: pageFactory,
		freeIds:     freeIdsCopy,
		freePages:   make([]T, 0, capacity),
	}
}

// GenerateNextId generate next unique ID
func (p *PagePool[P]) GenerateNextId() (id int) {
	if len(p.freeIds) > 0 {
		id = p.freeIds[len(p.freeIds)-1]
		p.freeIds = p.freeIds[0 : len(p.freeIds)-1]
	} else {
		p.currentId += 1
		id = p.currentId
	}

	return
}

// Get returns a Page from the pool, or load it from the storage if the page is not in the pool.
// Another Page may be potentially evicted.
func (p *PagePool[T]) Get(id PageId) (page T, err error) {
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
func (p *PagePool[T]) put(pageId PageId, page T) (err error) {
	evictedId, evictedPage, evicted := p.pagePool.Set(pageId, page)
	if evicted {
		err = p.storePage(evictedId, evictedPage)
		p.freePages = append(p.freePages, evictedPage)
	}

	return
}

// Remove deletes a page from this pool, which may cause deletion of the page
// from the storage
func (p *PagePool[T]) Remove(id PageId) (bool, error) {
	original, exists := p.pagePool.Remove(id)
	if exists {
		p.freePages = append(p.freePages, original)
	}
	// perhaps it is persisted, remove it from the storage
	if err := p.pageStore.Remove(id); err != nil {
		return exists, err
	}

	if id.IsOverFlowPage() {
		p.freeIds = append(p.freeIds, id.overflow)
	}

	return exists, nil
}

func (p *PagePool[T]) Flush() (err error) {
	p.pagePool.Iterate(func(k PageId, v T) bool {
		err = p.storePage(k, v)
		return err == nil
	})

	return nil
}

func (p *PagePool[T]) Close() (err error) {
	err = p.Flush()
	if err != nil {
		p.pagePool.Clear()
	}

	return
}

// GetFreeIds returns ID of removed pages, which can be used later to re-allocate space
func (p *PagePool[T]) GetFreeIds() []int {
	return p.freeIds
}

// storePage persist the Page to the disk
func (p *PagePool[T]) storePage(pageId PageId, page T) error {
	if page.IsDirty() {
		if err := p.pageStore.Store(pageId, page); err != nil {
			return err
		}
		page.SetDirty(false)
	}

	return nil
}

// loadPage loads a page from the disk
func (p *PagePool[T]) loadPage(pageId PageId) (page T, err error) {
	page = p.createPage() // it creates a new page instance or re-use from the freelist
	err = p.pageStore.Load(pageId, page)
	return
}

// createPage returns a new page either from the free list or it creates a new Page.
func (p *PagePool[T]) createPage() (page T) {
	if len(p.freePages) > 0 {
		page = p.freePages[len(p.freePages)-1]
		p.freePages = p.freePages[0 : len(p.freePages)-1]
		page.Clear()
	} else {
		page = p.pageFactory()
	}

	return
}

func (p *PagePool[T]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*p)
	pageSize := p.pageFactory().GetMemoryFootprint()
	var x int
	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("freeList", common.NewMemoryFootprint(uintptr(len(p.freePages))*pageSize.Value()))
	footprint.AddChild("freeIds", common.NewMemoryFootprint(uintptr(len(p.freeIds))*unsafe.Sizeof(x)))
	footprint.AddChild("pagePool", p.pagePool.GetMemoryFootprint(pageSize.Value()))
	footprint.AddChild("pageStore", p.pageStore.GetMemoryFootprint())
	return footprint
}
