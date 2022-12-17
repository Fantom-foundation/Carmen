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
// is evicted when a maxItemsPerPage exceeds and stored using PageStorage. When the pool is asked for
// a page that does not exist in this pool, it tries to load it from the PageStorage.
// The pool also creates and deletes pages when asked, and it generates new IDs for new pages.
// It maintains a list of released ID of the removed pages, which are re-claimed for newly created pages.
// It prevents empty space on the disk, when the pages are stored via PageStorage.
type PagePool[K comparable, V comparable] struct {
	pagePool        *common.Cache[PageId, *Page[K, V]]
	comparator      common.Comparator[K]
	pageStore       PageStorage[K, V] // store where the overflown pages will be stored and where they are read from
	capacity        int
	maxItemsPerPage int

	currentId int           // current ID which is incremented with every new page
	freePages []*Page[K, V] // list of free pages reused between a page eviction and a page load, an instance is re-used no to allocate memory again and again.
	freeIds   []int         // freeIDs are released IDs used for re-allocating disk space for deleted pages
}

// PageStorage is an interface to be implemented to persistent pages.
// A page is sent to the persistent storage by this page pool when it is evicted due to the pool exceeding its capacity.
// In opposite, a page is loaded from the storage when it is requested and not present in the page pool.
type PageStorage[K comparable, V comparable] interface {
	common.MemoryFootprintProvider

	Load(pageId PageId, page *Page[K, V]) error
	Store(pageId PageId, page *Page[K, V]) error
	Remove(pageId PageId) error
}

// NewPagePool creates a new instance. It sets the maxItemsPerPage, i.e. the number of pages hold in-memory by this pool,
// amd the initial capacity of each page. This pool does not check if the capacity of a page actually exceeds,
// it uses the size just to initialise the page.
// freeIds are used for allocating IDs for new pages, when they are all used, this pool starts to allocate new IDs.
func NewPagePool[K comparable, V comparable](capacity, pageItems int, freeIds []int, pageStore PageStorage[K, V], comparator common.Comparator[K]) *PagePool[K, V] {
	var freeIdsCopy []int
	if len(freeIds) == 0 {
		freeIdsCopy = make([]int, 0, releasedIdsCap)
	} else {
		freeIdsCopy = make([]int, len(freeIds))
		copy(freeIdsCopy, freeIds)
	}
	return &PagePool[K, V]{
		pagePool:        common.NewCache[PageId, *Page[K, V]](capacity),
		maxItemsPerPage: pageItems,
		pageStore:       pageStore,
		capacity:        capacity,
		comparator:      comparator,
		freeIds:         freeIdsCopy,
		freePages:       make([]*Page[K, V], 0, capacity),
	}
}

// GenerateNextId generate next unique ID
func (p *PagePool[K, V]) GenerateNextId() (id int) {
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
func (p *PagePool[K, V]) Get(id PageId) (page *Page[K, V], err error) {
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
func (p *PagePool[K, V]) put(pageId PageId, page *Page[K, V]) (err error) {
	evictedId, evictedPage := p.pagePool.Set(pageId, page)
	if evictedPage != nil {
		err = p.storePage(evictedId, evictedPage)
		p.freePages = append(p.freePages, evictedPage)
	}

	return
}

// Remove deletes a page from this pool, which may cause deletion of the page
// from the storage
func (p *PagePool[K, V]) Remove(id PageId) (bool, error) {
	original, exists := p.pagePool.Remove(id)
	if exists {
		p.freePages = append(p.freePages, original)
	}
	// perhaps it is persisted, remove it from the storage
	if err := p.pageStore.Remove(id); err != nil {
		return exists, err
	}

	if id.overflow != 0 {
		p.freeIds = append(p.freeIds, id.overflow)
	}

	return exists, nil
}

func (p *PagePool[K, V]) Flush() (err error) {
	p.pagePool.Iterate(func(k PageId, v *Page[K, V]) bool {
		err = p.storePage(k, v)
		return err == nil
	})

	return nil
}

func (p *PagePool[K, V]) Close() (err error) {
	err = p.Flush()
	if err != nil {
		p.pagePool.Clear()
	}

	return
}

// GetFreeIds returns ID of removed pages, which can be used later to re-allocate space
func (p *PagePool[K, V]) GetFreeIds() []int {
	return p.freeIds
}

// storePage persist the Page to the disk
func (p *PagePool[K, V]) storePage(pageId PageId, page *Page[K, V]) error {
	if page.isDirty {
		if err := p.pageStore.Store(pageId, page); err != nil {
			return err
		}
		page.isDirty = false
	}

	return nil
}

// loadPage loads a page from the disk
func (p *PagePool[K, V]) loadPage(pageId PageId) (page *Page[K, V], err error) {
	page = p.createPage() // it creates a new page instance or re-use from the freelist
	err = p.pageStore.Load(pageId, page)
	return
}

// createPage returns a new page either from the free list or it creates a new Page.
func (p *PagePool[K, V]) createPage() (page *Page[K, V]) {
	if len(p.freePages) > 0 {
		page = p.freePages[len(p.freePages)-1]
		p.freePages = p.freePages[0 : len(p.freePages)-1]
		page.Clear()
	} else {
		page = NewPage[K, V](p.maxItemsPerPage, p.comparator)
	}

	return
}

func (p *PagePool[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*p)
	pageSize := NewPage[K, V](p.maxItemsPerPage, p.comparator).GetMemoryFootprint()
	var x int
	footprint := common.NewMemoryFootprint(selfSize)
	footprint.AddChild("freeList", common.NewMemoryFootprint(uintptr(len(p.freePages))*pageSize.Value()))
	footprint.AddChild("freeIds", common.NewMemoryFootprint(uintptr(len(p.freeIds))*unsafe.Sizeof(x)))
	footprint.AddChild("pagePool", p.pagePool.GetMemoryFootprint(pageSize.Value()))
	footprint.AddChild("pageStore", p.pageStore.GetMemoryFootprint())
	return footprint
}
