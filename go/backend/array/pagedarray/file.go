package pagedarray

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"unsafe"
)

// Array is a filesystem-based array.Array implementation - it stores mapping of ID to value in binary files.
type Array[I common.Identifier, V any] struct {
	file         *os.File
	pagePool     *common.Cache[int, *Page]
	serializer   common.Serializer[V]
	pageSize     int64 // the maximum size of a page in bytes
	itemSize     int64 // the amount of bytes per one value
	itemsPerPage int
	freePage     *Page
	onDirtyPage  func(pageId int) // callback called on page eviction to notify clients
}

// NewArray constructs a new instance of paged file backed array.
func NewArray[I common.Identifier, V any](path string, serializer common.Serializer[V], pageSize int64, poolSize int) (*Array[I, V], error) {
	itemSize := int64(serializer.Size())
	if pageSize < itemSize {
		return nil, fmt.Errorf("page size must not be less than one item size")
	}
	// create directory structure
	f, err := os.OpenFile(path+"/data", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create data file; %s", err)
	}

	return &Array[I, V]{
		file:         f,
		serializer:   serializer,
		pageSize:     pageSize,
		itemSize:     itemSize,
		itemsPerPage: int(pageSize / itemSize),
		pagePool:     common.NewCache[int, *Page](poolSize),
		onDirtyPage:  func(pageId int) {},
	}, nil
}

// itemPosition provides the position of an item in data pages
func (m *Array[I, V]) itemPosition(id I) (page int, position int64) {
	return int(id / I(m.itemsPerPage)), int64(id%I(m.itemsPerPage)) * m.itemSize
}

// ensurePageLoaded loads the page into the page pool if it is not there already
func (m *Array[I, V]) ensurePageLoaded(pageId int) (page *Page, err error) {
	page, exists := m.pagePool.Get(pageId)
	if exists {
		return
	}
	// get an empty page
	page = m.getEmptyPage()
	// load the page from the disk
	err = page.Load(m.file, pageId)
	if err != nil {
		return
	}
	evictedPageId, evictedPage, evicted := m.pagePool.Set(pageId, page)
	if evicted {
		err = m.handleEvictedPage(evictedPageId, evictedPage)
	}
	return
}

// handleEvictedPage ensures storing an evicted page back to the disk
func (m *Array[I, V]) handleEvictedPage(pageId int, page *Page) error {
	if page.IsDirty() {
		err := page.Store(m.file, pageId)
		if err != nil {
			return fmt.Errorf("failed to store evicted page; %s", err)
		}
		m.onDirtyPage(pageId)
	}
	m.freePage = page
	return nil
}

// getEmptyPage provides an empty page for a page loading
func (m *Array[I, V]) getEmptyPage() *Page {
	if m.freePage != nil {
		return m.freePage // reuse the last evicted page
	} else {
		return &Page{
			data:  make([]byte, m.pageSize),
			dirty: false,
		}
	}
}

// Set a value of an item
func (m *Array[I, V]) Set(id I, value V) error {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	pageItemBytes := page.Get(itemPosition, m.itemSize)
	m.serializer.CopyBytes(value, pageItemBytes)
	page.SetDirty()
	return nil
}

// Get a value of the item (or a zero value, if not defined)
func (m *Array[I, V]) Get(id I) (value V, err error) {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return value, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	bytes := page.Get(itemPosition, m.itemSize)
	return m.serializer.FromBytes(bytes), nil
}

// GetPage provides the page content for the HashTree
func (m *Array[I, V]) GetPage(pageId int) ([]byte, error) {
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}

	return page.GetContent()[0 : m.pageSize/m.itemSize*m.itemSize], nil
}

// Flush all changes to the disk
func (m *Array[I, V]) Flush() (err error) {
	m.pagePool.Iterate(func(pageId int, page *Page) bool {
		if page.IsDirty() {
			// write the page to disk (but don't evict - keep in page pool as a clean page)
			err = page.Store(m.file, pageId)
			if err != nil {
				return false
			}
			m.onDirtyPage(pageId)
		}
		return true
	})

	// flush data file changes to disk
	if err = m.file.Sync(); err != nil {
		return err
	}
	return nil
}

// Close the array
func (m *Array[I, V]) Close() (err error) {
	if err = m.Flush(); err != nil {
		return err
	}
	return m.file.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Array[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	pageSize := unsafe.Sizeof(Page{}) + uintptr(m.pageSize)
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("pagePool", m.pagePool.GetMemoryFootprint(pageSize))
	return mf
}

func (m *Array[I, V]) SetOnDirtyPageCallback(onDirtyPage func(pageId int)) {
	m.onDirtyPage = onDirtyPage
}
