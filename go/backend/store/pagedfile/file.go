package pagedfile

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"unsafe"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[I common.Identifier, V any] struct {
	file         *os.File
	hashTree     hashtree.HashTree
	pagesPool    *common.Cache[int, *Page]
	serializer   common.Serializer[V]
	pageSize     int64 // the maximum size of a page in bytes
	itemSize     int64 // the amount of bytes per one value
	itemsPerPage int
	freePage     *Page
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], pageSize int64, hashtreeFactory hashtree.Factory, poolSize int) (*Store[I, V], error) {
	itemSize := int64(serializer.Size())
	if pageSize < itemSize {
		return nil, fmt.Errorf("page size must not be less than one item size")
	}
	// ensure the pageSize is rounded to whole items
	pageSize = pageSize / itemSize * itemSize

	// create directory structure
	err := os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, fmt.Errorf("failed to create hashes directory; %s", err)
	}

	f, err := os.OpenFile(path+"/data", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create data file; %s", err)
	}

	s := &Store[I, V]{
		file:         f,
		serializer:   serializer,
		pageSize:     pageSize,
		itemSize:     itemSize,
		itemsPerPage: int(pageSize / itemSize),
	}
	s.hashTree = hashtreeFactory.Create(s)
	s.pagesPool = common.NewCache[int, *Page](poolSize)
	return s, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	return int(id / I(m.itemsPerPage)), int64(id%I(m.itemsPerPage)) * m.itemSize
}

// ensurePageLoaded loads the page into the page pool if it is not there already
func (m *Store[I, V]) ensurePageLoaded(pageId int) (page *Page, err error) {
	page, exists := m.pagesPool.Get(pageId)
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
	evictedPageId, evictedPage := m.pagesPool.Set(pageId, page)
	if evictedPage != nil {
		err = m.handleEvictedPage(evictedPageId, evictedPage)
	}
	return
}

// handleEvictedPage ensures storing an evicted page back to the disk
func (m *Store[I, V]) handleEvictedPage(pageId int, page *Page) error {
	if page.IsDirty() {
		err := page.Store(m.file, pageId)
		if err != nil {
			return fmt.Errorf("failed to store evicted page; %s", err)
		}
		m.hashTree.MarkUpdated(pageId)
	}
	m.freePage = page
	return nil
}

// getEmptyPage provides an empty page for a page loading
func (m *Store[I, V]) getEmptyPage() *Page {
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
func (m *Store[I, V]) Set(id I, value V) error {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	pageItemBytes := page.Get(itemPosition, int64(m.serializer.Size()))
	m.serializer.CopyBytes(value, pageItemBytes)
	page.SetDirty()
	return nil
}

// Get a value of the item (or a zero value, if not defined)
func (m *Store[I, V]) Get(id I) (value V, err error) {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return value, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	bytes := page.Get(itemPosition, m.itemSize)
	return m.serializer.FromBytes(bytes), nil
}

// GetPage provides the page content for the HashTree
func (m *Store[I, V]) GetPage(pageId int) ([]byte, error) {
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	return page.GetContent(), nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (hash common.Hash, err error) {
	// mark dirty pages as updated in the hashtree
	m.pagesPool.Iterate(func(pageId int, page *Page) bool {
		if page.IsDirty() {
			// write the page to disk (but don't evict - keep in page pool as a clean page)
			err = page.Store(m.file, pageId)
			if err != nil {
				return false
			}
			m.hashTree.MarkUpdated(pageId)
		}
		return true
	})
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to flush pages pool to calculate store hash; %s", err)
	}
	// update the hashTree and get the hash
	return m.hashTree.HashRoot()
}

// Flush all changes to the disk
func (m *Store[I, V]) Flush() (err error) {
	// flush dirty pages and update the hashTree
	if _, err = m.GetStateHash(); err != nil {
		return err
	}
	// flush data file changes to disk
	if err = m.file.Sync(); err != nil {
		return err
	}
	return nil
}

// Close the store
func (m *Store[I, V]) Close() (err error) {
	if err = m.Flush(); err != nil {
		return err
	}
	return m.file.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() uintptr {
	pageSize := unsafe.Sizeof(Page{}) + uintptr(m.pageSize)
	return unsafe.Sizeof(*m) +
		m.hashTree.GetMemoryFootprint() +
		m.pagesPool.GetMemoryFootprint(pageSize)
}
