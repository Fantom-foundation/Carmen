package pagedfile

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile/eviction"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[I common.Identifier, V any] struct {
	file           *os.File
	hashTree       hashtree.HashTree
	evictionPolicy eviction.Policy
	pagesPool      map[int]*Page
	serializer     common.Serializer[V]
	pageSize       int64 // the maximum size of a page in bytes
	itemSize       int64 // the amount of bytes per one value
	poolSize       int
	itemsPerPage   int
	itemDefault    V
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], itemDefault V, pageSize int64, branchingFactor int, poolSize int, evictionPolicy eviction.Policy) (*Store[I, V], error) {
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
		file:           f,
		evictionPolicy: evictionPolicy,
		pagesPool:      make(map[int]*Page, poolSize),
		serializer:     serializer,
		pageSize:       pageSize,
		itemSize:       itemSize,
		poolSize:       poolSize,
		itemsPerPage:   int(pageSize / itemSize),
		itemDefault:    itemDefault,
	}
	s.hashTree = file.CreateHashTreeFactory(path+"/hashes", branchingFactor).Create(s)
	return s, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	return int(id / I(m.itemsPerPage)), int64(id%I(m.itemsPerPage)) * m.itemSize
}

// ensurePageLoaded loads the page into the page pool if it is not there already
func (m *Store[I, V]) ensurePageLoaded(pageId int) (*Page, error) {
	page, exists := m.pagesPool[pageId]
	if exists {
		return page, nil
	}
	// evict if the pool is full
	if len(m.pagesPool) >= m.poolSize {
		err := m.evictPage(m.evictionPolicy.GetPageToEvict())
		if err != nil {
			return nil, err
		}
	}
	// load the page into the pool
	page, err := LoadPage(m.file, pageId, m.pageSize)
	if err != nil {
		return nil, err
	}
	m.pagesPool[pageId] = page
	m.evictionPolicy.Read(pageId)
	return page, nil
}

// evictPage removes the page from the pool, stores it if changed
func (m *Store[I, V]) evictPage(pageId int) error {
	page, exists := m.pagesPool[pageId]
	if !exists {
		return fmt.Errorf("page to evict is missing in the pool")
	}
	if page.Dirty {
		err := page.Store(m.file, pageId, m.pageSize)
		if err != nil {
			return err
		}
		m.hashTree.MarkUpdated(pageId)
	}
	delete(m.pagesPool, pageId)
	m.evictionPolicy.Removed(pageId)
	return nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	bytes := m.serializer.ToBytes(value)
	page.Set(itemPosition, bytes)
	m.evictionPolicy.Written(pageId)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (V, error) {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return m.itemDefault, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	m.evictionPolicy.Read(pageId)
	bytes := page.Get(itemPosition, m.itemSize)
	return m.serializer.FromBytes(bytes), nil
}

// GetPage provides the page content for the HashTree
func (m *Store[I, V]) GetPage(pageId int) ([]byte, error) {
	page, err := m.ensurePageLoaded(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	return page.Data, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	// evict the whole pages pool - hash pages into the hashTree
	for pageId, _ := range m.pagesPool {
		err := m.evictPage(pageId)
		if err != nil {
			return common.Hash{}, err
		}
	}
	// get the hash
	return m.hashTree.HashRoot()
}

// Flush all changes to the disk
func (m *Store[I, V]) Flush() (err error) {
	if _, err = m.hashTree.HashRoot(); err != nil {
		return err
	}
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
