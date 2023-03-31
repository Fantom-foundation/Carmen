package pagedarray

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Array is a filesystem-based array.Array implementation - it stores mapping of ID to value in binary files.
type Array[I common.Identifier, V any] struct {
	pagePool     *pagepool.PagePool[int, *Page]
	pageStore    *pagepool.FilePageStorage
	serializer   common.Serializer[V]
	pageSize     int              // the maximum size of a page in bytes
	itemSize     int              // the amount of bytes per one value
	itemsPerPage int              // the amount of values in one page
	pagesCount   int              // the amount of array pages
	onPageDirty  func(pageId int) // callback called on page set dirty
}

// NewArray constructs a new instance of paged file backed array.
func NewArray[I common.Identifier, V any](path string, serializer common.Serializer[V], pageSize, poolSize int) (*Array[I, V], error) {
	itemSize := serializer.Size()
	if pageSize < itemSize {
		return nil, fmt.Errorf("page size must not be less than one item size")
	}

	pageStore, err := pagepool.NewFilePageStorage(path+"/data", pageSize)
	if err != nil {
		return nil, err
	}
	pagePool := pagepool.NewPagePool[int, *Page](poolSize, pageStore, func() *Page {
		return NewPage(pageSize)
	})

	return &Array[I, V]{
		pagePool:     pagePool,
		pageStore:    pageStore,
		serializer:   serializer,
		pageSize:     pageSize,
		itemSize:     itemSize,
		itemsPerPage: pageSize / itemSize,
		pagesCount:   pageStore.GetLastId() + 1,
		onPageDirty:  func(pageId int) {},
	}, nil
}

// itemPosition provides the position of an item in data pages
func (m *Array[I, V]) itemPosition(id I) (page int, position int64) {
	return int(id / I(m.itemsPerPage)), int64(id % I(m.itemsPerPage) * I(m.itemSize))
}

// Set a value of an item
func (m *Array[I, V]) Set(id I, value V) error {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.pagePool.Get(pageId)
	if err != nil {
		return fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	if pageId >= m.pagesCount {
		m.pagesCount = pageId + 1
	}
	m.onPageDirty(pageId)
	page.Set(itemPosition, m.serializer.ToBytes(value))
	return nil
}

// Get a value of the item (or a zero value, if not defined)
func (m *Array[I, V]) Get(id I) (value V, err error) {
	pageId, itemPosition := m.itemPosition(id)
	page, err := m.pagePool.Get(pageId)
	if err != nil {
		return value, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	bytes := page.Get(itemPosition, m.itemSize)
	return m.serializer.FromBytes(bytes), nil
}

// GetPage provides the page content for the HashTree
func (m *Array[I, V]) GetPage(pageId int) ([]byte, error) {
	page, err := m.pagePool.Get(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}

	return page.GetContent()[0 : m.pageSize/m.itemSize*m.itemSize], nil
}

// SetPage allows the callsite to import a page from a snapshot
func (m *Array[I, V]) SetPage(pageId int, data []byte) error {
	page, err := m.pagePool.Get(pageId)
	if err != nil {
		return fmt.Errorf("failed to load store page %d; %s", pageId, err)
	}
	if pageId >= m.pagesCount {
		m.pagesCount = pageId + 1
	}
	page.FromBytes(data)
	return nil
}

func (m *Array[I, V]) GetPagesCount() int {
	return m.pagesCount
}

// Flush all changes to the disk
func (m *Array[I, V]) Flush() (err error) {
	// flush data file changes to disk
	if err = m.pagePool.Flush(); err != nil {
		return err
	}
	if err = m.pageStore.Flush(); err != nil {
		return err
	}
	return nil
}

// Close the array
func (m *Array[I, V]) Close() (err error) {
	if err = m.Flush(); err != nil {
		return err
	}
	if err = m.pagePool.Close(); err != nil {
		return err
	}
	return m.pageStore.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Array[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	// pageStore included in pagePool footprint
	return mf
}

func (m *Array[I, V]) SetOnDirtyPageCallback(onPageDirty func(pageId int)) {
	m.onPageDirty = onPageDirty
}
