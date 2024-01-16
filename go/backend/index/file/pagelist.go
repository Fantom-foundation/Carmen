package file

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// PageList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
type PageList[K comparable, V comparable] struct {
	pagePool     *pagepool.PagePool[PageId, *IndexPage[K, V]]
	bucket       int // bucket this list belongs to, it is used to identify correct first page
	maxPageItems int

	size int // current size computed during addition for fast read
}

// opType represents internal operation type to distinguish between Map and MultiMap.
type opType int

const (
	put opType = iota
	getOrAdd
)

// NewPageList creates a new instance, each block will have the given maximal capacity.
func NewPageList[K comparable, V comparable](bucket, pageItems int, pagePool *pagepool.PagePool[PageId, *IndexPage[K, V]]) PageList[K, V] {
	return PageList[K, V]{
		maxPageItems: pageItems,
		pagePool:     pagePool,
		bucket:       bucket,
	}
}

// InitPageList creates a new instance, each block will have the given maximal capacity.
func InitPageList[K comparable, V comparable](bucket, pageItems int, pagePool *pagepool.PagePool[PageId, *IndexPage[K, V]], data []common.MapEntry[K, V]) error {
	pageList := NewPageList[K, V](bucket, pageItems, pagePool)
	return pageList.bulkInsert(data)
}

// ForEach all entries - calls the callback for each key-value pair in the list.
func (m *PageList[K, V]) ForEach(callback func(K, V)) error {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // first page from this bucket
	if err != nil {
		return err
	}
	for page != nil {
		page.forEach(callback)

		// fetch new page if it exists
		page, _, err = m.getNextPage(page)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get returns a value from the list or false.
func (m *PageList[K, V]) Get(key K) (val V, exists bool, err error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // first page from this bucket
	if err != nil {
		return
	}
	for page != nil {
		val, exists = page.get(key)
		if exists {
			break
		}
		// fetch new page if it exists
		page, _, err = m.getNextPage(page)
		if err != nil {
			return
		}
	}
	return
}

func (m *PageList[K, V]) GetOrAdd(key K, val V) (V, bool, error) {
	return m.addOrPut(key, val, getOrAdd)
}

// Put associates a key to the list.
// If the key is already present, the value is updated.
func (m *PageList[K, V]) Put(key K, val V) error {
	_, _, err := m.addOrPut(key, val, put)
	return err
}

func (m *PageList[K, V]) addOrPut(key K, val V, op opType) (V, bool, error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // first page from this bucket
	if err != nil {
		return val, false, err
	}

	// locate page with existing value
	var last *IndexPage[K, V]
	var position int
	var exists bool
	for page != nil {
		// check value exists in the page
		// either just put (replace), or do nothing when adding a value
		switch op {
		case put:
			// PUT operation: if the key exists, associate a new value with the same key
			if position, exists = page.findItem(key); exists {
				page.updateAt(position, val)
				return val, true, nil
			}
		case getOrAdd:
			// getOrAdd operation: when the key exists, its value is just returned
			if position, exists = page.findItem(key); exists {
				return page.getAt(position), true, nil
			}
		}

		last = page
		page, _, err = m.getNextPage(page)
		if err != nil {
			return val, false, err
		}
	}

	// value not found - add a new block when overflow
	if last.sizeKeys() == m.maxPageItems {
		position = 0
		newId := m.createNextPage(last)
		last, err = m.pagePool.Get(newId)
		if err != nil {
			return val, false, err
		}
	}

	// when we got here, a key will be added
	m.size += 1

	// insert at the last position we have located while iterating pages
	// it is either a position within the last non-full page
	// or the zero position of a newly created page
	last.insertAt(position, key, val)

	return val, false, nil
}

// bulkInsert creates content of this list from the input data.
func (m *PageList[K, V]) bulkInsert(data []common.MapEntry[K, V]) error {
	pageId := NewPageId(m.bucket, 0)
	var lastPage bool
	for i := 0; i < len(data); i += m.maxPageItems {
		page, err := m.pagePool.Get(pageId)
		if err != nil {
			return err
		}

		end := i + m.maxPageItems
		if end > len(data) {
			end = len(data)
			lastPage = true
		}

		// divide input entries into pages
		page.bulkInsert(data[i:end])

		// check there is next page
		if !lastPage {
			pageId = m.createNextPage(page)
		}
	}

	m.size = len(data)
	return nil
}

// GetEntries collects data from all blocks and returns them as one slice.
func (m *PageList[K, V]) GetEntries() ([]common.MapEntry[K, V], error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // first page from this bucket
	if err != nil {
		return nil, err
	}

	data := make([]common.MapEntry[K, V], 0, m.size)
	for page != nil {
		data = append(data, page.getEntries()...)
		// fetch next page if it exists
		page, _, err = m.getNextPage(page)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Remove deletes the key from the map and returns whether an element was removed.
func (m *PageList[K, V]) Remove(key K) (exists bool, err error) {
	return m.remove(key)
}

// remove delete items in the page, and replaces it by an item from the tail,
// and potentially it removes the tail if it becomes empty.
func (m *PageList[K, V]) remove(key K) (bool, error) {
	// it iterates all pages to find the tail, and last but tail ID
	var exists bool
	var removedFromPageId PageId     // ID of a page where an item was removed
	pageId := NewPageId(m.bucket, 0) // ID of current page
	lastId := NewPageId(m.bucket, 0) // ID of last but tail page
	tailId := NewPageId(m.bucket, 0) // ID of tail page

	// iterate all pages finding a key to delete
	// during the iteration, remember the page where a key was deleted
	// find the tail page and the page before the tail
	page, err := m.pagePool.Get(pageId)
	if err != nil {
		return false, err
	}
	for page != nil {
		// try to delete if not already found
		if !exists && page.remove(key) {
			exists = true
			m.size -= 1
			removedFromPageId = pageId
		}

		// move to next page and page ID
		lastId = tailId
		tailId = pageId
		page, pageId, err = m.getNextPage(page)
		if err != nil {
			return false, err
		}
	}

	// deletion found
	if exists {
		// get required pages from the pool again as they could have been evicted by previous iterations
		// but still we expect that the pool here can maintain at least two pages
		// for the page to delete from and for the tail page
		tailPage, err := m.pagePool.Get(tailId)
		if err != nil {
			return false, err
		}

		if tailId != removedFromPageId {
			removedFromPage, err := m.pagePool.Get(removedFromPageId)
			if err != nil {
				return false, err
			}

			fillFromTail[K, V](removedFromPage, tailPage)
		}

		// remove tail if it becomes empty
		if tailPage.sizeKeys() == 0 {
			_, err = m.pagePool.Remove(tailId) // break association with this pageID in the page pool
			if err != nil {
				return false, err
			}
			// break association from the page preceding the tail page
			prevPage, err := m.pagePool.Get(lastId)
			if err != nil {
				return false, err
			}
			prevPage.removeNext()
		}
	}

	return exists, nil
}

func (m *PageList[K, V]) createNextPage(page *IndexPage[K, V]) (nextId PageId) {
	tailPageId := m.pagePool.GenerateNextId().Overflow()
	pageId := NewPageId(m.bucket, tailPageId)
	page.setNext(tailPageId)
	return pageId
}

// fillFromTail reads a key-value pair from the tail and inserts it into the input page,
// no item is moved when the tail is empty.
func fillFromTail[K comparable, V comparable](page, tail *IndexPage[K, V]) {
	if tail.sizeKeys() > 0 {
		tailEntry := tail.getEntries()[tail.sizeKeys()-1]
		page.put(tailEntry.Key, tailEntry.Val)
		// remove from tail by moving the size
		tail.setNumKeys(tail.sizeKeys() - 1)
	}
}

func (m *PageList[K, V]) Size() int {
	return m.size
}

func (m *PageList[K, V]) Clear() error {
	// release pages from the pool
	pageId := NewPageId(m.bucket, 0)
	page, err := m.pagePool.Get(pageId) // first page from this bucket
	if err != nil {
		return err
	}
	for page != nil {
		_, err := m.pagePool.Remove(pageId)
		if err != nil {
			return err
		}
		// fetch new page if it exists
		page, pageId, err = m.getNextPage(page)
		if err != nil {
			return err
		}
	}
	m.size = 0
	return nil
}

// getNextPage gets next page of the input page from the linked list of pages. It returns the next page and its ID.
// If the input page does not have a next page, the returned page is nil.
func (m *PageList[K, V]) getNextPage(current *IndexPage[K, V]) (page *IndexPage[K, V], pageId PageId, err error) {
	if current.hasNext {
		pageId = NewPageId(m.bucket, current.next)
		page, err = m.pagePool.Get(pageId)
	}

	return
}

func (m *PageList[K, V]) PrintDump() {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // first page from this bucket
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	for page != nil {
		page.forEach(func(k K, v V) {
			fmt.Printf("  %2v -> %3v \n", k, v)
		})

		// fetch new page if it exists
		page, _, err = m.getNextPage(page)
		if err != nil {
			fmt.Printf("Error: %s", err)
		}
	}
}

func (m *PageList[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	return memoryFootprint
}
