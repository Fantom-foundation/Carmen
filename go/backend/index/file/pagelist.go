package file

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// PageList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
type PageList[K comparable, V comparable] struct {
	pagePool     *pagepool.PagePool[K, V]
	bucket       int   // bucket this list belongs to, it is used to identify correct first page
	pageList     []int // IDs of pages in this list
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
func NewPageList[K comparable, V comparable](bucket, pageItems int, pagePool *pagepool.PagePool[K, V]) *PageList[K, V] {
	return &PageList[K, V]{
		maxPageItems: pageItems,
		pagePool:     pagePool,
		bucket:       bucket,
		pageList:     make([]int, 1, 100),
	}
}

// InitPageList creates a new instance, each block will have the given maximal capacity.
func InitPageList[K comparable, V comparable](bucket, pageItems int, pagePool *pagepool.PagePool[K, V], data []common.MapEntry[K, V]) (*PageList[K, V], error) {
	pageList := NewPageList[K, V](bucket, pageItems, pagePool)
	err := pageList.bulkInsert(data)
	return pageList, err
}

// ForEach all entries - calls the callback for each key-value pair in the list.
func (m *PageList[K, V]) ForEach(callback func(K, V)) error {
	page, err := m.pagePool.Get(pagepool.NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return err
	}
	for page != nil {
		page.ForEach(callback)

		// fetch new page if it exists
		page, _, err = m.next(page)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get returns a value from the list or false.
func (m *PageList[K, V]) Get(key K) (val V, exists bool, err error) {
	page, err := m.pagePool.Get(pagepool.NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return
	}
	for page != nil {
		val, exists = page.Get(key)
		if exists {
			break
		}
		// fetch new page if it exists
		page, _, err = m.next(page)
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
	page, err := m.pagePool.Get(pagepool.NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return val, false, err
	}

	// locate page with existing value
	var last *pagepool.Page[K, V]
	var position int
	var exists bool
	for page != nil {
		// check value exists in the page
		// either just put (replace), or do nothing when adding a value
		switch op {
		case put:
			// PUT operation: if the key exists, associate a new value with the same key
			if position, exists = page.FindItem(key); exists {
				page.UpdateAt(position, val)
				return val, true, nil
			}
		case getOrAdd:
			// getOrAdd operation: when the key exists, its value is just returned
			if position, exists = page.FindItem(key); exists {
				return page.GetAt(position), true, nil
			}
		}

		last = page
		page, _, err = m.next(page)
		if err != nil {
			return val, false, err
		}
	}

	// value not found - add a new block when overflow
	if last.Size() == m.maxPageItems {
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
	last.InsertAt(position, key, val)

	return val, false, nil
}

// bulkInsert creates content of this list from the input data.
func (m *PageList[K, V]) bulkInsert(data []common.MapEntry[K, V]) error {
	pageId := pagepool.NewPageId(m.bucket, m.pageList[len(m.pageList)-1])

	var start int
	// fill-in possible half empty last element
	if m.Size() > 0 {
		tail, err := m.pagePool.Get(pageId)
		if err != nil {
			return err
		}

		start = m.maxPageItems - tail.Size()
		if start > len(data) {
			start = len(data)
		}
		if start > 0 {
			tail.BulkInsert(data[0:start])
			pageId = m.createNextPage(tail) // create new tail
		}
		m.size += start
	}

	var lastPage bool
	for i := start; i < len(data); i += m.maxPageItems {
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
		page.BulkInsert(data[i:end])

		// check there is next page
		if !lastPage {
			pageId = m.createNextPage(page)
		}

		m.size += end - i
	}

	return nil
}

// GetEntries collects data from all blocks and returns them as one slice.
func (m *PageList[K, V]) GetEntries() ([]common.MapEntry[K, V], error) {
	page, err := m.pagePool.Get(pagepool.NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return nil, err
	}

	data := make([]common.MapEntry[K, V], 0, m.size)
	for page != nil {
		data = append(data, page.GetEntries()...)
		// fetch next page if it exists
		page, _, err = m.next(page)
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

func (m *PageList[K, V]) remove(key K) (bool, error) {
	// Iterate pages from tail to the beginning,
	// remove items in the page, and potentially remove the tail if it becomes empty
	var exists bool
	for i := len(m.pageList) - 1; i >= 0; i-- {
		itemPageId := pagepool.NewPageId(m.bucket, m.pageList[i])
		item, err := m.pagePool.Get(itemPageId)
		if err != nil {
			return false, err
		}

		// remove value if it already exists.
		if exists = item.Remove(key); exists {
			tailIndex := len(m.pageList) - 1
			tailPageId := pagepool.NewPageId(m.bucket, m.pageList[tailIndex])
			tail, err := m.pagePool.Get(tailPageId) // tail page
			m.size -= 1

			if err != nil {
				return false, err
			}

			if itemPageId != tailPageId {
				fillFromTail[K, V](item, tail)
			}

			// remove tail if empty
			if tail.Size() == 0 {
				_, err = m.pagePool.Remove(tailPageId) // break association with this pageID in the page pool
				if err != nil {
					return false, err
				}

				if tailIndex > 0 {
					// remove the link from last but the tail
					m.pageList = m.pageList[:len(m.pageList)-1]
					prevPageId := pagepool.NewPageId(m.bucket, m.pageList[len(m.pageList)-1])
					prevPage, err := m.pagePool.Get(prevPageId)
					if err != nil {
						return false, err
					}
					prevPage.RemoveNext()
				}
			}
			break
		}
	}

	return exists, nil
}

func (m *PageList[K, V]) createNextPage(page *pagepool.Page[K, V]) (nextId pagepool.PageId) {
	tailPageId := m.pagePool.GenerateNextId()
	m.pageList = append(m.pageList, tailPageId)
	pageId := pagepool.NewPageId(m.bucket, tailPageId)
	page.SetNext(pageId)
	return pageId
}

// fillFromTail reads a key-value pair from the tail and inserts it into the input page,
// no item is moved when the tail is empty.
func fillFromTail[K comparable, V comparable](page, tail *pagepool.Page[K, V]) {

	if tail.Size() > 0 {
		entries := tail.GetEntries()[tail.Size()-1 : tail.Size()]
		for _, entry := range entries {
			page.Put(entry.Key, entry.Val)
		}
		// remove from tail by moving the size
		tail.SetSize(tail.Size() - 1)
	}
}

func (m *PageList[K, V]) Size() int {
	return m.size
}

func (m *PageList[K, V]) Clear() error {
	// release pages from the pool
	for _, overflow := range m.pageList {
		_, err := m.pagePool.Remove(pagepool.NewPageId(m.bucket, overflow))
		if err != nil {
			return err
		}
	}
	m.size = 0
	m.pageList = m.pageList[0:1]

	return nil
}

func (m *PageList[K, V]) next(current *pagepool.Page[K, V]) (page *pagepool.Page[K, V], pageId pagepool.PageId, err error) {
	if current.HasNext() {
		pageId = current.NextPage()
		page, err = m.pagePool.Get(pageId)
	}

	return
}

func (m *PageList[K, V]) PrintDump() {
	page, err := m.pagePool.Get(pagepool.NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	for page != nil {
		page.ForEach(func(k K, v V) {
			fmt.Printf("  %2v -> %3v \n", k, v)
		})

		// fetch new page if it exists
		page, _, err = m.next(page)
		if err != nil {
			fmt.Printf("Error: %s", err)
		}
	}
}

func (m *PageList[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)
	var x int
	pageListSize := uintptr(len(m.pageList)) * unsafe.Sizeof(x)
	memoryFootprint := common.NewMemoryFootprint(selfSize + pageListSize)
	memoryFootprint.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	return memoryFootprint
}
