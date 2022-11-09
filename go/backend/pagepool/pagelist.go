package pagepool

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// PageList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
type PageList[K comparable, V any] struct {
	pagePool     *PagePool[K, V]
	bucket       int   // bucket this list belongs to, it is used to identify correct first page
	pageList     []int // IDs of pages in this list
	maxPageItems int

	size int // current size computed during addition for fast read
}

// NewPageList creates a new instance, each block will have the given maximal capacity.
func NewPageList[K comparable, V any](bucket, pageItems int, pagePool *PagePool[K, V]) *PageList[K, V] {
	return &PageList[K, V]{
		maxPageItems: pageItems,
		pagePool:     pagePool,
		bucket:       bucket,
		pageList:     make([]int, 1, 100),
	}
}

// ForEach all entries - calls the callback for each key-value pair in the list
func (m *PageList[K, V]) ForEach(callback func(K, V)) error {
	page, err := m.pagePool.Get(PageId{m.bucket, 0}) // fist page from this bucket
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
	page, err := m.pagePool.Get(PageId{m.bucket, 0}) // fist page from this bucket
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

// Put associates a key to the list.
// If the key is already present, the value is updated.
func (m *PageList[K, V]) Put(key K, val V) error {
	page, err := m.pagePool.Get(PageId{m.bucket, 0}) // fist page from this bucket
	if err != nil {
		return err
	}

	// try to replace value if it exists
	var last *Page[K, V]
	for page != nil {
		// replace value if it already exists.
		if _, update := page.Get(key); update {
			page.Put(key, val)

			return nil
		}

		last = page
		page, _, err = m.next(page)
		if err != nil {
			return err
		}
	}

	// add a new block when overflow
	if last.Size() == m.maxPageItems {
		newId := m.createNextPage(last)
		last, err = m.pagePool.Get(newId)
		if err != nil {
			return err
		}
	}

	last.Put(key, val) // associate a new value
	m.size += 1

	return nil
}

// BulkInsert creates content of this list from the input data.
func (m *PageList[K, V]) BulkInsert(data []common.MapEntry[K, V]) error {
	pageId := PageId{m.bucket, m.pageList[len(m.pageList)-1]}

	var start int
	// fill-in possible half empty last element
	if m.Size() > 0 {
		tail, err := m.pagePool.Get(pageId)
		if err != nil {
			return err
		}

		start = m.maxPageItems - tail.Size()
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

// GetAll collects data from all blocks and returns them as one slice
func (m *PageList[K, V]) GetAll() ([]common.MapEntry[K, V], error) {
	page, err := m.pagePool.Get(PageId{m.bucket, 0}) // fist page from this bucket
	if err != nil {
		return nil, err
	}

	data := make([]common.MapEntry[K, V], 0, m.size)
	for page != nil {
		data = append(data, page.GetAll()...)
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
	itemPageId := PageId{m.bucket, 0}
	item, err := m.pagePool.Get(itemPageId) // fist page from this bucket
	if err != nil {
		return
	}

	for item != nil {
		// remove value if it already exists.
		if exists = item.Remove(key); exists {
			tailIndex := len(m.pageList) - 1
			tailPageId := PageId{m.bucket, m.pageList[tailIndex]}
			tail, err := m.pagePool.Get(tailPageId) // tail page
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
					tailIndex -= 1
					m.pageList = m.pageList[:tailIndex+1]
					prevPageId := PageId{m.bucket, m.pageList[tailIndex]}
					prevPage, err := m.pagePool.Get(prevPageId)
					if err != nil {
						return false, err
					}
					prevPage.RemoveNext()
				}
			}

			m.size -= 1
			break
		}
		item, itemPageId, err = m.next(item)
		if err != nil {
			return
		}
	}

	return
}

func (m *PageList[K, V]) createNextPage(page *Page[K, V]) (nextId PageId) {
	tailPageId := m.pagePool.GenerateNextId()
	m.pageList = append(m.pageList, tailPageId)
	pageId := PageId{m.bucket, tailPageId}
	page.SetNext(pageId)
	return pageId
}

// fillFromTail puts a random item from the tail of this list and inserts it into the input item
func fillFromTail[K comparable, V any](item, tail *Page[K, V]) {
	if k, v, exists := pickTailEntry(tail); exists {
		item.Put(k, v)
		tail.Remove(k)
	}
}

// pickTailEntry picks a random (first) value from tail
func pickTailEntry[K comparable, V any](tail *Page[K, V]) (key K, val V, exists bool) {
	if tail.Size() > 0 {
		entry := tail.GetAll()[tail.Size()-1]
		key = entry.Key
		val = entry.Val
		exists = true
	}

	return
}

func (m *PageList[K, V]) Size() int {
	return m.size
}

func (m *PageList[K, V]) Clear() error {
	// release pages from the pool
	for _, overflow := range m.pageList {
		_, err := m.pagePool.Remove(PageId{m.bucket, overflow})
		if err != nil {
			return err
		}
	}
	m.size = 0
	m.pageList = m.pageList[0:1]

	return nil
}

func (m *PageList[K, V]) next(current *Page[K, V]) (page *Page[K, V], pageId PageId, err error) {
	if current.hasNext {
		pageId = current.next
		page, err = m.pagePool.Get(pageId)
	}

	return
}

func (m *PageList[K, V]) PrintDump() {
	page, err := m.pagePool.Get(PageId{m.bucket, 0}) // fist page from this bucket
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
