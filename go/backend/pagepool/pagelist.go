package pagepool

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// PageList is a data structure that maintains a list of pages. Each page maps a fixed number of Key/Value pairs.
// When the size overflows, a new page is created and linked in this list. Further keys are stored in the new page.
// The list receives pages from a page pool.
type PageList[K comparable, V comparable] struct {
	pagePool     *PagePool[K, V]
	bucket       int   // bucket this list belongs to, it is used to identify correct first page
	pageList     []int // IDs of pages in this list
	maxPageItems int

	last int // current size computed during addition for fast read
}

// opType represents internal operation type to distinguish between Map and MultiMap
type opType int

const (
	put       opType = iota
	add              // add operation allows for adding another value under the same key
	getOrAdd         // getOrAdd operation either returns a value under given key if the key exists, or it associates a new value with the key
	remove           // remove operation removes single key-value pair of the given key
	removeVal        // removeVal operation removes single key-value pair for exactly matching key and value
	removeAll        // removeAll operation removes all values for the given input key
	unknown
)

// NewPageList creates a new instance, each block will have the given maximal capacity.
func NewPageList[K comparable, V comparable](bucket, pageItems int, pagePool *PagePool[K, V]) *PageList[K, V] {
	return &PageList[K, V]{
		maxPageItems: pageItems,
		pagePool:     pagePool,
		bucket:       bucket,
		pageList:     make([]int, 1, 100),
	}
}

// forEach all entries - calls the callback for each key-value pair in the list
func (m *PageList[K, V]) forEach(callback func(K, V)) error {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return err
	}
	for page != nil {
		page.forEach(callback)

		// fetch new page if it exists
		page, _, err = m.next(page)
		if err != nil {
			return err
		}
	}

	return nil
}

// get returns a value from the list or false.
func (m *PageList[K, V]) get(key K) (val V, exists bool, err error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return
	}
	for page != nil {
		val, exists = page.get(key)
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

func (m *PageList[K, V]) addOrPut(key K, val V, op opType) (V, bool, error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return val, false, err
	}

	// locate page with existing value
	var last *Page[K, V]
	var position int
	var exists bool
	for page != nil {
		// check value exists in the page
		// either just put (replace), or do nothing when adding a value
		switch op {
		case put:
			// PUT operation: if the key exists, associate a new value with the same key
			if position, exists = page.findItem(key); exists {
				page.update(position, val)
				return val, true, nil
			}
		case add:
			// ADD operation: if the key and value exist, return and do nothing
			// if only key exists, but the value does not match, it is ignored and the value
			// will be added at the end of this method
			if position, exists = page.findValue(key, val); exists {
				return val, true, nil
			}
		case getOrAdd:
			// getOrAdd operation: when the key exists, its value is just returned
			if position, exists = page.findItem(key); exists {
				return page.getVal(position), true, nil
			}
		}

		last = page
		page, _, err = m.next(page)
		if err != nil {
			return val, false, err
		}
	}

	// value not found - add a new block when overflow
	if last.size() == m.maxPageItems {
		position = 0
		newId := m.createNextPage(last)
		last, err = m.pagePool.Get(newId)
		if err != nil {
			return val, false, err
		}
	}

	// when we got here, a key will be added
	m.last += 1

	// insert at the last position we have located while iterating pages
	// it is either a position within the last non-full page
	// or the zero position of a newly created page
	last.insert(position, key, val)

	return val, false, nil
}

// bulkInsert creates content of this list from the input data.
func (m *PageList[K, V]) bulkInsert(data []common.MapEntry[K, V]) error {
	pageId := NewPageId(m.bucket, m.pageList[len(m.pageList)-1])

	var start int
	// fill-in possible half empty last element
	if m.last > 0 {
		tail, err := m.pagePool.Get(pageId)
		if err != nil {
			return err
		}

		start = m.maxPageItems - tail.size()
		if start > len(data) {
			start = len(data)
		}
		if start > 0 {
			tail.bulkInsert(data[0:start])
			pageId = m.createNextPage(tail) // create new tail
		}
		m.last += start
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
		page.bulkInsert(data[i:end])

		// check there is next page
		if !lastPage {
			pageId = m.createNextPage(page)
		}

		m.last += end - i
	}

	return nil
}

// getEntries collects data from all blocks and returns them as one slice
func (m *PageList[K, V]) getEntries() ([]common.MapEntry[K, V], error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return nil, err
	}

	data := make([]common.MapEntry[K, V], 0, m.last)
	for page != nil {
		data = append(data, page.getEntries()...)
		// fetch next page if it exists
		page, _, err = m.next(page)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (m *PageList[K, V]) getAll(key K) ([]V, error) {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		return nil, err
	}

	data := make([]V, 0, m.last)
	for page != nil {
		data = page.appendAll(key, data)
		// fetch next page if it exists
		page, _, err = m.next(page)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (m *PageList[K, V]) remove(key K, val *V, op opType) (bool, error) {

	// Iterate pages from tail to the beginning,
	// remove items in the page, and potentially remove the tail if it becomes empty
	var exists bool
	for i := len(m.pageList) - 1; i >= 0; i-- {
		itemPageId := NewPageId(m.bucket, m.pageList[i])
		item, err := m.pagePool.Get(itemPageId)
		if err != nil {
			return false, err
		}

		var numRemove int
		// remove either one key or all values under the same key
		// track the number of removed key-value pairs
		switch op {
		case remove:
			if removed := item.remove(key); removed {
				numRemove = 1
			}
		case removeVal:
			if removed := item.removeVal(key, *val); removed {
				numRemove = 1
			}
		case removeAll:
			if start, end, removed := item.removeAll(key); removed {
				numRemove = end - start
			}
		}

		m.last -= numRemove

		// fill removed data by keys from the tail
		// potentially remove the tail when it becomes empty
		for numRemove > 0 {
			exists = true
			tailIndex := len(m.pageList) - 1
			tailPageId := NewPageId(m.bucket, m.pageList[tailIndex])
			tail, err := m.pagePool.Get(tailPageId) // tail page
			if err != nil {
				return false, err
			}

			if itemPageId == tailPageId {
				numRemove = 0
			} else {
				numRemove -= fillFromTail[K, V](item, tail, numRemove)
			}

			// remove tail if empty
			if tail.size() == 0 {
				_, err = m.pagePool.Remove(tailPageId) // break association with this pageID in the page pool
				if err != nil {
					return false, err
				}

				if tailIndex > 0 {
					// remove the link from last but the tail
					m.pageList = m.pageList[:len(m.pageList)-1]
					prevPageId := NewPageId(m.bucket, m.pageList[len(m.pageList)-1])
					prevPage, err := m.pagePool.Get(prevPageId)
					if err != nil {
						return false, err
					}
					prevPage.removeNext()
				}
			}

			// for single remove, we are done,
			// for remove all we have to unfortunately iterate all pages
			if op == remove || op == removeVal {
				break
			}
		}

	}

	return exists, nil
}

func (m *PageList[K, V]) createNextPage(page *Page[K, V]) (nextId PageId) {
	tailPageId := m.pagePool.GenerateNextId()
	m.pageList = append(m.pageList, tailPageId)
	pageId := NewPageId(m.bucket, tailPageId)
	page.setNext(pageId)
	return pageId
}

// fillFromTail reads the requested number of key-value pairs from the tail and inserts them into the input page
// it returns the number of actually inserted items, which can be lower than the requested number
// if the tail has no more items
func fillFromTail[K comparable, V comparable](page, tail *Page[K, V], number int) int {
	n := number
	if number > tail.size() {
		n = tail.size()
	}

	// replace N entries
	if n > 0 {
		entries := tail.getEntries()[tail.size()-n : tail.size()]
		for _, entry := range entries {
			page.add(entry.Key, entry.Val)
		}
		// remove from tail by moving the size()
		tail.setSize(tail.size() - n)
	}

	return n
}

func (m *PageList[K, V]) clear() error {
	// release pages from the pool
	for _, overflow := range m.pageList {
		_, err := m.pagePool.Remove(NewPageId(m.bucket, overflow))
		if err != nil {
			return err
		}
	}
	m.last = 0
	m.pageList = m.pageList[0:1]

	return nil
}

func (m *PageList[K, V]) size() int {
	return m.last
}

func (m *PageList[K, V]) next(current *Page[K, V]) (page *Page[K, V], pageId PageId, err error) {
	if current.hasNext {
		pageId = current.next
		page, err = m.pagePool.Get(pageId)
	}

	return
}

func (m *PageList[K, V]) printDump() {
	page, err := m.pagePool.Get(NewPageId(m.bucket, 0)) // fist page from this bucket
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	for page != nil {
		page.forEach(func(k K, v V) {
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
