package pagepool

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"unsafe"
)

const freePagesCap = 10 // initial size of freed pages set.

// FilePageStorage receives requests to Load or Store pages identified by PageId.
// The PageId contains two integer IDs and the pages are distributed into two files - primary and overflow.
// It allows for distinguishing between primary pages, which have the overflow component of the ID set to zero
// and overflow pages of a primary page.
// Pages are fixed size and are stored in the files at positions corresponding to their IDs either to the primary
// secondary files.
// The FilePageStorage maintains a fixed size byte buffer used for reading
// and storing pages not to allocate new memory every-time.
// On Store execution, the stored page memory representation is kept in the free list and reused for a following Load execution.
// The free list is caped not to exhaust memory when many Store operations is executed without follow-up Load executions.
type FilePageStorage[K comparable, I comparable] struct {
	path            string // directory to store the files in
	pageSize        int    // page size in bytes
	pageItems       int    // max items per page
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	comparator      common.Comparator[K]

	primaryFile  *os.File // primary file contains first pages for the bucket, directly indexed by the bucket number
	overflowFile *os.File // overflow file contains next pages for the bucket, indexed by the page id computed by the page pool

	removedBuckets   map[int]bool // hold empty pages not to try to read them
	removedOverflows map[int]bool // hold empty pages not to try to read them
	lastBucket       int          // hold last item not to touch above the file size
	lastOverflow     int          // hold last item not to touch above the file size

	buffer []byte // a page binary data shared between Load and Store operations not to allocate memory every time.
}

func NewFilePageStorage[K comparable, I comparable](
	path string,
	pageSize int,
	pageItems int,
	lastBucket int,
	lastOverflow int,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I],
	comparator common.Comparator[K],
) (storage *FilePageStorage[K, I], err error) {

	primaryFile, err := os.OpenFile(path+"/primaryPages.dat", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	overflowFile, err := os.OpenFile(path+"/overflowPages.dat", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	list := make([]*common.MapEntry[K, I], pageItems)
	for i := 0; i < pageItems; i++ {
		list[i] = &common.MapEntry[K, I]{}
	}

	storage = &FilePageStorage[K, I]{
		path:             path,
		pageSize:         pageSize,
		pageItems:        pageItems,
		lastBucket:       lastBucket,
		lastOverflow:     lastOverflow,
		primaryFile:      primaryFile,
		overflowFile:     overflowFile,
		keySerializer:    keySerializer,
		indexSerializer:  indexSerializer,
		comparator:       comparator,
		removedBuckets:   make(map[int]bool, freePagesCap),
		removedOverflows: make(map[int]bool, freePagesCap),
		buffer:           make([]byte, pageSize),
	}

	return
}

// Load reads a page of the input ID from the persistent storage.
func (c *FilePageStorage[K, I]) Load(pageId PageId, page *Page[K, I]) error {
	if !c.shouldLoad(pageId) {
		page.Clear()
		return nil
	}

	pageData := c.buffer
	file := c.primaryFile
	pageNumber := pageId.Bucket()
	// Recover either from primary or overflow buckets
	if pageId.Overflow() != 0 { // even nicer: pageId.IsOverFlowPage()
		file = c.overflowFile
		pageNumber = pageId.Overflow() - 1
	}
	offset := int64(pageNumber * c.pageSize)
	if _, err := file.ReadAt(pageData, offset); err != nil {
		if err == io.EOF {
			// page does not yet exist
			page.Clear()
			return nil // maybe reset page first!
		}
		return err
	}

	// read in metadata - link to the next page
	next := binary.BigEndian.Uint32(pageData[len(pageData)-4:])
	if next != 0 {
		page.SetNext(NewPageId(pageId.Bucket(), int(next)))
	}
	numItems := binary.BigEndian.Uint16(pageData[len(pageData)-6:])

	// convert to key value pairs
	keySize := c.keySerializer.Size()
	valSize := c.indexSerializer.Size()
	pairSize := keySize + valSize

	// update directly the entries for the best speed
	list := page.GetRaw()
	var dataIndex uint16
	for i := 0; i < c.pageSize-pairSize; i += pairSize {
		if numItems == dataIndex {
			break
		}
		key := c.keySerializer.FromBytes(pageData[i : i+keySize])
		val := c.indexSerializer.FromBytes(pageData[i+keySize : i+keySize+valSize])
		list[dataIndex].Key = key
		list[dataIndex].Val = val

		dataIndex += 1
	}
	page.SetSize(int(numItems))

	return nil
}

// Store persists the input page under input key.
func (c *FilePageStorage[K, I]) Store(pageId PageId, page *Page[K, I]) (err error) {
	pageData := c.buffer
	// convert data from key-value pairs into byte array Page
	var offset int
	keySize := c.keySerializer.Size()
	valueSize := c.indexSerializer.Size()

	for _, item := range page.GetEntries() {
		c.keySerializer.CopyBytes(item.Key, pageData[offset:offset+keySize])
		c.indexSerializer.CopyBytes(item.Val, pageData[offset+keySize:offset+keySize+valueSize])
		offset += keySize + valueSize
	}

	// put in metadata - the link to the next page
	if page.HasNext() {
		binary.BigEndian.PutUint32(pageData[len(pageData)-4:], uint32(page.NextPage().Overflow()))
	} else {
		binary.BigEndian.PutUint32(pageData[len(pageData)-4:], uint32(0))
	}

	// number of keys
	binary.BigEndian.PutUint16(pageData[len(pageData)-6:len(pageData)-4], uint16(page.Size()))

	file := c.primaryFile
	pageNumber := pageId.Bucket()
	// Recover either from primary or overflow buckets
	if pageId.Overflow() != 0 { // even nicer: pageId.IsOverFlowPage()
		file = c.overflowFile
		pageNumber = pageId.Overflow() - 1
	}
	fileOffset := int64(pageNumber * c.pageSize)
	_, err = file.WriteAt(pageData, fileOffset)
	if err != nil {
		return
	}

	c.updateUse(pageId)
	return
}

// shouldLoad returns true it the page under pageId should be loaded.
// It happens when the page is not deleted and the pageId does not exceed actual size of the file
func (c *FilePageStorage[K, I]) shouldLoad(pageId PageId) bool {
	// do not necessarily query I/O if the page does not exist,
	// and it allows also for not actually deleting data, it only tracks non-existing items.
	if pageId.Bucket() > c.lastBucket {
		return false
	}
	if pageId.Overflow() == 0 {
		if removed, exists := c.removedBuckets[pageId.Bucket()]; exists && removed {
			return false
		}
	} else {
		if pageId.Overflow() > c.lastOverflow {
			return false
		}

		if removed, exists := c.removedOverflows[pageId.Overflow()]; exists && removed {
			return false
		}
	}

	return true
}

// updateUse sets that the input pageId is not deleted (if it was set so)
// and potentially extends markers of last positions in the dataset.
func (c *FilePageStorage[K, I]) updateUse(pageId PageId) {
	if removed, exists := c.removedBuckets[pageId.Bucket()]; exists && removed {
		c.removedBuckets[pageId.Bucket()] = false
	}
	if removed, exists := c.removedOverflows[pageId.Overflow()]; exists && removed {
		c.removedOverflows[pageId.Overflow()] = false
	}
	if pageId.Bucket() >= c.lastBucket {
		c.lastBucket = pageId.Bucket()
	}
	if pageId.Overflow() >= c.lastOverflow {
		c.lastOverflow = pageId.Overflow()
	}
}

func (c *FilePageStorage[K, I]) GetLastId() PageId {
	return NewPageId(c.lastBucket, c.lastOverflow)
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *FilePageStorage[K, I]) Remove(pageId PageId) error {
	if pageId.Overflow() == 0 {
		c.removedBuckets[pageId.Bucket()] = true
	}
	c.removedOverflows[pageId.Overflow()] = true
	return nil
}

// Flush all changes to the disk
func (c *FilePageStorage[K, I]) Flush() (err error) {
	// flush data file changes to disk
	primFileErr := c.primaryFile.Sync()
	overflowFileErr := c.overflowFile.Sync()

	if primFileErr != nil || overflowFileErr != nil {
		err = fmt.Errorf("flush error: Primary file: %s, Overflow file: %s", primFileErr, overflowFileErr)
	}

	return
}

// Close the store
func (c *FilePageStorage[K, I]) Close() (err error) {
	flushErr := c.Flush()
	primFileErr := c.primaryFile.Close()
	overflowFileErr := c.overflowFile.Close()

	if flushErr != nil || primFileErr != nil || overflowFileErr != nil {
		err = fmt.Errorf("close error: Flush: %s,  Primary file: %s, Overflow file: %s", flushErr, primFileErr, overflowFileErr)
	}

	return
}

func (c *FilePageStorage[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	var byteType byte
	bufferSize := uintptr(len(c.buffer)) * unsafe.Sizeof(byteType)

	var boolType bool
	var intType int
	removedBucketsSize := uintptr(len(c.removedBuckets)) * (unsafe.Sizeof(boolType) + unsafe.Sizeof(intType))
	removedOverflowsSize := uintptr(len(c.removedOverflows)) * (unsafe.Sizeof(boolType) + unsafe.Sizeof(intType))

	memoryFootprint := common.NewMemoryFootprint(selfSize + bufferSize)
	memoryFootprint.AddChild("removedIds", common.NewMemoryFootprint(removedBucketsSize+removedOverflowsSize))
	return memoryFootprint
}

// MemoryPageStore stores pages in-memory only, its use is mainly for testing.
type MemoryPageStore[K comparable, V comparable] struct {
	table map[PageId]Page[K, V]
}

func NewMemoryPageStore[K comparable, V comparable]() *MemoryPageStore[K, V] {
	return &MemoryPageStore[K, V]{
		table: make(map[PageId]Page[K, V]),
	}
}

func (c *MemoryPageStore[K, I]) Remove(pageId PageId) error {
	delete(c.table, pageId)
	return nil
}

func (c *MemoryPageStore[K, I]) Store(pageId PageId, page *Page[K, I]) (err error) {
	// store a pageCopy just like a persistent storage
	copyPage := NewPage[K, I](len(page.list), page.comparator)
	copyPage.hasNext = page.hasNext
	copyPage.next = page.next
	list := copyPage.GetRaw()
	var i int
	page.ForEach(func(k K, v I) {
		list[i] = common.MapEntry[K, I]{k, v}
		i += 1
	})
	copyPage.SetSize(page.Size())
	c.table[pageId] = *copyPage
	return nil
}

func (c *MemoryPageStore[K, I]) Load(pageId PageId, page *Page[K, I]) error {
	storedPage, exists := c.table[pageId]
	if exists {
		list := page.GetRaw()
		var i int
		storedPage.ForEach(func(k K, v I) {
			list[i] = common.MapEntry[K, I]{k, v}
			i += 1
		})
		page.SetSize(storedPage.Size())
		page.hasNext = storedPage.hasNext
		page.next = storedPage.next
	} else {
		page.Clear()
	}
	return nil
}

func (c *MemoryPageStore[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	memfootprint := common.NewMemoryFootprint(selfSize)
	var pageId PageId
	var page Page[K, V]
	size := uintptr(len(c.table)) * (unsafe.Sizeof(pageId) + unsafe.Sizeof(page))
	memfootprint.AddChild("pageStore", common.NewMemoryFootprint(size))
	return memfootprint
}
