package pagepool

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// TwoFilesPageStorage receives requests to Load or Store pages identified by PageId.
// The PageId contains two integer IDs and the pages are distributed into two files - primary and overflow.
// It allows for distinguishing between primary pages, which have the overflow component of the ID set to zero
// and overflow pages of a primary page.
// Pages are fixed size and are stored in the files at positions corresponding to their IDs either to the primary
// secondary files.
// The TwoFilesPageStorage maintains a fixed size byte buffer used for reading
// and storing pages not to allocate new memory every-time.
type TwoFilesPageStorage struct {
	path string // directory to store the files in

	primaryFile  *FilesPageStorage // primary file contains first pages for the bucket, directly indexed by the bucket number
	overflowFile *FilesPageStorage // overflow file contains next pages for the bucket, indexed by the page id computed by the page pool
}

func NewTwoFilesPageStorage(
	path string,
	pageSize int,
) (storage *TwoFilesPageStorage, err error) {

	primaryFile, err := NewFilesPageStorage(path+"/primaryPages.dat", pageSize)
	if err != nil {
		return
	}

	overflowFile, err := NewFilesPageStorage(path+"/overflowPages.dat", pageSize)
	if err != nil {
		return
	}

	storage = &TwoFilesPageStorage{
		path:         path,
		primaryFile:  primaryFile,
		overflowFile: overflowFile,
	}

	return
}

// Load reads a page of the input ID from the persistent storage.
func (c *TwoFilesPageStorage) Load(pageId PageId, page Page) error {
	// Recover either from primary or overflow buckets
	if pageId.IsOverFlowPage() {
		return c.overflowFile.Load(pageId.Overflow()-1, page)
	} else {
		return c.primaryFile.Load(pageId.Bucket(), page)
	}
}

// Store persists the input page under input key.
func (c *TwoFilesPageStorage) Store(pageId PageId, page Page) (err error) {
	// Recover either from primary or overflow buckets
	if pageId.IsOverFlowPage() {
		return c.overflowFile.Store(pageId.Overflow()-1, page)
	} else {
		return c.primaryFile.Store(pageId.Bucket(), page)
	}
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *TwoFilesPageStorage) Remove(pageId PageId) error {
	if pageId.IsOverFlowPage() {
		return c.overflowFile.Remove(pageId.Overflow() - 1)
	} else {
		return c.primaryFile.Remove(pageId.Bucket())
	}
}

// Flush all changes to the disk
func (c *TwoFilesPageStorage) Flush() (err error) {
	// flush data file changes to disk
	primFileErr := c.primaryFile.Flush()
	overflowFileErr := c.overflowFile.Flush()

	if primFileErr != nil || overflowFileErr != nil {
		err = fmt.Errorf("flush error: Primary file: %s, Overflow file: %s", primFileErr, overflowFileErr)
	}

	return
}

// Close the store
func (c *TwoFilesPageStorage) Close() (err error) {
	flushErr := c.Flush()
	primFileErr := c.primaryFile.Close()
	overflowFileErr := c.overflowFile.Close()

	if flushErr != nil || primFileErr != nil || overflowFileErr != nil {
		err = fmt.Errorf("close error: Flush: %s,  Primary file: %s, Overflow file: %s", flushErr, primFileErr, overflowFileErr)
	}

	return
}

func (c *TwoFilesPageStorage) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("primaryFile", c.primaryFile.GetMemoryFootprint())
	memoryFootprint.AddChild("overflowFile", c.overflowFile.GetMemoryFootprint())
	return memoryFootprint
}
