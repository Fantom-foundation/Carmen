package pagepool

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"unsafe"
)

// FilesPageStorage receives requests to Load or Store pages identified by an ID.
// Pages are fixed size and are stored in the files at positions corresponding to their IDs
// The FilesPageStorage maintains a fixed size byte buffer used for reading
// and storing pages not to allocate new memory every-time.
// The storage maintains last used ID and a list of released IDs. The IDs are re-used for storing further pages
// so there are no unused holes in the file. When a page ID to load is beyond the stored last ID, nothing
// happens not to trigger useless I/O.
type FilesPageStorage struct {
	file *os.File

	removedIDs map[int]bool // hold empty pages not to try to read them
	lastID     int          // hold last item not to touch above the file size

	buffer []byte // a page binary data shared between Load and Store operations not to allocate memory every time.
}

// NewFilesPageStorage creates a new instance with path to the file, it defines the page size to pre-allocate
// the page buffer, and lastID used in the page file.
func NewFilesPageStorage(filePath string, pageSize int, lastID int) (storage *FilesPageStorage, err error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	storage = &FilesPageStorage{
		file:       file,
		removedIDs: make(map[int]bool, freePagesCap),
		lastID:     lastID,
		buffer:     make([]byte, pageSize),
	}

	return
}

// Load reads a page of the input ID from the persistent storage.
func (c *FilesPageStorage) Load(pageId int, page Page) error {
	if !c.shouldLoad(pageId) {
		page.Clear()
		return nil
	}

	offset := int64(pageId * page.SizeBytes())
	if _, err := c.file.ReadAt(c.buffer, offset); err != nil {
		if err == io.EOF {
			// page does not yet exist
			page.Clear()
			return nil
		}
		return err
	}

	page.FromBytes(c.buffer)
	return nil
}

// Store persists the input page under input key.
func (c *FilesPageStorage) Store(pageId int, page Page) (err error) {
	page.ToBytes(c.buffer)

	fileOffset := int64(pageId * page.SizeBytes())
	_, err = c.file.WriteAt(c.buffer, fileOffset)
	if err != nil {
		return
	}

	c.updateUse(pageId)
	return
}

// shouldLoad returns true it the page under pageId should be loaded.
// It happens when the page is not deleted and the pageId does not exceed actual size of the file
func (c *FilesPageStorage) shouldLoad(pageId int) bool {
	// do not necessarily query I/O if the page does not exist,
	// and it allows also for not actually deleting data, it only tracks non-existing items.
	if pageId > c.lastID {
		return false
	}
	if removed, exists := c.removedIDs[pageId]; exists && removed {
		return false
	}

	return true
}

// updateUse sets that the input pageId is not deleted (if it was set so)
// and potentially extends markers of last positions in the dataset.
func (c *FilesPageStorage) updateUse(pageId int) {
	if removed, exists := c.removedIDs[pageId]; exists && removed {
		c.removedIDs[pageId] = false
	}
	if pageId > c.lastID {
		c.lastID = pageId
	}
}

func (c *FilesPageStorage) GetLastId() int {
	return c.lastID
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *FilesPageStorage) Remove(pageId int) error {
	c.removedIDs[pageId] = true
	return nil
}

// Flush all changes to the disk
func (c *FilesPageStorage) Flush() (err error) {
	// flush data file changes to disk
	return c.file.Sync()
}

// Close the store
func (c *FilesPageStorage) Close() (err error) {
	flushErr := c.Flush()
	fileErr := c.file.Close()

	if flushErr != nil || fileErr != nil {
		err = fmt.Errorf("close error: Flush: %s,  file: %s", flushErr, fileErr)
	}

	return
}

func (c *FilesPageStorage) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	var byteType byte
	bufferSize := uintptr(len(c.buffer)) * unsafe.Sizeof(byteType)

	var boolType bool
	var intType int
	removedIDsSize := uintptr(len(c.removedIDs)) * (unsafe.Sizeof(boolType) + unsafe.Sizeof(intType))

	memoryFootprint := common.NewMemoryFootprint(selfSize + bufferSize)
	memoryFootprint.AddChild("removedIds", common.NewMemoryFootprint(removedIDsSize))
	return memoryFootprint
}
