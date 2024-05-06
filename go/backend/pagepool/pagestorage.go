// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package pagepool

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"unsafe"
)

// FilePageStorage receives requests to Load or Store pages identified by an ID.
// Pages are fixed size and are stored in the files at positions corresponding to their IDs
// The FilePageStorage maintains a fixed size byte buffer used for reading
// and storing pages not to allocate new memory every-time.
// The storage maintains last used ID and a list of released IDs. The IDs are re-used for storing further pages
// so there are no unused holes in the file. When a page ID to load is beyond the stored last ID, nothing
// happens not to trigger useless I/O.
type FilePageStorage struct {
	file *os.File

	freeIdsMap map[int]bool // free page IDs tracks deleted pages so that requests for loading them
	// do not necessarily query I/O and also Pages do not have to be actually deleted from file
	freeIds  []int // list of free IDs that are re-used for new pages
	nextID   int   // hold last item not to touch above the file size
	pageSize int

	buffer []byte // a page binary data shared between Load and Store operations not to allocate memory every time.
}

// NewFilePageStorage creates a new instance with path to the file, it defines the page size to pre-allocate
// the page buffer.
func NewFilePageStorage(filePath string, pageSize int) (storage *FilePageStorage, err error) {
	removedIDs, lastID, err := readMetadata(filePath, pageSize)
	if err != nil {
		return
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	freeIds := make([]int, 0, len(removedIDs))
	for k := range removedIDs {
		freeIds = append(freeIds, k)
	}

	storage = &FilePageStorage{
		file:       file,
		freeIdsMap: removedIDs,
		freeIds:    freeIds,
		pageSize:   pageSize,
		nextID:     lastID,
		buffer:     make([]byte, pageSize),
	}

	return
}

// Load reads a page of the input ID from the persistent storage.
func (c *FilePageStorage) Load(pageId int, page Page) error {
	if !c.shouldLoad(pageId) {
		page.Clear()
		return nil
	}

	offset := int64(pageId * c.pageSize)
	if _, err := c.file.ReadAt(c.buffer, offset); err != nil {
		if err == io.EOF {
			// page does not yet exist
			page.Clear()
			return nil
		}
		return err
	}

	page.FromBytes(c.buffer)
	page.SetDirty(false)
	return nil
}

// Store persists the input page under input key.
func (c *FilePageStorage) Store(pageId int, page Page) (err error) {
	page.ToBytes(c.buffer)

	fileOffset := int64(pageId * c.pageSize)
	_, err = c.file.WriteAt(c.buffer, fileOffset)
	if err != nil {
		return
	}

	c.updateUse(pageId)
	page.SetDirty(false)
	return
}

func (c *FilePageStorage) GenerateNextId() int {
	var id int
	if len(c.freeIds) > 0 {
		id = c.freeIds[len(c.freeIds)-1]
		c.freeIds = c.freeIds[0 : len(c.freeIds)-1]
	} else {
		id = c.nextID
		c.nextID += 1
	}

	// treat as free at first to prevent tangling IDs when actually not used
	c.freeIdsMap[id] = true

	return id
}

// GetLastId returns the id of the last page in the storage
func (c *FilePageStorage) GetLastId() int {
	return c.nextID - 1
}

// shouldLoad returns true it the page under pageId should be loaded.
// It happens when the page is not deleted and the pageId does not exceed actual size of the file.
func (c *FilePageStorage) shouldLoad(pageId int) bool {
	// do not necessarily query I/O if the page does not exist,
	// and it allows also for not actually deleting data, it only tracks non-existing items.
	if pageId >= c.nextID {
		return false
	}
	if removed, exists := c.freeIdsMap[pageId]; exists && removed {
		return false
	}

	return true
}

// updateUse sets that the input pageId is not deleted (if it was set so)
// and potentially extends markers of last positions in the dataset.
func (c *FilePageStorage) updateUse(pageId int) {
	if removed, exists := c.freeIdsMap[pageId]; exists && removed {
		c.freeIdsMap[pageId] = false
	}
	if pageId >= c.nextID {
		c.nextID = pageId + 1
	}
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *FilePageStorage) Remove(pageId int) error {
	c.freeIdsMap[pageId] = true
	c.freeIds = append(c.freeIds, pageId)
	return nil
}

// Flush all changes to the disk
func (c *FilePageStorage) Flush() (err error) {
	// flush data file changes to disk
	if err := c.writeMetadata(); err != nil {
		return err
	}

	return c.file.Sync()
}

// Close the store
func (c *FilePageStorage) Close() (err error) {
	flushErr := c.Flush()
	fileErr := c.file.Close()

	if flushErr != nil || fileErr != nil {
		err = fmt.Errorf("close error: Flush: %s,  file: %s", flushErr, fileErr)
	}

	return
}

func (c *FilePageStorage) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	var byteType byte
	bufferSize := uintptr(len(c.buffer)) * unsafe.Sizeof(byteType)

	var boolType bool
	var intType int
	removedIDsSize := uintptr(len(c.freeIdsMap)) * (unsafe.Sizeof(boolType) + unsafe.Sizeof(intType))
	freeIdsSize := uintptr(len(c.freeIds)) * unsafe.Sizeof(intType)

	memoryFootprint := common.NewMemoryFootprint(selfSize + bufferSize)
	memoryFootprint.AddChild("removedIds", common.NewMemoryFootprint(removedIDsSize))
	memoryFootprint.AddChild("freeIds", common.NewMemoryFootprint(freeIdsSize))
	return memoryFootprint
}

func readMetadata(filePath string, pageSize int) (removedIDs map[int]bool, lastID int, err error) {
	removedIDs = make(map[int]bool)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) || err == os.ErrNotExist || err == io.EOF {
			return removedIDs, lastID, nil
		}
		return removedIDs, lastID, err
	}
	defer func() {
		file.Close()
	}()

	// data are structured as
	// [page1][page2]...[pageN][freeIds][lastId]
	// seek at the last Uint34 in the file and read the LastID
	metadataEndOffset, err := file.Seek(-4, io.SeekEnd)
	if err != nil {
		return removedIDs, lastID, err
	}

	lastID, err = parseLastId(file)
	if err != nil {
		return removedIDs, lastID, err
	}

	// read removed IDs that are stored after pages and before the LastID
	metadataStartOffset := int64((lastID) * pageSize)
	offsetWindow := metadataEndOffset - metadataStartOffset
	metadata := make([]byte, offsetWindow)
	if _, err := file.ReadAt(metadata, metadataStartOffset); err != nil {
		return removedIDs, lastID, err
	}

	// convert to a map
	for i := int64(0); i < offsetWindow; i += 4 {
		id := int(binary.LittleEndian.Uint32(metadata[i : i+4]))
		removedIDs[id] = true
	}

	return removedIDs, lastID, nil
}

func parseLastId(reader io.Reader) (int, error) {
	buffer := make([]byte, 4)
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(buffer)), nil
}

func (c *FilePageStorage) writeMetadata() error {
	// data are structured as
	// [page1][page2]...[pageN][freeIds][lastId]
	metadata := make([]byte, 0, 4*len(c.freeIdsMap))
	for id, removed := range c.freeIdsMap {
		if removed {
			metadata = binary.LittleEndian.AppendUint32(metadata, uint32(id))
		}
	}
	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(c.nextID))
	// append at the end, after data
	fileOffset := int64((c.nextID) * c.pageSize)
	_, err := c.file.WriteAt(metadata, fileOffset)
	return err
}
