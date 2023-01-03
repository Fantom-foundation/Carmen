package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// MemoryPageStore stores pages in-memory only, its use is mainly for testing.
type MemoryPageStore struct {
	table map[PageId][]byte
}

func NewMemoryPageStore() *MemoryPageStore {
	return &MemoryPageStore{
		table: make(map[PageId][]byte),
	}
}

func (c *MemoryPageStore) Remove(pageId PageId) error {
	delete(c.table, pageId)
	return nil
}

func (c *MemoryPageStore) Store(pageId PageId, page Page) (err error) {
	data := make([]byte, page.SizeBytes())
	page.ToBytes(data)
	c.table[pageId] = data
	return nil
}

func (c *MemoryPageStore) Load(pageId PageId, page Page) error {
	storedPage, exists := c.table[pageId]
	if exists {
		page.FromBytes(storedPage)
	} else {
		page.Clear()
	}
	return nil
}

func (c *MemoryPageStore) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	memfootprint := common.NewMemoryFootprint(selfSize)
	var size uintptr
	for k, v := range c.table {
		size += unsafe.Sizeof(k) + unsafe.Sizeof(v)
	}
	memfootprint.AddChild("pageStore", common.NewMemoryFootprint(size))
	return memfootprint
}
