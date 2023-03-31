package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// MemoryPageStore stores pages in-memory only, its use is mainly for testing.
type MemoryPageStore[T comparable] struct {
	table  map[T][]byte
	nextId func() T
}

func NewMemoryPageStore[T comparable](nextId func() T) *MemoryPageStore[T] {
	return &MemoryPageStore[T]{
		table:  make(map[T][]byte),
		nextId: nextId,
	}
}

func (c *MemoryPageStore[T]) Remove(pageId T) error {
	delete(c.table, pageId)
	return nil
}

func (c *MemoryPageStore[T]) Store(pageId T, page Page) (err error) {
	data := make([]byte, page.Size())
	page.ToBytes(data)
	c.table[pageId] = data
	return nil
}

func (c *MemoryPageStore[T]) Load(pageId T, page Page) error {
	storedPage, exists := c.table[pageId]
	if exists {
		page.FromBytes(storedPage)
	} else {
		page.Clear()
	}
	return nil
}

func (c *MemoryPageStore[T]) GenerateNextId() T {
	return c.nextId()
}

func (c *MemoryPageStore[T]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	memfootprint := common.NewMemoryFootprint(selfSize)
	var size uintptr
	for k, v := range c.table {
		size += unsafe.Sizeof(k) + unsafe.Sizeof(v)
	}
	memfootprint.AddChild("pageStore", common.NewMemoryFootprint(size))
	return memfootprint
}
