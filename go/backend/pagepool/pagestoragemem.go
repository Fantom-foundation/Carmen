//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

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
