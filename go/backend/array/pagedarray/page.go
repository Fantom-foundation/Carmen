//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package pagedarray

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Page is the in-memory version of a page of the file store.
// It retains an in-memory copy of the binary data stored in the corresponding page file.
// Furthermore, it provides index based access to the contained data.
type Page struct {
	data  []byte
	dirty bool
}

func NewPage(byteSize int) *Page {
	return &Page{
		data:  make([]byte, byteSize),
		dirty: true,
	}
}

func (p *Page) FromBytes(pageData []byte) {
	copy(p.data, pageData)
	p.dirty = true
}

func (p *Page) ToBytes(pageData []byte) {
	copy(pageData, p.data)
}

func (p *Page) Clear() {
	for i := 0; i < len(p.data); i++ {
		p.data[i] = 0x00
	}
	p.dirty = true
}

func (p *Page) IsDirty() bool {
	return p.dirty
}

func (p *Page) GetContent() []byte {
	return p.data
}

func (p *Page) Set(position int64, bytes []byte) {
	copy(p.data[position:position+int64(len(bytes))], bytes)
	p.dirty = true
}

func (p *Page) SetDirty(dirty bool) {
	p.dirty = dirty
}

func (p *Page) Size() int {
	return len(p.data)
}

func (p *Page) Get(position int64, size int) []byte {
	return p.data[position : position+int64(size)]
}

func (p *Page) GetMemoryFootprint() *common.MemoryFootprint {
	var b byte
	pageSize := unsafe.Sizeof(*p) + unsafe.Sizeof(b)*uintptr(len(p.data))
	return common.NewMemoryFootprint(pageSize)
}
