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

package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// RawPage wraps only a byte array
type RawPage struct {
	data  []byte
	dirty bool
}

// NewRawPage creates a new page with the size given in bytes
func NewRawPage(byteSize int) *RawPage {
	return &RawPage{
		data:  make([]byte, 0, byteSize),
		dirty: true,
	}
}

func (p *RawPage) ToBytes(pageData []byte) {
	copy(pageData, p.data)
}

func (p *RawPage) FromBytes(pageData []byte) {
	p.data = append(p.data[0:0], pageData...)
	p.dirty = true
}

func (p *RawPage) Clear() {
	p.data = p.data[0:0]
	p.dirty = true
}

func (p *RawPage) Size() int {
	return len(p.data)
}

func (p *RawPage) IsDirty() bool {
	return p.dirty
}

func (p *RawPage) SetDirty(dirty bool) {
	p.dirty = dirty
}

func (p *RawPage) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*p)
	var v byte
	valSize := unsafe.Sizeof(v)
	// the page is always fully allocated - i.e. use the capacity
	return common.NewMemoryFootprint(selfSize + uintptr(len(p.data))*valSize)
}
