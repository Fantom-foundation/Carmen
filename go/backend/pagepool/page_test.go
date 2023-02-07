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
		data: make([]byte, 0, byteSize),
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
}

func (p *RawPage) Size() int {
	return len(p.data)
}

func (p *RawPage) IsDirty() bool {
	return p.dirty
}

func (p *RawPage) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*p)
	var v byte
	valSize := unsafe.Sizeof(v)
	// the page is always fully allocated - i.e. use the capacity
	return common.NewMemoryFootprint(selfSize + uintptr(len(p.data))*valSize)
}
