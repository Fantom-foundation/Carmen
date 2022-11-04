package pagedfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// Page is the in-memory version of a page of the file store.
// It retains an in-memory copy of the binary data stored in the corresponding page file.
// Furthermore, it provides index based access to the contained data.
type Page struct {
	data  []byte
	dirty bool
}

func (p *Page) Load(file *os.File, pageId int) error {
	n, err := file.ReadAt(p.data, int64(pageId)*int64(len(p.data)))
	if err != nil && !errors.Is(err, io.EOF) { // EOF = the page does not exist in the data file yet
		return err
	}

	for ; n < len(p.data); n++ {
		p.data[n] = 0x00
	}
	p.dirty = false
	return nil
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

func (p *Page) Get(position int64, size int64) []byte {
	return p.data[position : position+size]
}

func (p *Page) Store(file *os.File, pageId int) error {
	_, err := file.WriteAt(p.data, int64(pageId)*int64(len(p.data)))
	if err != nil {
		return fmt.Errorf("failed to write the page into file; %s", err)
	}
	p.dirty = false
	return nil
}
