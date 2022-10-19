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

func LoadPage(file *os.File, pageId int, pageSize int64) (*Page, error) {
	buffer := make([]byte, pageSize)

	_, err := file.Seek(int64(pageId)*pageSize, io.SeekStart)
	if err != nil {
		return nil, err
	}

	_, err = file.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) { // EOF = the page does not exist in the data file yet
		return nil, err
	}

	return &Page{
		data:  buffer,
		dirty: false,
	}, nil
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

func (p *Page) Store(file *os.File, pageId int, size int64) error {
	_, err := file.Seek(int64(pageId)*size, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = file.Write(p.data)
	if err != nil {
		return fmt.Errorf("failed to write the page into file; %s", err)
	}
	return nil
}
