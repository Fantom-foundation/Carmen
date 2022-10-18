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
	Data  []byte
	Dirty bool
}

func LoadPage(file *os.File, pageId int, size int64) (*Page, error) {
	bytes := make([]byte, size)

	_, err := file.Seek(int64(pageId)*size, io.SeekStart)
	if err != nil {
		return nil, err
	}

	_, err = file.Read(bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err // the page does not exist in the data file yet
	}

	return &Page{
		Data:  bytes,
		Dirty: false,
	}, nil
}

func (page *Page) Set(position int64, bytes []byte) {
	copy(page.Data[position:position+int64(len(bytes))], bytes)
	page.Dirty = true
}

func (page *Page) Get(position int64, size int64) []byte {
	return page.Data[position : position+size]
}

func (page *Page) Store(file *os.File, pageId int, size int64) error {
	_, err := file.Seek(int64(pageId)*size, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = file.Write(page.Data)
	if err != nil {
		return fmt.Errorf("failed to write the page into file; %s", err)
	}
	return nil
}
