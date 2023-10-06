package utils

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type pagedFile struct {
	file        *os.File                    // the file handle to represent
	pagesInFile int64                       // the number of pages in the file
	pages       *common.Cache[int64, *page] // an in-memory page cache
	pool        sync.Pool                   // a pool for recycling pages
}

func OpenPagedFile(path string) (File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	stats, err := os.Stat(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	size := stats.Size()
	if size%pageSize != 0 {
		f.Close()
		return nil, fmt.Errorf("invalid file size, got %d, expected multiple of %d", size, pageSize)
	}
	return &pagedFile{
		file:        f,
		pagesInFile: size / pageSize,
		pages:       common.NewCache[int64, *page](1024), // ~ 4 MB of cache
		pool:        sync.Pool{New: func() any { return new(page) }},
	}, nil
}

func (f *pagedFile) Read(position int64, dst []byte) error {

	for len(dst) > 0 {
		page, err := f.getPage(position / pageSize)
		if err != nil {
			return err
		}

		data := page.data[position%pageSize:]
		n := copy(dst, data)
		dst = dst[n:]

		position += int64(n)
	}

	return nil
}

func (f *pagedFile) Write(position int64, src []byte) error {
	for len(src) > 0 {
		page, err := f.getPage(position / pageSize)
		if err != nil {
			return err
		}

		page.dirty = true
		data := page.data[position%pageSize:]
		n := copy(data, src)
		src = src[n:]

		position += int64(n)
	}
	return nil
}

func (f *pagedFile) Flush() error {
	var flushErr error
	f.pages.Iterate(func(id int64, page *page) bool {
		if page.dirty {
			if err := f.writePage(id, page); err != nil {
				flushErr = err
				return false
			}
			page.dirty = false
		}
		return true
	})
	return flushErr
}

func (f *pagedFile) Close() error {
	return errors.Join(
		f.Flush(),
		f.file.Close(),
	)
}

func (f *pagedFile) getPage(id int64) (*page, error) {
	if res, found := f.pages.Get(id); found {
		return res, nil
	}

	// Read page from file.
	res, err := f.readPage(id)
	if err != nil {
		return nil, err
	}

	// Process evicted file.
	// TODO: consider doing this asynchronous
	if evictedId, evictedPage, evicted := f.pages.Set(id, res); evicted && evictedPage.dirty {
		if err := f.writePage(evictedId, evictedPage); err != nil {
			return nil, err
		}
		evictedPage.clear()
		f.pool.Put(evictedPage)
	}

	return res, nil
}

func (f *pagedFile) readPage(id int64) (*page, error) {
	// Reading data beyond the end of file is allowed and
	// returns zero values.
	res := f.pool.Get().(*page)
	if id >= f.pagesInFile {
		return res, nil
	}
	if _, err := f.file.Seek(id*pageSize, 0); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(f.file, res.data[:]); err != nil {
		return nil, err
	}
	return res, nil
}

func (f *pagedFile) writePage(id int64, page *page) error {
	if !page.dirty {
		return nil
	}

	// add empty pages if needed
	/*
		if f.pagesInFile < id {
			fmt.Printf("growing file from %d to %d pages ..\n", f.pagesInFile, id)
			if _, err := f.file.Seek(f.pagesInFile*pageSize, 0); err != nil {
				return err
			}
			var empty [pageSize]byte
			for f.pagesInFile < id {
				if _, err := f.file.Write(empty[:]); err != nil {
					return err
				}
				f.pagesInFile++
			}
		}
	*/
	if _, err := f.file.Seek(id*pageSize, 0); err != nil {
		return err
	}
	if _, err := f.file.Write(page.data[:]); err != nil {
		return err
	}
	if f.pagesInFile < id+1 {
		f.pagesInFile = id + 1
	}
	return nil
}

const pageSize = 1 << 12 // = 4 KB

type page struct {
	data  [pageSize]byte
	dirty bool
}

func (p *page) clear() {
	p.dirty = false
	p.data = [pageSize]byte{}
}
