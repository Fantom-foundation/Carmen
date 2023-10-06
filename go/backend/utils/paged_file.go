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
	file          *os.File                    // the file handle to represent
	fileMutex     sync.Mutex                  // mutex must be hold when interacting with the file
	pagesInFile   int64                       // the number of pages in the file
	pages         *common.Cache[int64, *page] // an in-memory page cache
	pool          sync.Pool                   // a pool for recycling pages
	writeQueue    chan<- writeTask            // a queue for sending write jobs to async writer
	flushDone     <-chan struct{}             // a signal channel for writer to report on finished syncs
	done          <-chan struct{}             // a signal channel of writer to report termination
	writeErr      error                       // track errors occurred during writing
	writeErrMutex sync.Mutex                  // a lock for accessing writeErr
	closed        bool                        // flag indicating whether the file has been closed
	pending       map[int64]*pendingPage
	pendingMutex  sync.Mutex
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

	queue := make(chan writeTask, 32)
	flushDone := make(chan struct{})
	done := make(chan struct{})

	res := &pagedFile{
		file:        f,
		pagesInFile: size / pageSize,
		pages:       common.NewCache[int64, *page](1024), // ~ 4 MB of cache
		pool:        sync.Pool{New: func() any { return new(page) }},
		writeQueue:  queue,
		flushDone:   flushDone,
		done:        done,
		pending:     map[int64]*pendingPage{},
	}

	// Start a background go-routine performing write operations.
	go res.processWrites(queue, flushDone, done)

	return res, nil
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
	if f.closed {
		return nil
	}
	f.pages.Iterate(func(id int64, page *page) bool {
		if page.dirty {
			f.writeQueue <- writeTask{
				id:      id,
				page:    page,
				release: false,
			}
		}
		return true
	})

	// Wait until all writes are done.
	f.writeQueue <- writeTask{sync: true}
	<-f.flushDone

	f.writeErrMutex.Lock()
	defer f.writeErrMutex.Unlock()
	return f.writeErr
}

func (f *pagedFile) Close() error {
	// TODO: synchronize close!
	if f.closed {
		return nil
	}
	if err := f.Flush(); err != nil {
		return err
	}
	// Stop the writer goroutine.
	close(f.writeQueue)
	<-f.done
	f.closed = true
	return f.file.Close()
}

func (f *pagedFile) getPage(id int64) (*page, error) {
	if res, found := f.pages.Get(id); found {
		return res, nil
	}

	// Check pending writes for the required page.
	f.pendingMutex.Lock()
	var res *page
	pending, found := f.pending[id]
	if found {
		// Cancel write and re-instate.
		delete(f.pending, id)
		res = pending.page
		pending.lock.Lock() // Wait until the pending page is available
	}
	f.pendingMutex.Unlock()

	// Read page from file.
	if res == nil {
		page, err := f.readPage(id)
		if err != nil {
			return nil, err
		}
		res = page
	}

	// Process evicted file.
	if evictedId, evictedPage, evicted := f.pages.Set(id, res); evicted && evictedPage.dirty {
		f.pendingMutex.Lock()
		f.pending[evictedId] = &pendingPage{page: evictedPage}
		f.pendingMutex.Unlock()
		f.writeQueue <- writeTask{
			id:      evictedId,
			page:    evictedPage,
			release: true,
		}
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
	f.fileMutex.Lock()
	defer f.fileMutex.Unlock()
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
	f.fileMutex.Lock()
	defer f.fileMutex.Unlock()
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

func (f *pagedFile) processWrites(queue <-chan writeTask, flushDone chan<- struct{}, done chan<- struct{}) {
	defer close(done)
	defer close(flushDone)
	for task := range queue {
		if task.sync {
			flushDone <- struct{}{}
		} else if !task.release {
			// This write instruction is part of a flush; we just need
			// to write the node, no handling of pending lists required.
			if err := f.writePage(task.id, task.page); err != nil {
				f.writeErrMutex.Lock()
				f.writeErr = errors.Join(f.writeErr, err)
				f.writeErrMutex.Unlock()
			}
		} else {
			// check whether the write was canceled
			f.pendingMutex.Lock()
			pendingPage, found := f.pending[task.id]
			if !found {
				f.pendingMutex.Unlock()
				continue
			}
			pendingPage.lock.Lock()
			f.pendingMutex.Unlock()

			if err := f.writePage(task.id, pendingPage.page); err != nil {
				f.writeErrMutex.Lock()
				f.writeErr = errors.Join(f.writeErr, err)
				f.writeErrMutex.Unlock()
			}

			pendingPage.lock.Unlock()

			// remove page from set of pending pages (if it is still there)
			f.pendingMutex.Lock()
			_, found = f.pending[task.id]
			if found {
				delete(f.pending, task.id)
			}
			f.pendingMutex.Unlock()

			// Release the page, unless it was fetched from the pending set.
			if found {
				pendingPage.page.clear()
				if task.release {
					f.pool.Put(pendingPage.page)
				}
			}
		}
	}
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

type writeTask struct {
	id      int64
	page    *page
	release bool
	sync    bool
}

type pendingPage struct {
	page *page
	lock sync.Mutex
}
