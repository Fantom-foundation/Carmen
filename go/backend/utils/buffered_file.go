// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package utils

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// BufferedFile is a wrapper around an *os.File coordinating seek, read, and
// write operations.
//
// It tracks the position of the file reader internally to avoid seek operation
// calls when already positioned at the right location in the file. Especially for
// sequences of read/write operations targeting consecutive locations in a file,
// this can significantly increase performance by reducing system calls.
//
// The wrapper also adds a small write buffer grouping multiple small writes
// into fewer system calls. Those writes are synchronized with read operations.
// Thus, when writing to the buffer, subsequent reads will see the data in the
// buffer, although it has not yet been written to the file.
type BufferedFile struct {
	file         OsFile           // the file handle to represent
	filesize     int64            // the current size of the file
	position     int64            // the current position in the file
	buffer       [bufferSize]byte // a buffer for write operations
	bufferOffset int64            // the offset of the write buffer
}

const bufferSize = 1 << 12 // = 4 KB

// OpenBufferedFile opens the file at the given path for read/write operations.
// If it does not exist, a new file is implicitly created.
func OpenBufferedFile(path string) (*BufferedFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return openBufferedFile(f)
}

func openBufferedFile(f OsFile) (*BufferedFile, error) {
	stats, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := stats.Size()
	if size%bufferSize != 0 {
		f.Close()
		return nil, fmt.Errorf("invalid file size, got %d, expected multiple of %d", size, bufferSize)
	}
	res := &BufferedFile{
		file:     f,
		filesize: size,
	}

	if err := res.readFile(0, res.buffer[:]); err != nil {
		f.Close()
		return nil, err
	}

	return res, nil
}

// WriteAt writes the given byte data at the given position in the file. The file
// will be extended in case the target position is beyond the file size.
func (f *BufferedFile) WriteAt(src []byte, position int64) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	if len(src) > bufferSize {
		return 0, fmt.Errorf(fmt.Sprintf("writing data > %d bytes not supported so far, got %d", bufferSize, len(src)))
	}

	if position < 0 {
		return 0, fmt.Errorf("cannot write at negative position: %d", position)
	}

	// If the data to be written covers multiple buffer blocks the write needs to be
	// split in two.
	from, to := position, position+int64(len(src))
	if position/bufferSize != (to-1)/bufferSize {
		covered := bufferSize - position%bufferSize
		nextBlock := position + covered
		n1, err1 := f.WriteAt(src[0:covered], position)
		n2, err2 := f.WriteAt(src[covered:], nextBlock)
		return n1 + n2, errors.Join(err1, err2)
	}

	// Check whether the write operation targets the internal buffer.
	if f.bufferOffset <= from && to <= f.bufferOffset+int64(len(f.buffer)) {
		n := copy(f.buffer[int(position-f.bufferOffset):], src)
		return n, nil
	}

	// Flush the buffer and load another.
	if err := f.writeFile(f.bufferOffset, f.buffer[:]); err != nil {
		return 0, err
	}
	newOffset := position - position%bufferSize
	if err := f.readFile(newOffset, f.buffer[:]); err != nil {
		return 0, err
	}
	f.bufferOffset = newOffset
	return f.WriteAt(src, position)
}

func (f *BufferedFile) writeFile(position int64, src []byte) error {
	// Grow file if required.
	if f.filesize < position {
		padding := int(position - f.filesize)
		data := make([]byte, padding+len(src))
		copy(data[padding:], src)
		return f.writeFile(f.filesize, data)
	}
	if err := f.seek(position); err != nil {
		return err
	}
	n, err := f.file.Write(src)
	if err != nil {
		return err
	}
	if n != len(src) {
		return fmt.Errorf("failed to write sufficient bytes to file, wanted %d, got %d", len(src), n)
	}
	f.position += int64(n)
	if f.position > f.filesize {
		f.filesize = f.position
	}
	return nil
}

// ReadAt reads a slice of bytes from the file starting at the given position.
// If the targeted range is partially or fully beyond the range of the file,
// uncovered data is zero-padded in the destination slice.
func (f *BufferedFile) ReadAt(dst []byte, position int64) (int, error) {
	if position < 0 {
		return 0, fmt.Errorf("cannot read at negative index: %d", position)
	}

	from, to := position, position+int64(len(dst))
	bufferFrom, bufferTo := f.bufferOffset, f.bufferOffset+bufferSize

	// Read data from buffer if covered.
	if bufferFrom <= from && to <= bufferTo {
		n := copy(dst, f.buffer[int(from-bufferFrom):])
		return n, nil
	}

	// Split read if partially covered by write buffer.
	if from < bufferTo && bufferTo < to {
		covered := bufferTo - from
		n1, err1 := f.ReadAt(dst[0:covered], from)
		n2, err2 := f.ReadAt(dst[covered:], bufferTo)
		return n1 + n2, errors.Join(err1, err2)
	}

	if from < bufferFrom && bufferFrom < to {
		notCovered := len(dst) - int(to-bufferFrom)
		n1, err1 := f.ReadAt(dst[0:notCovered], from)
		n2, err2 := f.ReadAt(dst[notCovered:], bufferFrom)
		return n1 + n2, errors.Join(err1, err2)
	}

	// If not covered by the buffer at all, read from the file.
	return len(dst), f.readFile(position, dst)
}

func (f *BufferedFile) readFile(position int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	// If read segment exceeds the current file size, read covered part and pad with zeros.
	if position >= f.filesize {
		for i := range dst {
			dst[i] = 0
		}
		return nil
	}
	if position+int64(len(dst)) > f.filesize {
		covered := f.filesize - position
		if err := f.readFile(position, dst[0:covered]); err != nil {
			return err
		}
		for i := covered; i < int64(len(dst)); i++ {
			dst[i] = 0
		}
		return nil
	}
	if err := f.seek(position); err != nil {
		return err
	}
	n, err := io.ReadFull(f.file, dst)
	if err != nil {
		return err
	}
	f.position += int64(n)
	return nil
}

func (f *BufferedFile) seek(position int64) error {
	if f.position == position {
		return nil
	}
	pos, err := f.file.Seek(position, 0)
	if err != nil {
		return err
	}
	if pos != position {
		return fmt.Errorf("failed to seek to required position, wanted %d, got %d", position, pos)
	}
	f.position = position
	return nil
}

// Flush syncs temporary cached content to the file system.
func (f *BufferedFile) Flush() error {
	return errors.Join(
		f.writeFile(f.bufferOffset, f.buffer[:]),
		f.file.Sync(),
	)
}

// Close flushes and closes this file.
func (f *BufferedFile) Close() error {
	return errors.Join(
		f.Flush(),
		f.file.Close(),
	)
}
