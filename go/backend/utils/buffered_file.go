package utils

import (
	"errors"
	"fmt"
	"os"
)

// BufferedFile is a wrapper arround an *os.File coordinating seek, read, and
// write operations.
//
// It tracks the position of the file reader internally to avoid seek operation
// calls when already positioned at the right location in a file. Especially for
// sequences of read/write operations targeting consecutiv locations in a file
// this can significantly increase performance by reducing system calls.
//
// The wrapper also adds a small write buffer grouping multiple small writes
// into fewer system calls. Those writes are synchronized with read operations.
// Thus, when writing to the buffer, subsequent reads will see the data in the
// buffer, although it has not yet been written to the file.
type BufferedFile struct {
	file         *os.File         // the file handle to represent
	filesize     int64            // the current size of the file
	position     int64            // the current position in the file
	buffer       [bufferSize]byte // a buffer for write operations
	bufferOffset int64            // the offset of the write buffer
}

const bufferSize = 4092

// OpenBufferedFile opens the file at the given path for read/write operations.
// If it does not exist, a new file is implicitly created.
func OpenBufferedFile(path string) (*BufferedFile, error) {
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
	if size%bufferSize != 0 {
		f.Close()
		return nil, fmt.Errorf("invalid file size, got %d, expected multiple of %d", size, bufferSize)
	}
	res := &BufferedFile{
		file:     f,
		filesize: size,
	}

	if err := res.readInternal(0, res.buffer[:]); err != nil {
		f.Close()
		return nil, err
	}

	return res, nil
}

// Write write the given byte data at the given position in the file. The file
// will be extended in case the target position is beyond the file size.
func (f *BufferedFile) Write(position int64, src []byte) error {

	if len(src) > bufferSize {
		panic(fmt.Sprintf("writing data > %d bytes not supported so far, got %d", bufferSize, len(src)))
	}

	// If the data to be written covers multiple buffer blocks the write needs to be
	// split in two.
	if position/bufferSize != (position+int64(len(src))-1)/bufferSize {
		covered := bufferSize - position%bufferSize
		offsetB := f.bufferOffset + bufferSize
		return errors.Join(
			f.Write(position, src[0:covered]),
			f.Write(offsetB, src[covered:]),
		)
	}

	// Check whether the write operation targets the internal buffer.
	if position >= f.bufferOffset && position+int64(len(src)) <= f.bufferOffset+int64(len(f.buffer)) {
		copy(f.buffer[int(position-f.bufferOffset):], src)
		return nil
	}

	// Flush the buffer and load another.
	if err := f.writeInternal(f.bufferOffset, f.buffer[:]); err != nil {
		return err
	}
	newOffset := position - position%bufferSize
	if err := f.readInternal(newOffset, f.buffer[:]); err != nil {
		return err
	}
	f.bufferOffset = newOffset
	return f.Write(position, src)
}

func (f *BufferedFile) writeInternal(position int64, src []byte) error {
	// Grow file if required.
	if f.filesize < position {
		data := make([]byte, int(position-f.filesize))
		copy(data[int(position-f.filesize):], src)
		if err := f.writeInternal(f.filesize, data); err != nil {
			return err
		}
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

// Read reads a slice of bytes from the file starting at the given position.
// If the targeted range is partially or fully beyond the range of the file,
// uncovered data is zero-padded in the destination slice.
func (f *BufferedFile) Read(position int64, dst []byte) error {
	// Read data from buffer if covered.
	if position >= f.bufferOffset && position+int64(len(dst)) <= f.bufferOffset+int64(len(f.buffer)) {
		copy(dst, f.buffer[int(position-f.bufferOffset):])
		return nil
	}
	return f.readInternal(position, dst)
}

func (f *BufferedFile) readInternal(position int64, dst []byte) error {
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
		if err := f.readInternal(position, dst[0:covered]); err != nil {
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
	n, err := f.file.Read(dst)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return fmt.Errorf("failed to read sufficient bytes from file, wanted %d, got %d", len(dst), n)
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
		f.writeInternal(f.bufferOffset, f.buffer[:]),
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
