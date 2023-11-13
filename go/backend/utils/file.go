package utils

import (
	"io"
	"io/fs"
	"os"
)

//go:generate mockgen -source file.go -destination file_mocks.go -package utils

// OsFile provides a layer of abstraction
// between file-level primitives like the buffered file, stock, or stacks
// and build-in os.File operations to facilitate the effective testing of those constructs.
// This interface comprehends selected methods of build-in os.File struct
// to provide interoperability between clients and this struct while enabling mocking.
type OsFile interface {
	io.ReadWriteCloser
	io.Seeker

	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
}

// FileInfo interfaces is an equal representation of the build-in fs.FileInfo interface,
// provided for easier mocking.
type FileInfo interface {
	fs.FileInfo
}

// SeekableFile is an interface for files that can be read or written
// at or from the given position.
// This interface provides a layer of abstraction
// between file-level primitives like the buffered file, stock, or stacks
// and operating system operations to facilitate the effective testing of those constructs.
// TODO update to match interfaces io.ReaderAt, io.WriterAt
type SeekableFile interface {
	io.Closer

	// Write writes the given byte data at the given position in the file. The file
	// will be extended in case the target position is beyond the file size.
	Write(position int64, src []byte) error

	// Read reads a slice of bytes from the file starting at the given position.
	// If the targeted range is partially or fully beyond the range of the file,
	// uncovered data is zero-padded in the destination slice.
	Read(position int64, dst []byte) error

	// Flush syncs temporary cached content to the file system.
	Flush() error
}
