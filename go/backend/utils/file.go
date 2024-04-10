package utils

import (
	"github.com/Fantom-foundation/Carmen/go/common"
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
type SeekableFile interface {
	io.ReaderAt
	io.WriterAt
	common.FlushAndCloser
}
