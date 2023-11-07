package utils

import (
	"io/fs"
	"os"
)

//go:generate mockgen -source file.go -destination file_mocks.go -package utils

// OsFile interface represents selected methods of build-in os.File struct.
// This interface is provided to enable mocking.
type OsFile interface {
	Write(b []byte) (n int, err error)
	Stat() (os.FileInfo, error)
	Seek(offset int64, whence int) (ret int64, err error)
	Read(p []byte) (n int, err error)
	Sync() error
	Close() error
}

type FileInfo interface {
	fs.FileInfo
}