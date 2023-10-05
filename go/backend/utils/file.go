package utils

// File is a common interface for a random-access file abstraction.
type File interface {
	Read(position int64, dst []byte) error
	Write(position int64, src []byte) error
	Flush() error
	Close() error
}
