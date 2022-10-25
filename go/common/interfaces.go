package common

import "io"

// Flusher is any type that can be flushed.
type Flusher interface {
	Flush() error
}

type FlushAndCloser interface {
	Flusher
	io.Closer
}
