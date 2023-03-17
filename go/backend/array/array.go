package array

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Array is a structure that allows to store and fetch a value to/from ordinal indexes.
type Array[I common.Identifier, V any] interface {
	// Set creates a new mapping from the index to the value
	Set(id I, value V) error

	// Get a value associated with the index (or a default value if not defined)
	Get(id I) (V, error)

	// provides the size of the store in memory in bytes
	common.MemoryFootprintProvider

	// Also, stores need to be flush and closable.
	common.FlushAndCloser
}
