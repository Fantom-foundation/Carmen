package multimap

import "github.com/Fantom-foundation/Carmen/go/common"

// MultiMap defines the interface for mapping keys to sets of multiple values.
type MultiMap[K common.Identifier, V common.Identifier] interface {
	// Add adds the given key/value pair.
	Add(key K, value V) error

	// Remove removes a single key/value entry.
	Remove(key K, value V) error

	// RemoveAll removes all entries with the given key.
	RemoveAll(key K) error

	// GetAll provides all values associated with the given key.
	GetAll(key K) ([]V, error)

	// provides the size of the store in memory in bytes
	common.MemoryFootprintProvider

	// needs to be flush and closable
	common.FlushAndCloser
}
