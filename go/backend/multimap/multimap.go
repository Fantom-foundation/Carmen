package multimap

import "github.com/Fantom-foundation/Carmen/go/common"

// MultiMap defines the interface for mapping keys to sets of values.
// It serves as a specialized index structure enabling the fast
// accessing of a set of values associated to a given key.
type MultiMap[K common.Identifier, V common.Identifier] interface {
	// Add adds the given key/value pair.
	Add(key K, value V) error

	// Remove removes a single key/value entry.
	Remove(key K, value V) error

	// RemoveAll removes all entries with the given key.
	RemoveAll(key K) error

	// ForEach applies the given operation on each value associated to the given key.
	ForEach(key K, callback func(V)) error

	// provides the size of the store in memory in bytes
	common.MemoryFootprintProvider

	// needs to be flush and closable
	common.FlushAndCloser
}
