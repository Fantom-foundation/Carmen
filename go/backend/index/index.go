package index

import "github.com/Fantom-foundation/Carmen/go/common"

// Index is an append-only index for a set of values, mapping each added
// new element to a unique ordinal number.
//
// The type parameter K, the key type, can be any type that can
// be hashed and compared. The type I is the type used for the
// ordinal numbers.
type Index[K comparable, I common.Identifier] interface {

	// GetOrAdd returns an index mapping for the key, or creates the new index
	GetOrAdd(key K) (I, error)

	// Contains returns a bool flag to test existence of the key in the mapping
	Contains(key K) bool

	// GetStateHash returns the index hash.
	GetStateHash() common.Hash

	// Close closes the storage and clean-ups all possible dirty values
	Close() error
}
