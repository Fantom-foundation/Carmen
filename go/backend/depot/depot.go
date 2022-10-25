package depot

import "github.com/Fantom-foundation/Carmen/go/common"

// Depot is a mutable key/value store for values of variable length.
// It provides mutation/lookup support, as well as global state hashing support
// to obtain a quick hash for the entire content.
//
// The type I is the type used for the ordinal numbers.
type Depot[I common.Identifier] interface {
	// Set creates a new mapping from the index to the value
	Set(id I, value []byte) error

	// Get a value associated with the index (or nil if not defined)
	Get(id I) ([]byte, error)

	// GetStateHash computes and returns a cryptographical hash of the stored data
	GetStateHash() (common.Hash, error)

	// Close the depot
	Close() error
}
