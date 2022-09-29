package store

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Store is a mutable key/value store. It provides mutation/lookup support, as well as
// global state hashing support to obtain a quick hash for the entire content.
//
// The type I is the type used for the ordinal numbers,
// the type V for the store values - needs to be serializable.
type Store[I common.Identifier, V any] interface {
	// Set creates a new mapping from the index to the value
	Set(id I, value V) error

	// Get a value associated with the index (or a default value if not defined)
	Get(id I) (V, error)

	// GetStateHash computes and returns a cryptographical hash of the stored data
	GetStateHash() (common.Hash, error)

	// Close the store
	Close() error
}
