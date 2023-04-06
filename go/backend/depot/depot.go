package depot

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

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

	// GetSize of a value associated with the index (or 0 if not defined)
	GetSize(id I) (int, error)

	// GetStateHash computes and returns a cryptographical hash of the stored data
	GetStateHash() (common.Hash, error)

	// provides the size of the depot in memory in bytes
	common.MemoryFootprintProvider

	// Also, depots need to be flush and closable.
	common.FlushAndCloser

	backend.Snapshotable
}
