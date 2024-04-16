//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

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
