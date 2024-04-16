//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package index

import (
	"errors"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Index is an append-only index for a set of values, mapping each added
// new element to a unique ordinal number.
//
// The type parameter K, the key type, can be any type that can
// be hashed and compared. The type I is the type used for the
// ordinal numbers.
type Index[K comparable, I common.Identifier] interface {
	// Get the number of elements in this index, which corresponds to the
	// identifier that will be assigned to the next key to be registered.
	Size() I

	// GetOrAdd returns an index mapping for the key, or creates the new index
	GetOrAdd(key K) (I, error)

	// Get returns an index mapping for the key, returns ErrNotFound if not exists
	Get(key K) (I, error)

	// Contains returns whether the key exists in the mapping or not.
	Contains(key K) bool

	// GetStateHash returns the index hash.
	GetStateHash() (common.Hash, error)

	// provides the size of the index in memory in bytes
	common.MemoryFootprintProvider

	// Also, indexes need to be flush and closable.
	common.FlushAndCloser

	// Snapshotable indexes.
	backend.Snapshotable
}

var (
	ErrNotFound = errors.New("index: key not found")
)
