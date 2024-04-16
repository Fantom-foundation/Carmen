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
