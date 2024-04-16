//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package pagepool

import "github.com/Fantom-foundation/Carmen/go/common"

// Page is an interface for a page that can be converted to/from bytes
// usually to be persisted.
type Page interface {
	common.MemoryFootprintProvider
	// ToBytes converts a Page into raw bytes and fills the input slice
	ToBytes(pageData []byte)

	// FromBytes converts and fills a Page from the input raw bytes
	FromBytes(pageData []byte)

	// Clear empties the page
	Clear()

	// Size is the size of the page in bytes
	Size() int

	// IsDirty should return true if the was modified after it has been last saved
	IsDirty() bool

	// SetDirty sets the dirty flag of this page
	SetDirty(dirty bool)
}
