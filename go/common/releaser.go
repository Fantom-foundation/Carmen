// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

// Releaser is an interface for types owning resources that should be released
// after use to facilitate resource re-utilization.
type Releaser interface {
	// Release releases bound resources for re-use. The object this function is
	// called on becomes invalid for any future operation afterwards.
	Release()
}
