// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import "github.com/Fantom-foundation/Carmen/go/common/tribool"

// Tribool is a wrapper around tribool.Tribool used by the public API.
type Tribool struct {
	internal tribool.Tribool
}

// Unknown returns true if the value is unknown.
func (t Tribool) Unknown() bool {
	return t.internal.Unknown()
}

// True returns true if the value is true.
func (t Tribool) True() bool {
	return t.internal.True()
}

// False returns true if the value is false.
func (t Tribool) False() bool {
	return t.internal.False()
}

// String returns a string representation of the Tribool.
func (t Tribool) String() string {
	return t.internal.String()
}

// NewTribool creates a new Tribool with value depending on b.
func NewTribool(b bool) Tribool {
	if b {
		return TriboolTrue()
	}
	return TriboolFalse()
}

// TriboolUnknown creates a new Tribool with unknown value.
func TriboolUnknown() Tribool {
	return Tribool{}
}

// TriboolFalse creates a new Tribool with false value.
func TriboolFalse() Tribool {
	return Tribool{internal: tribool.False()}
}

// TriboolTrue creates a new Tribool with true value.
func TriboolTrue() Tribool {
	return Tribool{internal: tribool.True()}
}
