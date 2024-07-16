// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package tribool

// Tribool is a three-valued logic type.
// It can be either True, False or Unknown.
// It is useful for representing the result of a comparison
// when the comparison is not possible or could not be determined.
type Tribool struct {
	value byte
}

// New creates a new Tribool with the given value.
func New(value bool) Tribool {
	if value {
		return True()
	}
	return False()
}

// Unknown returns true if the value is unknown.
func (t Tribool) Unknown() bool {
	return t.value == 0 || t.value > 2
}

// True returns true if the value is true.
func (t Tribool) True() bool {
	return t.value == 1
}

// False returns true if the value is false.
func (t Tribool) False() bool {
	return t.value == 2
}

// String returns a string representation of the Tribool.
func (t Tribool) String() string {
	switch t.value {
	case 1:
		return "true"
	case 2:
		return "false"
	default:
		return "unknown"
	}
}

// Unknown creates a new Tribool with unknown value.
func Unknown() Tribool {
	return Tribool{}
}

// False creates a new Tribool with false value.
func False() Tribool {
	return Tribool{value: 2}
}

// True creates a new Tribool with true value.
func True() Tribool {
	return Tribool{value: 1}
}
