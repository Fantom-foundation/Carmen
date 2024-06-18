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

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common/tribool"
)

func TestTriboolUnknown(t *testing.T) {
	tb := TriboolUnknown()
	if !tb.Unknown() {
		t.Errorf("Expected TriboolUnknown to create an unknown Tribool, got %v", tb.String())
	}
}

func TestTriboolFalse(t *testing.T) {
	tb := TriboolFalse()
	if !tb.False() {
		t.Errorf("Expected TriboolFalse to create a false Tribool, got %v", tb.String())
	}
}

func TestTriboolTrue(t *testing.T) {
	tb := TriboolTrue()
	if !tb.True() {
		t.Errorf("Expected TriboolTrue to create a true Tribool, got %v", tb.String())
	}
}

func TestTriboolString(t *testing.T) {
	// Test when Tribool is true
	tbTrue := Tribool{internal: tribool.True()}
	if got, want := tbTrue.String(), "true"; got != want {
		t.Errorf("unexpected value, got: %v, want: %v", got, want)
	}

	// Test when Tribool is false
	tbFalse := Tribool{internal: tribool.False()}
	if got, want := tbFalse.String(), "false"; got != want {
		t.Errorf("unexpected value, got: %v, want: %v", got, want)
	}

	// Test when Tribool is unknown
	tbUnknown := Tribool{internal: tribool.Unknown()}
	if got, want := tbUnknown.String(), "unknown"; got != want {
		t.Errorf("unexpected value, got: %v, want: %v", got, want)
	}
}

func TestNewTribool(t *testing.T) {
	// Test when b is true
	tbTrue := NewTribool(true)
	if !tbTrue.True() {
		t.Errorf("Expected NewTribool(true) to create a true Tribool, got %v", tbTrue.String())
	}

	// Test when b is false
	tbFalse := NewTribool(false)
	if !tbFalse.False() {
		t.Errorf("Expected NewTribool(false) to create a false Tribool, got %v", tbTrue.String())
	}
}
