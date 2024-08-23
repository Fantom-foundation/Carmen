// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package immutable

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBytes_EqualWhenContainingSameContent(t *testing.T) {
	b1 := NewBytes([]byte{1, 2, 3})
	b2 := NewBytes([]byte{1, 2, 3})
	b3 := NewBytes([]byte{3, 2, 1})

	if &b1 == &b2 {
		t.Fatalf("instances are not distinct, got %v and %v", &b1, &b2)
	}

	if b1 != b2 {
		t.Errorf("instances are not equal, got %v and %v", b1, b2)
	}

	if b1 == b3 {
		t.Errorf("instances are equal, got %v and %v", b1, b3)
	}
}

func TestBytes_AssignmentProducesEqualValue(t *testing.T) {
	b1 := NewBytes([]byte{1, 2, 3})
	b2 := b1

	if b1 != b2 {
		t.Errorf("assigned value is not equal: %v vs %v", b1, b2)
	}
}

func TestBytes_CanBeConverted_Back_And_Forth(t *testing.T) {
	original := []byte{1, 2, 3}
	b := NewBytes(original)

	if got, want := b.ToBytes(), original; !bytes.Equal(got, want) {
		t.Errorf("conversion failed, got %v, want %v", got, want)
	}
}

func TestBytes_String(t *testing.T) {
	original := []byte{1, 2, 3}
	b := NewBytes(original)

	if got, want := fmt.Sprintf("%s", b), "0x010203"; got != want {
		t.Errorf("string failed, got %v, want %v", got, want)
	}
}
