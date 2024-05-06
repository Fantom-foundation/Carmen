// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package file

import (
	"bytes"
	"testing"
)

func TestCopyOffsetsToLengths(t *testing.T) {
	offsets := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1A, 0x1B, 0x1C,
	}
	out := make([]byte, 2*LengthSize)
	copyOffsetsToLengths(out, offsets)
	if !bytes.Equal(out, []byte{0x09, 0x0A, 0x0B, 0x0C, 0x19, 0x1A, 0x1B, 0x1C}) {
		t.Errorf("copyOffsetsToLengths provides unexpected results")
	}
}
