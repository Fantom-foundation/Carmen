// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package evmstore

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestTxPositionSerializer(t *testing.T) {
	var s TxPositionSerializer
	var _ common.Serializer[TxPosition] = s

	// convert back and forth
	position := TxPosition{
		Block:       412345689,
		Event:       common.Hash{0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x33, 0x58, 0x98, 0x77, 0x88, 0x99},
		EventOffset: 258374,
		BlockOffset: 9129874,
	}
	b := s.ToBytes(position)
	position2 := s.FromBytes(b)
	b2 := s.ToBytes(position2)
	b3 := make([]byte, s.Size())
	s.CopyBytes(position2, b3)

	if position != position2 {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b2) {
		t.Errorf("Conversion fails")
	}
	if !bytes.Equal(b, b3) {
		t.Errorf("Conversion fails")
	}
}
