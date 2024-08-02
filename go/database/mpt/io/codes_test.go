// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"bytes"
	"io"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestCodes_OutputIsSortedByHash(t *testing.T) {

	codes := map[common.Hash][]byte{}

	codes[common.Hash{1}] = []byte{1, 2, 3}
	codes[common.Hash{2}] = []byte{4, 5}
	codes[common.Hash{3}] = []byte{6, 7, 8}

	buffer := new(bytes.Buffer)
	if err := writeCodes(codes, buffer); err != nil {
		t.Fatalf("failed to write codes: %v", err)
	}

	in := bytes.NewBuffer(buffer.Bytes())

	expectations := [][]byte{
		{1, 2, 3},
		{4, 5},
		{6, 7, 8},
	}

	for _, expectation := range expectations {
		token, err := in.ReadByte()
		if err != nil {
			t.Fatalf("failed to read code token: %v", err)
		}
		if want, got := byte('C'), token; want != got {
			t.Errorf("unexpected token, wanted %c, got %c", want, got)
		}
		code, err := readCode(in)
		if err != nil {
			t.Fatalf("failed to read code: %v", err)
		}
		if !bytes.Equal(code, expectation) {
			t.Errorf("unexpected code, wanted %x, got %x", expectation, code)
		}
	}

	_, err := in.ReadByte()
	if err != io.EOF {
		t.Errorf("expected end of encoded data, but %v", err)
	}
}
