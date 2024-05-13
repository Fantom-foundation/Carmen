// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package stock

import (
	"slices"
	"testing"
)

func TestEncodeIndex(t *testing.T) {
	t.Run("TestEncodeIndex_1", func(t *testing.T) {
		testEncodeIndex(t, byte(0xFF), make([]byte, 1), []byte{0xFF})
	})
	t.Run("TestEncodeIndex_2", func(t *testing.T) {
		testEncodeIndex(t, uint16(0xFFAA), make([]byte, 2), []byte{0xFF, 0xAA})
	})
	t.Run("TestEncodeIndex_4", func(t *testing.T) {
		testEncodeIndex(t, uint32(0xFFAABBCC), make([]byte, 4), []byte{0xFF, 0xAA, 0xBB, 0xCC})
	})
	t.Run("TestEncodeIndex_8", func(t *testing.T) {
		testEncodeIndex(t, uint64(0xFFAABBCCDDEEFFAA), make([]byte, 8), []byte{0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA})
	})
}

func testEncodeIndex[I Index](t *testing.T, val I, got, want []byte) {
	t.Helper()
	EncodeIndex(val, got)
	if !slices.Equal(got, want) {
		t.Errorf("values do not match: %v != %v", got, want)
	}
}

func TestDecodeIndex(t *testing.T) {
	t.Run("TestDecodeIndex_1", func(t *testing.T) {
		testDecodeIndex(t, byte(0xFF), []byte{0xFF})
	})
	t.Run("TestDecodeIndex_2", func(t *testing.T) {
		testDecodeIndex(t, uint16(0xFFAA), []byte{0xFF, 0xAA})
	})
	t.Run("TestDecodeIndex_4", func(t *testing.T) {
		testDecodeIndex(t, uint32(0xFFAABBCC), []byte{0xFF, 0xAA, 0xBB, 0xCC})
	})
	t.Run("TestDecodeIndex_8", func(t *testing.T) {
		testDecodeIndex(t, uint64(0xFFAABBCCDDEEFFAA), []byte{0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA})
	})
}

func testDecodeIndex[I Index](t *testing.T, want I, val []byte) {
	t.Helper()
	if got, want := DecodeIndex[I](val), want; got != want {
		t.Errorf("values do not match: %v != %v", got, want)
	}
}
