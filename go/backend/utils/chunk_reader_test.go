// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package utils

import (
	"errors"
	"io"
	"slices"
	"testing"
)

func TestChunkReader_ReadChunks(t *testing.T) {
	const size = 10_000
	want := make([]byte, size)
	for i := 0; i < size; i++ {
		want[i] = byte(i)
	}

	reader := NewChunkReader(want, size/3)

	got := make([]byte, 0, size)
	buffer := make([]byte, size)

	var err error
	for !errors.Is(err, io.EOF) {
		var n int
		n, err = reader.Read(buffer)
		got = append(got, buffer[0:n]...)
	}

	if !slices.Equal(got, want) {
		t.Errorf("incorrect byte stream read: %v != %v", got, want)
	}
}
