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
