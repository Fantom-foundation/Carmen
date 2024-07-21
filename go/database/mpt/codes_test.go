package mpt

import (
	"path/filepath"
	"testing"
)

func TestCodes_OpenCodes(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	if want, got := 0, len(codes.codes); want != got {
		t.Fatalf("expected codes to be empty, got %d", got)
	}
}
