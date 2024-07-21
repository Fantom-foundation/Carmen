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

func TestCodes_CodesCanBeAddedAndRetrieved(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	code1 := []byte("code1")
	code2 := []byte("code2")

	hash1 := codes.add(code1)
	hash2 := codes.add(code2)

	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have 2 entries, got %d", got)
	}

	if want, got := code1, codes.getCodeForHash(hash1); string(want) != string(got) {
		t.Fatalf("expected code1, got %s", got)
	}

	if want, got := code2, codes.getCodeForHash(hash2); string(want) != string(got) {
		t.Fatalf("expected code2, got %s", got)
	}
}
