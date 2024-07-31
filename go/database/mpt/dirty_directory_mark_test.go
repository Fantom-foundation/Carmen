// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirtyDirectoryMark_InitiallyDirectoriesAreClean(t *testing.T) {
	dir := t.TempDir()
	dirty, err := isDirty(dir)
	if err != nil {
		t.Fatalf("error checking for dirty directory: %v", err)
	}
	if dirty {
		t.Errorf("test directory should be considered clean")
	}
}

func TestDirtyDirectoryMark_NonExistingDirectoryResultsInAnError(t *testing.T) {
	_, err := isDirty("<something that does not exist>")
	if err == nil {
		t.Fatalf("operation should have produced an error")
	}
}

func TestDirtyDirectoryMark_PassingAFileResultsInAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test")
	if err := os.WriteFile(path, []byte{}, 0600); err != nil {
		t.Fatalf("failed to create test input file: %v", err)
	}
	_, err := isDirty(path)
	if err == nil {
		t.Fatalf("operation should have produced an error")
	}
}

func TestDirtyDirectoryMark_DirectoryCanBeMarkedDirtyAndCleanedAgain(t *testing.T) {
	dir := t.TempDir()
	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("unexpected initial state: %t, %v", dirty, err)
	}
	if err := markDirty(dir); err != nil {
		t.Fatalf("failed to mark directory as dirty: %v", err)
	}
	if dirty, err := isDirty(dir); !dirty || err != nil {
		t.Fatalf("unexpected state of dirty directory: %t, %v", dirty, err)
	}
	if err := markClean(dir); err != nil {
		t.Fatalf("failed to mark directory as clean: %v", err)
	}
	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("unexpected state of cleaned directory: %t, %v", dirty, err)
	}
}

func TestDirtyDirectoryMark_DirtyFlagMustBeAFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, dirtyFileName), 0700); err != nil {
		t.Fatalf("failed to create a directory with the dirty flag: %v", err)
	}
	if dirty, err := isDirty(dir); dirty || err != nil {
		t.Fatalf("a directory with the dirty-file name should not be considered a valid dirty mark")
	}
}
