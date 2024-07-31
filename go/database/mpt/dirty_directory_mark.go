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
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const dirtyFileName = "~dirty"

// isDirty checks whether the given directory is marked as dirty. The mark
// is represented by the presence of a file in the respective directory.
// An error is returned if the directory does not exist, the provided
// path does not point to a directory, or another IO error occurred.
func isDirty(directory string) (bool, error) {
	// Check that the directory exists.
	info, err := os.Stat(directory)
	if err != nil {
		return false, err
	}

	if !info.IsDir() {
		return false, fmt.Errorf("%s is not a directory", directory)
	}

	// Check for the dirty flag.
	stat, err := os.Stat(filepath.Join(directory, dirtyFileName))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return !stat.IsDir(), err
}

// markDirty marks the given directory as dirty, and thus, potentially
// corrupted. MPT instances mark their directories as dirty as long as
// they are opened by a process and only clear the mark if the DB got
// successfully closed.
func markDirty(directory string) error {
	return os.WriteFile(filepath.Join(directory, dirtyFileName), []byte{}, 0600)
}

// markClean marks the given directory as clean, and thus, expected
// to be consistent. MPT instances mark their directories as dirty as
// long as they are opened by a process and only clear the mark if the
// DB got successfully closed.
func markClean(directory string) error {
	return os.Remove(filepath.Join(directory, dirtyFileName))
}
