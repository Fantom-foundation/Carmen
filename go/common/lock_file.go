// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"fmt"
	"syscall"
)

// LockFile is an inter-process synchronization primitive facilitating mutual
// exclusion of operations between processes. Internally, the lock creates
// a file in the file system to mark the ownership of a lock and deletes
// this file if the lock is released.
//
// Note: locks that are not released by a process are not automatically
// released at the end of the process.
type LockFile interface {
	// Release releases the exclusive lock ownership provided by a valid
	// instance of this type by deleting the underlying file. Each lock
	// may only be released once. Subsequent calls produce errors.
	Release() error
	// Valid checks whether this lock still owns the underlying resource
	// or whether it has already been released.
	Valid() bool
}

type lockFile struct {
	path           string
	fileDescriptor int
}

// CreateLockFile atomically creates a file with the given path and holds
// a lock on it. The operation fails if a file with the given name already
// exists. The operation is atomic, facilitating inter-process synchronization.
func CreateLockFile(path string) (LockFile, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire file lock: %w", err)
	}
	return &lockFile{path: path, fileDescriptor: fd}, nil
}

func (f *lockFile) Valid() bool {
	return f.fileDescriptor != 0
}

func (f *lockFile) Release() error {
	if f.fileDescriptor == 0 {
		return fmt.Errorf("unable to release invalid lock")
	}
	if err := syscall.Close(f.fileDescriptor); err != nil {
		return fmt.Errorf("failed to release file lock: %w", err)
	}
	if err := syscall.Unlink(f.path); err != nil {
		return fmt.Errorf("failed to release file lock: %w", err)
	}
	f.fileDescriptor = 0
	return nil
}
