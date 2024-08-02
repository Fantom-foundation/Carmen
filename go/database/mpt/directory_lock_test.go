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
	"fmt"
	"os"
	"path"
	"testing"
)

func TestDirectoryLock_AcquireAndRelease(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		lock, err := LockDirectory(dir)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		if err := lock.Release(); err != nil {
			t.Fatalf("failed to release lock: %v", err)
		}
	}
}

func TestDirectoryLock_LockIsExclusive(t *testing.T) {
	dir := t.TempDir()

	lockA, err := LockDirectory(dir)
	if err != nil {
		t.Fatalf("failed to lock directory: %v", err)
	}

	lockB, err := LockDirectory(dir)
	if err == nil {
		t.Fatalf("should not be able to acquire a second lock")
	}

	want := fmt.Sprintf("unable to gain exclusive access to %s: failed to acquire file lock: file exists", dir)
	if got := err.Error(); want != got {
		t.Errorf("unexpected error message, wanted '%s', got '%s'", want, got)
	}

	if lockB != nil {
		t.Errorf("unexpected non-nil lock result in error case: %v", lockB)
	}

	lockA.Release()
}

func TestDirectoryLock_CannotLockFile(t *testing.T) {
	dir := t.TempDir()
	file := path.Join(dir, "file.txt")
	if _, err := os.Create(file); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if _, err := LockDirectory(file); err == nil {
		t.Errorf("should not be able to lock a file")
	}

}

func TestDirectoryLock_ForceUnlockDirectory_RemovesLock(t *testing.T) {
	dir := t.TempDir()
	_, err := LockDirectory(dir)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if err := ForceUnlockDirectory(dir); err != nil {
		t.Fatalf("failed to force unlock: %v", err)
	}

	lock, err := LockDirectory(dir)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}
}

func TestDirectoryLock_ForceUnlockDirectory_IgnoresUnlockedDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := ForceUnlockDirectory(dir); err != nil {
		t.Fatalf("failed to force unlock: %v", err)
	}

	lock, err := LockDirectory(dir)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}
}

func TestDirectoryLock_ForceUnlockDirectory_ReportsErrorIfUnlockingFailed(t *testing.T) {
	dir := t.TempDir()

	_, err := LockDirectory(dir)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// remove permission to delete the lock file
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatalf("failed to remove write permission: %v", err)
	}
	defer os.Chmod(dir, 0700)

	if err := ForceUnlockDirectory(dir); err == nil {
		t.Fatalf("expected error when unlocking failed")
	}
}
