package mpt

import (
	"fmt"
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
