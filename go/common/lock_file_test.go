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
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLockFile_DefaultLockFileIsInvalid(t *testing.T) {
	lock := lockFile{}
	if lock.Valid() {
		t.Errorf("default lockfile should be invalid")
	}
}

func TestLockFile_CanBeAcquiredAndReleased(t *testing.T) {
	exists := func(path string) bool {
		_, err := os.Stat(path)
		return !errors.Is(err, os.ErrNotExist)
	}

	path := filepath.Join(t.TempDir(), "a")
	if exists(path) {
		t.Errorf("lock file should not exist before acquiring it")
	}
	lock, err := CreateLockFile(path)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	if !lock.Valid() {
		t.Errorf("acquired lock file is not valid")
	}
	if !exists(path) {
		t.Errorf("lock file should exist while acquired")
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}
	if lock.Valid() {
		t.Errorf("released lock file is still valid")
	}
	if exists(path) {
		t.Errorf("lock file should no longer exist after releasing it")
	}
}

func TestLockFile_LockFilesAreExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a")

	lockA, err := CreateLockFile(path)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	_, err = CreateLockFile(path)
	if err == nil {
		t.Errorf("should not be able to acquire an occupied lock")
	}

	if err := lockA.Release(); err != nil {
		t.Errorf("failed to release acquired file lock: %v", err)
	}

	_, err = CreateLockFile(path)
	if err != nil {
		t.Errorf("should be able to acquire a released lock")
	}
}

func TestLockFile_CanOnlyBeReleasedOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a")

	lockA, err := CreateLockFile(path)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if err := lockA.Release(); err != nil {
		t.Errorf("failed to release lock: %v", err)
	}

	if err := lockA.Release(); err == nil {
		t.Errorf("second release should have failed")
	}
}

func TestLockFile_CannotRelease_GivesError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a")

	lock, err := CreateLockFile(path)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("cannot prepare for test: %v", err)
	}
	if err := lock.Release(); err == nil {
		t.Errorf("release should fail")
	}
}

func TestLockFile_CannotRelease_Invalid_fd_GivesError(t *testing.T) {
	lock := lockFile{}
	lock.fileDescriptor = -1

	if err := lock.Release(); err == nil {
		t.Errorf("release should fail")
	}
}

func TestLockFile_GovernsExclusiveAccessForSingleProcess(t *testing.T) {
	const N = 8
	path := filepath.Join(t.TempDir(), "a")
	timesAcquired := atomic.Int32{}
	numOwners := atomic.Int32{}

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lock, err := CreateLockFile(path)
				if err != nil {
					continue
				}
				timesAcquired.Add(1)
				new := numOwners.Add(1)
				if new > 1 {
					t.Errorf("Invalid number of lock owners: %d", new)
				}
				numOwners.Add(-1)
				if err := lock.Release(); err != nil {
					t.Errorf("failed to release lock: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	if timesAcquired.Load() < 1 {
		t.Errorf("lock was never acquired")
	}
}

func TestLockFile_GovernsExclusiveAccessForMultipleProcesses(t *testing.T) {
	// This test runs multiple sub-processes and utilizes a lock file to
	// synchronize access to a shared resource file. Each of the sub-processes
	// tries to acquire the file lock. If successful, the resource file is
	// opened twice to log the ID of the process owning the lock, before
	// the lock is released again. The resulting access history is then
	// checked by this parent process for potential access order violations.
	const N = 8
	dir := t.TempDir()
	path := filepath.Join(dir, "lock")
	resource := filepath.Join(dir, "resource")

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				execFileLockAcquireInSubProcess(t, path, resource)
			}
		}(i)
	}
	wg.Wait()

	accesses, err := getDataFromResourceFile(resource)
	if err != nil {
		t.Fatalf("failed to fetch resource file content: %v", err)
	}

	if len(accesses) == 0 {
		t.Errorf("no accesses logged to resource file")
	}
	if len(accesses)%2 != 0 {
		t.Fatalf("invalid list of accesses logged to resource file")
	}

	// accesses need to be in pairs
	for i := 0; i < len(accesses); i += 2 {
		if accesses[i] != accesses[i+1] {
			t.Fatalf("Failed synchronization of accesses at position %d\n%v", i, accesses)
		}
	}
}

var lockFilePath = flag.String("lock_file_path", "NONE", "the path to the file lock to be acquired")
var resourceFilePath = flag.String("resource_file_path", "", "the path to a shared resource")

func TestFileLock_AcquireLock(t *testing.T) {
	// This test is a helper for the test above. It is processed in the
	// sub-process spawned by the main test coordinator.
	// only run this test if called with a target lock file
	path := *lockFilePath
	if path == "NONE" {
		return
	}

	resource := *resourceFilePath

	lock, err := CreateLockFile(path)
	if err != nil {
		return // somebody else got the lock, which is fine
	}
	pid := os.Getpid()

	// Write the PID twice to the file, risking that somebody else may do the same
	// resulting in an interleaved access pattern.
	if err := addToResourceFile(resource, pid); err != nil {
		t.Fatalf("failed to append pid to resource file: %v", err)
	}
	if err := addToResourceFile(resource, pid); err != nil {
		t.Fatalf("failed to append pid to resource file: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Errorf("failed to release lock: %v", err)
	}
}

func execFileLockAcquireInSubProcess(t *testing.T, lock, resource string) {
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to resolve path to test binary: %v", err)
	}

	cmd := exec.Command(path, "-test.run", "TestFileLock_AcquireLock", "-lock_file_path="+lock, "-resource_file_path="+resource)
	errBuf := new(bytes.Buffer)
	cmd.Stderr = errBuf
	stdBuf := new(bytes.Buffer)
	cmd.Stdout = stdBuf

	if err := cmd.Run(); err != nil {
		t.Errorf("Subprocess finished with error: %v\n stdout:\n%s stderr:\n%s", err, stdBuf.String(), errBuf.String())
	}
}

func addToResourceFile(path string, pid int) (err error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, f.Close())
	}()

	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(pid))
	_, err = f.Write(buffer)
	return err
}

func getDataFromResourceFile(path string) ([]uint64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw)%8 != 0 {
		return nil, fmt.Errorf("invalid resource file format; invalid length %d", len(raw))
	}
	res := make([]uint64, 0, len(raw)/8)
	for len(raw) > 0 {
		res = append(res, binary.BigEndian.Uint64(raw))
		raw = raw[8:]
	}
	return res, nil
}
