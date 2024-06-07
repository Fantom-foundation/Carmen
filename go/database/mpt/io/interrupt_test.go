// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"errors"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

func TestExport_CanBeInterrupted(t *testing.T) {
	type testFuncs struct {
		// export is the tested export func
		export func(string, io.Writer) error
		// createDB is an init of the database
		createDB func(t *testing.T, sourceDir string)
		// check that the interrupted did not corrupt the db by re-opening it
		check func(t *testing.T, sourceDir string)
	}

	tests := map[string]testFuncs{
		"live": {
			export:   Export,
			createDB: createTestLive,
			check:    checkCanOpenLiveDB,
		},
		"archive": {
			export:   ExportArchive,
			createDB: createTestArchive,
			check:    checkCanOpenArchive,
		},
	}

	for name, tf := range tests {
		t.Run(name, func(t *testing.T) {
			// Create a small db to be exported.
			sourceDir := t.TempDir()
			tf.createDB(t, sourceDir)

			writer := &mockWriter{signalInterrupt: false}
			// first find number of writes
			if err := tf.export(sourceDir, writer); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// save max count and reset number of writes
			maxCount := writer.numOfWrites
			// reset number of writes, so we can compare
			// that export was indeed interrupted
			writer.numOfWrites = 0
			writer.signalInterrupt = true

			err := tf.export(sourceDir, writer)
			if err == nil {
				t.Fatal("export was interrupted, error must not be nil")
			}

			got := err.Error()
			want := ErrCanceled.Error()
			if !strings.Contains(got, want) {
				t.Errorf("unexpected error: got: %v, want: %v", got, want)
			}

			if maxCount == writer.numOfWrites {
				t.Error("export was not interrupted")
			}

			// lastly check that the database is not corrupted
			tf.check(t, sourceDir)
		})
	}
}

func createTestLive(t *testing.T, sourceDir string) {
	t.Helper()
	db := createExampleLiveDB(t, sourceDir)
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close example live db: %v", err)
	}
}

func createTestArchive(t *testing.T, sourceDir string) {
	t.Helper()
	source, err := mpt.OpenArchiveTrie(sourceDir, mpt.S5ArchiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	fillTestBlocksIntoArchive(t, source)
	if err = source.Close(); err != nil {
		t.Fatalf("failed to close test DB: %v", err)
	}
}

// checkCanOpenLiveDB makes sure LiveDB is not corrupted and can be opened (and closed)
func checkCanOpenLiveDB(t *testing.T, sourceDir string) {
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5LiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close db: %v", err)
	}
}

// checkCanOpenLiveDB makes sure Archive is not corrupted and can be opened (and closed)
func checkCanOpenArchive(t *testing.T, sourceDir string) {
	archive, err := mpt.OpenArchiveTrie(sourceDir, mpt.S5ArchiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		t.Fatalf("failed to open archive: %v", err)
	}
	err = archive.Close()
	if err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
}

type mockWriter struct {
	numOfWrites     int
	signalInterrupt bool
}

func (m *mockWriter) Write([]byte) (n int, err error) {
	m.numOfWrites++
	// inform the test that first write has happened
	if m.numOfWrites > 0 && m.signalInterrupt {
		m.signalInterrupt = false
		err = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		if err != nil {
			return 0, errors.New("failed to create a SIGINT signal")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return 0, nil
}
