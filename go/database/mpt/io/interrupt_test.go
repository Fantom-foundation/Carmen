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
	"io"
	"strings"
	"sync"
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
			check:    checkLive,
		},
		"archive": {
			export:   ExportArchive,
			createDB: createTestArchive,
			check:    checkArchive,
		},
	}

	for name, tf := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Create a small db to be exported.
			sourceDir := t.TempDir()
			tf.createDB(t, sourceDir)

			writerWg := new(sync.WaitGroup)
			writer := &mockWriter{wg: writerWg, signalWrite: false, waitTime: 0}
			writerWg.Add(1)

			// first find number of writes
			if err := tf.export(sourceDir, &mockWriter{wg: writerWg}); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// save max count and reset number of writes
			maxCount := writer.numOfWrites
			writer.signalWrite = true
			writer.waitTime = 100 * time.Millisecond
			hasEnded := new(sync.WaitGroup)
			go func() {
				hasEnded.Add(1)
				defer hasEnded.Done()
				if err := ExportArchive(sourceDir, writer); err != nil {
					got := err.Error()
					want := errCanceled.Error()
					if !strings.Contains(got, want) {
						t.Errorf("unexpected error: got: %v, want: %v", got, want)
					}
					return
				}
				t.Error("export was interrupt hence should raise an error")
			}()

			// wait for first write
			writerWg.Wait()
			err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			if err != nil {
				t.Error("failed to create a SIGINT signal")
				return
			}

			// wait for the export to finish
			hasEnded.Wait()
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
	_ = fillTestBlocksIntoArchive(t, source)
	if err = source.Close(); err != nil {
		t.Fatalf("failed to close test DB: %v", err)
	}
}

func checkLive(t *testing.T, sourceDir string) {
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5ArchiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close db: %v", err)
	}
}

func checkArchive(t *testing.T, sourceDir string) {
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
	numOfWrites int
	signalWrite bool
	waitTime    time.Duration
	wg          *sync.WaitGroup
}

func (m *mockWriter) Write(_ []byte) (n int, err error) {
	m.numOfWrites++
	// inform the test that first write has happened
	if m.signalWrite {
		m.signalWrite = false
		m.wg.Done()
	}
	// slow down writing to be able to interrupt the export
	time.Sleep(m.waitTime)
	return 0, nil
}
