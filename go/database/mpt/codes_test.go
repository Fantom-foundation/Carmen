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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common"
	"go.uber.org/mock/gomock"
)

func TestCodes_OpenCodes(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	if want, got := 0, len(codes.codes); want != got {
		t.Fatalf("expected codes to be empty, got %d", got)
	}
}

func TestCodes_OpenCodes_IOErrorsAreHandled(t *testing.T) {
	tests := map[string]func(t *testing.T) string{
		"invalid directory": func(t *testing.T) string {
			dir := t.TempDir()
			file := filepath.Join(dir, "file")
			if err := os.WriteFile(file, []byte{}, 0600); err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
			return file //< passing a file instead of a directory
		},
		"missing directory permissions": func(t *testing.T) string {
			dir := t.TempDir()
			stat, err := os.Stat(dir)
			if err != nil {
				t.Fatalf("failed to stat directory: %v", err)
			}
			if err := os.Chmod(dir, 0500); err != nil {
				t.Fatalf("failed to change directory permissions: %v", err)
			}
			t.Cleanup(func() {
				os.Chmod(dir, stat.Mode())
			})
			return dir
		},
		"missing permissions to create code file": func(t *testing.T) string {
			dir := t.TempDir()
			// the code directory must exist to reach the code file creation
			if err := os.MkdirAll(filepath.Join(dir, "codes"), 0700); err != nil {
				t.Fatalf("failed to create codes directory: %v", err)
			}
			stat, err := os.Stat(dir)
			if err != nil {
				t.Fatalf("failed to stat directory: %v", err)
			}
			if err := os.Chmod(dir, 0500); err != nil {
				t.Fatalf("failed to change directory permissions: %v", err)
			}
			t.Cleanup(func() {
				os.Chmod(dir, stat.Mode())
			})
			return dir
		},
		"missing permissions to read code file": func(t *testing.T) string {
			dir := t.TempDir()
			file := filepath.Join(dir, "codes.dat")
			if err := os.WriteFile(file, []byte{}, 0600); err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
			if err := os.Chmod(file, 0200); err != nil {
				t.Fatalf("failed to change file permissions: %v", err)
			}
			t.Cleanup(func() {
				os.Chmod(file, 0600)
			})
			return dir
		},
		"missing permissions to read checkpoint data": func(t *testing.T) string {
			dir := t.TempDir()
			nested := filepath.Join(dir, "codes")
			if err := os.MkdirAll(nested, 0700); err != nil {
				t.Fatalf("failed to create codes directory: %v", err)
			}
			file := filepath.Join(nested, "committed.json")
			if err := os.WriteFile(file, []byte{}, 0600); err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
			if err := os.Chmod(file, 0200); err != nil {
				t.Fatalf("failed to change file permissions: %v", err)
			}
			t.Cleanup(func() {
				os.Chmod(file, 0600)
			})
			return dir
		},
	}

	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			dir := prepare(t)
			_, err := openCodes(dir)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestCodes_CodesCanBeAddedAndRetrieved(t *testing.T) {
	codes, err := openCodes(t.TempDir())
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

func TestCodes_Flush_EmptyCodesCanBeFlushed(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	if err := codes.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	stats, err := os.Stat(codes.file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if want, got := int64(0), stats.Size(); want != got {
		t.Fatalf("expected file size to be %d, got %d", want, got)
	}
}

func TestCodes_Flush_CodesAreWrittenIncrementally(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	code1 := []byte("code1")
	code2 := []byte("code2")
	code3 := []byte("code3")

	codes.add(code1)
	codes.add(code2)

	if want, got := 2, len(codes.pending); want != got {
		t.Fatalf("expected %d pending codes, got %d", want, got)
	}

	if err := codes.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	if want, got := 0, len(codes.pending); want != got {
		t.Fatalf("expected %d pending codes, got %d", want, got)
	}

	snapshot1, err := os.ReadFile(codes.file)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	if codes.fileSize != uint64(len(snapshot1)) {
		t.Fatalf("expected file size to be %d, got %d", len(snapshot1), codes.fileSize)
	}

	// The next step is incremental.
	codes.add(code3)

	if want, got := 1, len(codes.pending); want != got {
		t.Fatalf("expected %d pending codes, got %d", want, got)
	}

	if err := codes.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	if want, got := 0, len(codes.pending); want != got {
		t.Fatalf("expected %d pending codes, got %d", want, got)
	}

	snapshot2, err := os.ReadFile(codes.file)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	if codes.fileSize != uint64(len(snapshot2)) {
		t.Fatalf("expected file size to be %d, got %d", len(snapshot2), codes.fileSize)
	}

	if !bytes.HasPrefix(snapshot2, snapshot1) {
		t.Fatalf("expected snapshot2 to be a continuation of snapshot1")
	}
}

func TestCodes_getCodes_ReturnsAllCodes(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	code1 := []byte("code1")
	code2 := []byte("code2")

	hash1 := codes.add(code1)
	hash2 := codes.add(code2)

	got := codes.getCodes()

	if want, got := 2, len(got); want != got {
		t.Fatalf("expected %d codes, got %d", want, got)
	}

	if want, got := code1, got[hash1]; !bytes.Equal(want, got) {
		t.Fatalf("expected %x, got %x", want, got)
	}

	if want, got := code2, got[hash2]; !bytes.Equal(want, got) {
		t.Fatalf("expected %x, got %x", want, got)
	}
}

func TestCodes_GetMemoryFootprint_ReturnsProperSize(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	code1 := []byte("short")
	code2 := []byte("something longer")

	codes.add(code1)
	codes.add(code2)

	footprint := codes.GetMemoryFootprint()
	want := unsafe.Sizeof(*codes) + uintptr(len(code1)+len(code2)+2*32)
	got := footprint.Total()
	if want != got {
		t.Fatalf("expected %d, got %d", want, got)
	}
}

func TestCodes_GuaranteeCheckpoint_PendingCheckpointIsCommitted(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	cp0 := checkpoint.Checkpoint(0)

	if err := codes.GuaranteeCheckpoint(cp0); err != nil {
		t.Fatalf("failed to guarantee initial checkpoint: %v", err)
	}

	cp1 := checkpoint.Checkpoint(1)
	if err := codes.Prepare(cp1); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if want, got := cp0, codes.checkpoint; want != got {
		t.Fatalf("expected checkpoint to be %d, got %d", want, got)
	}

	if err := codes.GuaranteeCheckpoint(cp1); err != nil {
		t.Fatalf("failed to guarantee pending checkpoint: %v", err)
	}

	if want, got := cp1, codes.checkpoint; want != got {
		t.Fatalf("expected checkpoint to be %d, got %d", want, got)
	}

	if err := codes.GuaranteeCheckpoint(cp0); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_GuaranteeCheckpoint_IoErrorsAreHandled(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}
	cp1 := checkpoint.Checkpoint(1)
	if err := codes.Prepare(cp1); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	pendingFile := filepath.Join(codes.directory, "prepare.json")
	if err := os.WriteFile(pendingFile, []byte("invalid json"), 0600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	if err := codes.GuaranteeCheckpoint(cp1); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_Prepare_CheckpointIsIncremental(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	cp1 := checkpoint.Checkpoint(1)
	if err := codes.Prepare(cp1); err != nil {
		t.Fatalf("failed to prepare initial checkpoint: %v", err)
	}

	cp2 := checkpoint.Checkpoint(2)
	if err := codes.Prepare(cp2); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_Prepare_FailsIfFlushFails(t *testing.T) {
	codes, err := openCodes(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	codes.add([]byte("code1"))

	os.Chmod(codes.file, 0400) // make the file read-only
	defer os.Chmod(codes.file, 0600)

	cp1 := checkpoint.Checkpoint(1)
	if err := codes.Prepare(cp1); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_Commit_HandlesIoIssues(t *testing.T) {
	tests := map[string]func(*testing.T, string) error{
		"missing prepare file": func(t *testing.T, dir string) error {
			return os.Remove(filepath.Join(dir, "codes", "prepare.json"))
		},
		"invalid prepare file": func(t *testing.T, dir string) error {
			return os.WriteFile(filepath.Join(dir, "codes", "prepare.json"), []byte("invalid json"), 0600)
		},
		"missing rename permissions": func(t *testing.T, dir string) error {
			subDir := filepath.Join(dir, "codes")
			if err := os.Chmod(subDir, 0500); err != nil {
				return err
			}
			t.Cleanup(func() {
				os.Chmod(subDir, 0700)
			})
			return nil
		},
	}

	for name, temper := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			codes, err := openCodes(dir)
			if err != nil {
				t.Fatalf("failed to open codes: %v", err)
			}

			codes.add([]byte("code1"))

			cp1 := checkpoint.Checkpoint(1)
			if err := codes.Prepare(cp1); err != nil {
				t.Fatalf("failed to prepare test: %v", err)
			}

			if err := temper(t, dir); err != nil {
				t.Fatalf("failed to prepare test: %v", err)
			}

			if err := codes.Commit(cp1); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestCodes_Restore_InvalidCheckpointMetaDataIsDetected(t *testing.T) {
	dir := t.TempDir()
	restorer := getCodeRestorer(dir)

	subDir := filepath.Join(dir, "codes")
	if err := os.MkdirAll(subDir, 0700); err != nil {
		t.Fatalf("failed to create codes directory: %v", err)
	}

	if err := os.WriteFile(filepath.Join(subDir, "committed.json"), []byte("invalid json"), 0600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	cp := checkpoint.Checkpoint(0)
	if err := restorer.Restore(cp); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_Restore_InvalidCheckpointDataIsDetected(t *testing.T) {
	dir := t.TempDir()
	restorer := getCodeRestorer(dir)

	cp := checkpoint.Checkpoint(42) // < non-existing checkpoint
	if err := restorer.Restore(cp); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_Restore_CanHandleErrorCorruptedData(t *testing.T) {
	tests := map[string]func(dir string) error{
		"no corruption": func(string) error {
			return nil
		},
		"extra data in code file": func(dir string) error {
			file, _ := getCodePaths(dir)
			data, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			data = append(data, []byte("extra")...)
			return os.WriteFile(file, data, 0600)
		},
	}

	for name, temper := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			// Prepare a valid code state.
			codes, err := openCodes(dir)
			if err != nil {
				t.Fatalf("failed to open codes: %v", err)
			}

			codes.add([]byte("code1"))
			codes.add([]byte("code2"))

			cp := checkpoint.Checkpoint(1)
			if err := codes.Prepare(cp); err != nil {
				t.Fatalf("failed to prepare checkpoint: %v", err)
			}
			if err := codes.Commit(cp); err != nil {
				t.Fatalf("failed to commit checkpoint: %v", err)
			}

			backup, err := os.ReadFile(codes.file)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}
			if len(backup) == 0 {
				t.Fatalf("expected file to be non-empty")
			}

			// Corrupt the code state.
			if err := temper(dir); err != nil {
				t.Fatalf("failed to corrupt codes: %v", err)
			}

			// Attempt to restore the code state.
			restorer := getCodeRestorer(dir)
			if err := restorer.Restore(cp); err != nil {
				t.Fatalf("failed to restore checkpoint: %v", err)
			}

			// Verify the restored state.
			restored, err := os.ReadFile(codes.file)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}

			if !bytes.Equal(backup, restored) {
				t.Fatalf("expected file to be equal after restore")
			}
		})
	}
}

func TestCodes_CheckpointsCanBeRestored(t *testing.T) {
	dir := t.TempDir()
	file, _ := getCodePaths(dir)
	codes, err := openCodes(dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	codes.add([]byte("code1"))
	codes.add([]byte("code2"))

	checkpoint := checkpoint.Checkpoint(1)
	if err := codes.Prepare(checkpoint); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := codes.Commit(checkpoint); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}

	backup, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	codes.add([]byte("code3"))
	if want, got := 3, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

	if err := codes.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	modified, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	if modified.Size() <= backup.Size() {
		t.Fatalf("expected file to be larger after flush")
	}

	if err := getCodeRestorer(dir).Restore(checkpoint); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}

	restored, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	if restored.Size() != backup.Size() {
		t.Fatalf("expected file to be same size after restore")
	}

	codes, err = openCodes(dir)
	if err != nil {
		t.Fatalf("failed to re-open recovered codes: %v", err)
	}

	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}
}

func TestCodes_CheckpointsCanBeAborted(t *testing.T) {
	dir := t.TempDir()
	codes, err := openCodes(dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	codes.add([]byte("code1"))
	codes.add([]byte("code2"))

	cp := checkpoint.Checkpoint(1)
	if err := codes.Prepare(cp); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := codes.Abort(cp); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}

	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

	cp = checkpoint.Checkpoint(0)
	if err := getCodeRestorer(dir).Restore(cp); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}

	codes, err = openCodes(dir)
	if err != nil {
		t.Fatalf("failed to re-open recovered codes: %v", err)
	}

	if want, got := 0, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}
}

func TestCodes_CanBeHandledByCheckpointCoordinator(t *testing.T) {
	dir := t.TempDir()
	codes, err := openCodes(dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	coordinator, err := checkpoint.NewCoordinator(t.TempDir(), codes)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	codes.add([]byte("code1"))

	if _, err := coordinator.CreateCheckpoint(); err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}

	codes.add([]byte("code2"))

	if err := getCodeRestorer(dir).Restore(coordinator.GetCurrentCheckpoint()); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}

	codes, err = openCodes(dir)
	if err != nil {
		t.Fatalf("failed to re-open recovered codes: %v", err)
	}

	if want, got := 1, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

}

func TestCodes_writeCodes_WritesCodesToFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")

	codes := map[common.Hash][]byte{
		{1}: {5},
		{2}: {7, 8},
	}

	if err := writeCodes(codes, file); err != nil {
		t.Fatalf("failed to write codes: %v", err)
	}

	readCodes, _, err := readCodesAndSize(file)
	if err != nil {
		t.Fatalf("failed to read codes: %v", err)
	}

	if want, got := 2, len(readCodes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}
}

func TestCodes_writeCodes_WriteFailures(t *testing.T) {
	codes := make(map[common.Hash][]byte, 1)
	var h common.Hash
	code := make([]byte, 5)
	h[0] = byte(1)
	code[0] = byte(5)
	codes[h] = code

	// execute dry-run to compute the number of calls to io.Writer
	var count int
	{
		ctrl := gomock.NewController(t)
		osfile := utils.NewMockOsFile(ctrl)

		osfile.EXPECT().Write(gomock.Any()).AnyTimes().DoAndReturn(func(data []byte) (int, error) {
			count++
			return len(data), nil
		})
		if err := writeCodesTo(codes, osfile); err != nil {
			t.Fatalf("cannot execute writeCodesTo: %s", err)
		}
	}

	var injectedErr = errors.New("write error")
	ctrl := gomock.NewController(t)
	osfile := utils.NewMockOsFile(ctrl)

	// execute the computed number of loops and mock calls to io.Writer so that
	// the last one is failing.
	// This way all branches are exercised.
	for i := 0; i < count; i++ {
		t.Run(fmt.Sprintf("io_error_%d", i), func(t *testing.T) {
			calls := make([]*gomock.Call, 0, i+1)
			for j := 0; j < i; j++ {
				calls = append(calls, osfile.EXPECT().Write(gomock.Any()).Return(0, nil))
			}
			calls = append(calls, osfile.EXPECT().Write(gomock.Any()).Return(0, injectedErr))
			gomock.InOrder(calls...)

			if err := writeCodesTo(codes, osfile); !errors.Is(err, injectedErr) {
				t.Errorf("writing roots should fail")
			}
		})

	}
}

func TestCodes_writeCodes_CannotCreateTheOutputFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes")
	if err := os.Mkdir(file, os.FileMode(0644)); err != nil {
		t.Fatalf("cannot create dir: %s", err)
	}
	if err := writeCodes(make(map[common.Hash][]byte, 1), file); err == nil {
		t.Errorf("writing roots should fail")
	}
}

func TestCodes_writeCodesTo_ForwardWriteErrors(t *testing.T) {
	ctrl := gomock.NewController(t)

	codes := map[common.Hash][]byte{
		{1}: {5},
		{2}: {7, 8},
	}

	// count number of writing steps
	counter := 0
	file := utils.NewMockOsFile(ctrl)
	file.EXPECT().Write(gomock.Any()).AnyTimes().DoAndReturn(func(data []byte) (int, error) {
		counter++
		return len(data), nil
	})

	if err := writeCodesTo(codes, file); err != nil {
		t.Fatalf("cannot execute writeCodesTo: %s", err)
	}
	if counter == 0 {
		t.Fatalf("expected at least one write operation")
	}

	for i := 0; i < counter; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			file := utils.NewMockOsFile(ctrl)
			injectedError := errors.New("injected error")
			gomock.InOrder(
				file.EXPECT().Write(gomock.Any()).Times(i).DoAndReturn(func(data []byte) (int, error) {
					return len(data), nil
				}),
				file.EXPECT().Write(gomock.Any()).Return(0, injectedError),
			)
			err := writeCodesTo(codes, file)
			if !errors.Is(err, injectedError) {
				t.Fatalf("expected error, got %v", err)
			}
		})
	}
}

func TestCodes_readCodesAndSize_ReadingNonExistingFileReturnsEmptyCodeMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codes.dat")
	codes, size, err := readCodesAndSize(path)
	if err != nil {
		t.Fatalf("failed to read codes: %v", err)
	}
	if want, got := 0, len(codes); want != got {
		t.Fatalf("expected codes to be empty, got %d", got)
	}
	if want, got := uint64(0), size; want != got {
		t.Fatalf("expected code file-size to be 0, got %d", got)
	}
}

func TestCodes_readCodesAndSize_ReadingIssuesAreReported(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codes.dat")

	if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatalf("failed to prepare invalid code file: %v", err)
	}

	_, _, err := readCodesAndSize(path)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_readCodesAndSize_PermissionErrorsAreDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codes.dat")

	if err := os.Chmod(dir, 0000); err != nil {
		t.Fatalf("failed to change directory permissions: %v", err)
	}
	defer os.Chmod(dir, 0700)

	_, _, err := readCodesAndSize(path)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCodes_readCodes_Cannot_Read(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "dir")
	if err := os.Mkdir(file, os.FileMode(0)); err != nil {
		t.Fatalf("cannot create dir: %s", err)
	}
	if _, err := readCodes(file); err == nil {
		t.Errorf("reading codes should fail")
	}
}

func TestCodes_parseCodes_ReadFailures(t *testing.T) {
	var injectedErr = errors.New("read error")
	ctrl := gomock.NewController(t)
	osfile := utils.NewMockOsFile(ctrl)

	var h common.Hash
	sizes := []int{len(h), 4, 100}
	// execute three times - parseCode calls io.Reader three times to get [<key>, <length>, <code>]
	for i := 0; i < 3; i++ {
		calls := make([]*gomock.Call, 0, i+1)
		for j := 0; j < i; j++ {
			pos := j
			call := osfile.EXPECT().Read(gomock.Any()).DoAndReturn(func(buf []byte) (int, error) {
				buf[0] = 1             // fill in an non-zero value not to return an empty array
				return sizes[pos], nil // returning expected size causes this io.Reader is called exactly once
			})
			calls = append(calls, call)
		}
		calls = append(calls, osfile.EXPECT().Read(gomock.Any()).Return(1, injectedErr))
		gomock.InOrder(calls...)

		if _, err := parseCodes(osfile); !errors.Is(err, injectedErr) {
			t.Errorf("reading codes should fail")
		}

	}
}
