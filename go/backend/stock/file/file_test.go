// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package file

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"go.uber.org/mock/gomock"
)

func TestFileStock(t *testing.T) {
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "file",
		Open:               openFileStock,
	})
}

func openFileStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	return OpenStock[int, int](stock.IntEncoder{}, directory)
}

func openInitFileStock(directory string, items int) (*fileStock[int, int], error) {
	s, err := openStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}

	for i := 0; i < items; i++ {
		id, err := s.New()
		if err != nil {
			return nil, err
		}
		if err := s.Set(id, i); err != nil {
			return nil, err
		}
	}

	for i := 0; i < items/2; i++ {
		if err := s.Delete(i); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func TestFile_MemoryReporting(t *testing.T) {
	stock, err := openStock[int, int](stock.IntEncoder{}, t.TempDir())
	if err != nil {
		t.Fatalf("failed to open empty stock: %v", err)
	}
	size := stock.GetMemoryFootprint()
	if size == nil {
		t.Errorf("invalid memory footprint reported: %v", size)
	}

	// adding elements is not affecting the size
	if _, err := stock.New(); err != nil {
		t.Errorf("failed to add new element")
	}

	newSize := stock.GetMemoryFootprint()
	if newSize == nil {
		t.Errorf("invalid memory footprint reported: %v", newSize)
	}
	if size.Total() != newSize.Total() {
		t.Errorf("size of file based stock was affected by new element")
	}
}

func TestFile_Open_CannotMkdir(t *testing.T) {
	directory := "/root/dir"
	if _, err := openStock[int, int](stock.IntEncoder{}, directory); err == nil {
		t.Errorf("creating directory should fail")
	}
}

func TestFile_Open_MissingFile(t *testing.T) {
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("cannot close stock: %s", err)
	}

	// delete file
	if err := os.Remove(filepath.Join(directory, fileNameValues)); err != nil {
		t.Fatalf("cannot delete file: %s", err)
	}

	if _, err := openStock[int, int](stock.IntEncoder{}, directory); err == nil {
		t.Errorf("opening stock should fail")
	}
}

func TestFile_Open_CorruptedValueFile(t *testing.T) {
	testOpenCorruptedFiles(t, fileNameValues)
}

func TestFile_Open_CorruptedFreelistFile(t *testing.T) {
	testOpenCorruptedFiles(t, fileNameFreeList)
}

func TestFile_Open_CorruptedCheckpointFile(t *testing.T) {
	testOpenCorruptedFiles(t, fileNameCommittedCheckpoint)
}

func testOpenCorruptedFiles(t *testing.T, filename string) {
	t.Helper()
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	err = errors.Join(
		s.Prepare(checkpoint.Checkpoint(1)),
		s.Commit(checkpoint.Checkpoint(1)),
	)
	if err != nil {
		t.Fatalf("cannot checkpoint stock: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("cannot close stock: %s", err)
	}

	// corrupt the file by adding an unrelated string
	file, err := os.OpenFile(fmt.Sprintf("%s/%s", directory, filename), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("cannot open file: %s", err)
	}
	if _, err = file.WriteString("Hello, World!"); err != nil {
		t.Fatalf("cannot write to file: %s", err)
	}

	if er := file.Close(); er != nil {
		t.Fatalf("cannot close file: %s", err)
	}

	emptyVerifier := func(encoder stock.ValueEncoder[int], directory string) (meta metadata, err error) {
		return meta, nil
	}
	if _, err := openVerifyStock[int, int](stock.IntEncoder{}, directory, emptyVerifier); err == nil {
		t.Errorf("opening stock should fail")
	}
}

func TestFile_VerifyStock_FailReadMeta(t *testing.T) {
	testFileVerifyStockMissingFile(t, fileNameMetadata)
}

func TestFile_VerifyStock_FailReadValues(t *testing.T) {
	testFileVerifyStockMissingFile(t, fileNameValues)
}

func TestFile_VerifyStock_FailReadFreelist(t *testing.T) {
	testFileVerifyStockMissingFile(t, fileNameFreeList)
}

func testFileVerifyStockMissingFile(t *testing.T, filename string) {
	t.Helper()
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("cannot close stock: %s", err)
	}

	meta, values, freelist := getFileNames(directory)
	// cause missing file error
	if err := os.Remove(fmt.Sprintf("%s/%s", directory, filename)); err != nil {
		t.Fatalf("cannot delete file: %s", err)
	}

	if _, err := verifyStockFilesInternal[int, int](stock.IntEncoder{}, meta, values, freelist); err == nil {
		t.Errorf("stock validation should fail")
	}
}

func TestFile_VerifyStock_FailGetFreeListStats(t *testing.T) {
	ctrl := gomock.NewController(t)

	err := fmt.Errorf("expected error")
	freelist := utils.NewMockOsFile(ctrl)
	freelist.EXPECT().Stat().Return(nil, err)

	var meta metadata
	if err := verifyStackInternal[int](meta, freelist); err == nil {
		t.Errorf("verifycation should fail")
	}
}

func TestFile_VerifyStock_FailInitFreeList(t *testing.T) {
	ctrl := gomock.NewController(t)

	err := fmt.Errorf("expected error")
	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	freelist := utils.NewMockOsFile(ctrl)
	freelist.EXPECT().Stat().Return(info, nil).AnyTimes()
	freelist.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), err) // causes init error

	var meta metadata
	meta.FreeListLength = 1
	if err := verifyStackInternal[int](meta, freelist); err == nil {
		t.Errorf("verifycation should fail")
	}
}

func TestFile_VerifyStock_FailReadFreeList(t *testing.T) {
	ctrl := gomock.NewController(t)

	err := fmt.Errorf("expected error")
	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	freelist := utils.NewMockOsFile(ctrl)
	freelist.EXPECT().Stat().Return(info, nil).AnyTimes()
	freelist.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	freelist.EXPECT().Read(gomock.Any()).Return(8, nil).AnyTimes()
	freelist.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, err) // causes reading error (flush called before read)
	freelist.EXPECT().Close()

	var meta metadata
	meta.FreeListLength = 1
	if err := verifyStackInternal[int](meta, freelist); err == nil {
		t.Errorf("verifycation should fail")
	}
}

func TestFile_NewId_FailReadFile(t *testing.T) {
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	if err := os.Truncate(filepath.Join(directory, fileNameValues), 0); err != nil {
		t.Fatalf("failed to delete file %v", err)
	}
	s.freelist.buffer = make([]int, 0, 1) // force to try to load from the file

	if _, err := s.New(); err == nil {
		t.Errorf("getting new ID should fail")
	}
}

func TestFile_New_DoesNotReuseIdsCoveredByCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}

	id, err := s.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}

	if err := s.Delete(id); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}

	// before the checkpoint, the ID would be reused.
	id2, err := s.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}
	if id2 != id {
		t.Fatalf("expected ID to be reused, got %v", id2)
	}
	if err := s.Delete(id2); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}

	// after the checkpoint, the ID should not be reused.
	checkpoint := checkpoint.Checkpoint(1)
	err = errors.Join(
		s.Prepare(checkpoint),
		s.Commit(checkpoint),
	)
	if err != nil {
		t.Fatalf("failed to checkpoint stock: %v", err)
	}

	id3, err := s.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}
	if id3 == id || id3 == id2 {
		t.Fatalf("expected new ID, got reused ID %v", id3)
	}
}

func TestFile_Delete_FailsIfIdCoveredByCheckpointIsDeleted(t *testing.T) {
	dir := t.TempDir()
	s, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}

	id, err := s.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}

	checkpoint := checkpoint.Checkpoint(1)
	err = errors.Join(
		s.Prepare(checkpoint),
		s.Commit(checkpoint),
	)
	if err != nil {
		t.Fatalf("failed to checkpoint stock: %v", err)
	}

	if err := s.Delete(id); err == nil {
		t.Fatalf("expected delete to fail")
	}
}

func TestFile_Get_FailReadFile(t *testing.T) {
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	// inject failing file
	ctrl := gomock.NewController(t)
	values := utils.NewMockSeekableFile(ctrl)
	values.EXPECT().WriteAt(gomock.Any(), gomock.Any()).Return(0, fmt.Errorf("expected error"))
	s.values = values

	if err := s.Set(1, 100); err == nil {
		t.Errorf("setting value should fail")
	}
}

func TestFile_Set_FailReadFile(t *testing.T) {
	directory := t.TempDir()
	// init stock above the buffer size to force reading from file during the test
	const bufferSize = 1 << 12
	s, err := openInitFileStock(directory, bufferSize)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	if err := os.Truncate(filepath.Join(directory, fileNameValues), 0); err != nil {
		t.Fatalf("failed to delete file %v", err)
	}

	if _, err := s.Get(5); err == nil {
		t.Errorf("getting value should fail")
	}
}

func TestFile_GetIds_FailReadFile(t *testing.T) {
	directory := t.TempDir()
	s, err := openStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	ctrl := gomock.NewController(t)

	info := utils.NewMockFileInfo(ctrl)
	info.EXPECT().Size().Return(int64(8)).AnyTimes()

	freelist := utils.NewMockOsFile(ctrl)
	freelist.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("expected error")) // causes init error

	// inject failing freelist
	s.freelist.file = freelist

	if _, err := s.GetIds(); err == nil {
		t.Errorf("getting IDs should fail")
	}
}

func TestFile_ANonExistingDirectoryCanNotBeVerification(t *testing.T) {
	if err := VerifyStock[int, int]("/some/directory/that/does/not/exist", nil); err == nil {
		t.Errorf("verification should have failed")
	}
}

func TestFile_AnEmptyDirectoryPassesTheVerification(t *testing.T) {
	dir := t.TempDir()
	if err := VerifyStock[int, int](dir, stock.IntEncoder{}); err != nil {
		t.Errorf("unexpected error encountered on empty directory: %v", err)
	}
}

func TestFile_AFreshStockPassesVerification(t *testing.T) {
	dir := t.TempDir()
	encoder := stock.IntEncoder{}
	stock, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}
	id, err := stock.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}
	if err := stock.Delete(id); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	if err := VerifyStock[int, int](dir, encoder); err != nil {
		t.Fatalf("detected verification error in unmodified file stock: %v", err)
	}
}

func TestFile_DetectsMissingFiles(t *testing.T) {
	for _, file := range []string{fileNameMetadata, fileNameValues, fileNameFreeList} {
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			encoder := stock.IntEncoder{}
			stock, err := openFileStock(t, dir)
			if err != nil {
				t.Fatalf("failed to create new stock: %v", err)
			}
			if err := stock.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}
			if err := os.Remove(dir + "/" + file); err != nil {
				t.Fatalf("failed to delete file %v: %v", file, err)
			}
			if err := VerifyStock[int, int](dir, encoder); err == nil {
				t.Errorf("failed to detect missing stock file")
			}
		})
	}
}

func TestFile_DetectsCorruptedMetaFile(t *testing.T) {
	dir := t.TempDir()
	metafile, _, _ := getFileNames(dir)
	encoder := stock.IntEncoder{}
	stock, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	data, err := os.ReadFile(metafile)
	if err != nil || len(data) == 0 {
		t.Fatalf("failed to read metadata data: %v", err)
	}
	data[0]++
	if err := os.WriteFile(metafile, data, 0600); err != nil {
		t.Fatalf("failed to write modified metadata: %v", err)
	}

	if err := VerifyStock[int, int](dir, encoder); err == nil {
		t.Errorf("failed to detect corrupted content of stock")
	}
}

func TestFile_DetectsInvalidMetaDataContent(t *testing.T) {
	tests := []struct {
		meta  metadata
		issue string
	}{
		{metadata{Version: dataFormatVersion + 1, IndexTypeSize: 8, ValueTypeSize: 4, FreeListLength: 0, NumValuesInFile: 0}, "invalid file format version"},
		{metadata{Version: dataFormatVersion, IndexTypeSize: 4, ValueTypeSize: 4, FreeListLength: 0, NumValuesInFile: 0}, "invalid index type encoding"},
		{metadata{Version: dataFormatVersion, IndexTypeSize: 8, ValueTypeSize: 2, FreeListLength: 0, NumValuesInFile: 0}, "invalid value type encoding"},
		{metadata{Version: dataFormatVersion, IndexTypeSize: 8, ValueTypeSize: 4, FreeListLength: 12, NumValuesInFile: 0}, "invalid free-list file size"},
		{metadata{Version: dataFormatVersion, IndexTypeSize: 8, ValueTypeSize: 4, FreeListLength: 0, NumValuesInFile: 1200}, "insufficient value file size"},
	}

	dir := t.TempDir()
	metafile, _, _ := getFileNames(dir)
	encoder := stock.IntEncoder{}
	stock, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	for _, test := range tests {
		data, _ := json.Marshal(test.meta)
		if err := os.WriteFile(metafile, data, 0600); err != nil {
			t.Fatalf("failed to write modified metadata: %v", err)
		}

		err := VerifyStock[int, int](dir, encoder)
		if err == nil {
			t.Fatalf("failed to detect corrupted content of stock")
		}

		found := fmt.Sprintf("%v", err)
		if !strings.Contains(found, test.issue) {
			t.Fatalf("failed to detect expected issue, wanted %s, got %v", test.issue, found)
		}
	}
}

func TestFile_DetectsCorruptedFreeList(t *testing.T) {
	dir := t.TempDir()
	_, _, freelist := getFileNames(dir)
	encoder := stock.IntEncoder{}
	stock, err := openFileStock(t, dir)
	if err != nil {
		t.Fatalf("failed to create new stock: %v", err)
	}
	id, err := stock.New()
	if err != nil {
		t.Fatalf("failed to get new value: %v", err)
	}
	if err := stock.Delete(id); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	data, err := os.ReadFile(freelist)
	if err != nil || len(data) == 0 {
		t.Fatalf("failed to read freelist data: %v", err)
	}
	data[0]++
	if err := os.WriteFile(freelist, data, 0600); err != nil {
		t.Fatalf("failed to write modified freelist: %v", err)
	}

	if err := VerifyStock[int, int](dir, encoder); err == nil {
		t.Errorf("failed to detect corrupted content of stock")
	}
}

func TestFile_SetValue_FailingEncoder(t *testing.T) {
	ctrl := gomock.NewController(t)

	encoder := stock.NewMockValueEncoder[int](ctrl)
	encoder.EXPECT().GetEncodedSize().Return(1).AnyTimes()
	encoder.EXPECT().Store(gomock.Any(), gomock.Any()).Return(fmt.Errorf("expected error"))

	s, err := openStock[int, int](encoder, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create stock: %v", err)
	}
	defer s.Close()
	id, err := s.New()
	if err != nil {
		t.Fatalf("cannot generate ID: %s", err)
	}

	if err := s.Set(id, 100); err == nil {
		t.Errorf("failing encoder should fail this call")
	}
}

func TestFile_GetValue_FailingEncoder(t *testing.T) {
	ctrl := gomock.NewController(t)

	encoder := stock.NewMockValueEncoder[int](ctrl)
	encoder.EXPECT().GetEncodedSize().Return(1).AnyTimes()
	encoder.EXPECT().Store(gomock.Any(), gomock.Any()).DoAndReturn(func(data []byte, val *int) error {
		data[0] = 0xA
		return nil
	})
	encoder.EXPECT().Load(gomock.Any(), gomock.Any()).Return(fmt.Errorf("expected error"))

	s, err := openStock[int, int](encoder, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create stock: %v", err)
	}
	defer s.Close()

	id, err := s.New()
	if err != nil {
		t.Fatalf("cannot generate ID: %s", err)
	}

	if err := s.Set(id, 100); err != nil {
		t.Fatalf("cannot set ID: %s", err)
	}

	if _, err := s.Get(id); err == nil {
		t.Errorf("failing encoder should fail this call")
	}
}

func TestFile_Flush_CannotWriteMetadata(t *testing.T) {
	dir := t.TempDir()
	stock, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	// mimic non-writable directory by redirecting the configured directory.
	stock.directory = "/root/dir"

	if err := stock.Flush(); err == nil {
		t.Errorf("flush should fail")
	}
}

func TestStock_GuaranteeCheckpoint_FailsOnCorruptedPendingFile(t *testing.T) {
	dir := t.TempDir()
	s, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		t.Fatalf("failed to create stock: %v", err)
	}

	file := getPendingCheckpointFile(dir)
	if err := os.WriteFile(file, []byte("corrupted"), 0600); err != nil {
		t.Fatalf("failed to write corrupted checkpoint file: %v", err)
	}

	if err := s.GuaranteeCheckpoint(checkpoint.Checkpoint(1)); err == nil {
		t.Errorf("guaranteeing checkpoint should fail")
	}
}

func TestStock_Prepare_FailsOnFailedFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		t.Fatalf("failed to create stock: %v", err)
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("failed to flush stock: %v", err)
	}

	meta, _, _ := getFileNames(dir)
	if err := os.Chmod(meta, 0x400); err != nil {
		t.Fatalf("failed to make file read-only: %v", err)
	}

	if err := s.Flush(); err == nil {
		t.Errorf("flush should fail")
	}

	if s.Prepare(checkpoint.Checkpoint(1)) == nil {
		t.Errorf("prepare should fail due to failed flush")
	}
}

func TestStock_Commit_FailsOnIoIssues(t *testing.T) {
	tests := map[string]func(t *testing.T, dir string) error{
		"missing pending file": func(_ *testing.T, dir string) error {
			return os.Remove(getPendingCheckpointFile(dir))
		},
		"corrupted pending file": func(_ *testing.T, dir string) error {
			return utils.WriteJsonFile(getPendingCheckpointFile(dir), checkpointMetaData{})
		},
		"missing rename permissions": func(t *testing.T, dir string) error {
			info, err := os.Stat(dir)
			if err != nil {
				return err
			}
			t.Cleanup(func() {
				// undo permission change at end of test
				_ = os.Chmod(dir, info.Mode())
			})
			return os.Chmod(dir, 0500)
		},
	}

	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := openStock[int, int](stock.IntEncoder{}, dir)
			if err != nil {
				t.Fatalf("failed to create stock: %v", err)
			}

			if err := s.Prepare(checkpoint.Checkpoint(1)); err != nil {
				t.Fatalf("prepare should succeed, got %v", err)
			}

			modify(t, dir)

			if s.Commit(checkpoint.Checkpoint(1)) == nil {
				t.Errorf("commit should fail")
			}
		})
	}
}

func TestStock_Abort_FailsOnIoIssues(t *testing.T) {
	tests := map[string]func(t *testing.T, dir string) error{
		"corrupted committed checkpoint file": func(t *testing.T, dir string) error {
			file := getCommittedCheckpointFile(dir)
			return os.WriteFile(file, []byte("corrupted"), 0600)
		},
		"missing file delete permissions": func(t *testing.T, dir string) error {
			info, err := os.Stat(dir)
			if err != nil {
				return err
			}
			t.Cleanup(func() {
				// undo permission change at end of test
				_ = os.Chmod(dir, info.Mode())
			})
			return os.Chmod(dir, 0500)
		},
	}

	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := openStock[int, int](stock.IntEncoder{}, dir)
			if err != nil {
				t.Fatalf("failed to create stock: %v", err)
			}

			if err := s.Prepare(checkpoint.Checkpoint(1)); err != nil {
				t.Fatalf("prepare should succeed, got %v", err)
			}

			modify(t, dir)

			if s.Abort(checkpoint.Checkpoint(1)) == nil {
				t.Errorf("abort should fail")
			}
		})
	}
}

func TestRestore_CanRestoreCommittedAndPendingCheckpoint(t *testing.T) {
	for _, name := range []string{"committed", "pending"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			s, err := openStock[int, int](stock.IntEncoder{}, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}
			id, err := s.New()
			if err != nil {
				t.Fatalf("failed to create new item: %v", err)
			}
			if err := s.Set(id, 1); err != nil {
				t.Fatalf("failed to set value: %v", err)
			}

			cp1 := checkpoint.Checkpoint(1)
			if err := s.Prepare(cp1); err != nil {
				t.Fatalf("failed to prepare checkpoint: %v", err)
			}
			if name == "committed" {
				if err := s.Commit(cp1); err != nil {
					t.Fatalf("failed to commit checkpoint: %v", err)
				}
			}

			id, err = s.New()
			if err != nil {
				t.Fatalf("failed to create new item: %v", err)
			}
			if err := s.Set(id, 2); err != nil {
				t.Fatalf("failed to set value: %v", err)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}

			// restore the stock
			if err := GetRestorer(dir).Restore(cp1); err != nil {
				t.Fatalf("failed to restore checkpoint: %v", err)
			}

			s, err = openStock[int, int](stock.IntEncoder{}, dir)
			if err != nil {
				t.Fatalf("failed to re-open recovered stock: %v", err)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("failed to recovered stock: %v", err)
			}
		})
	}
}

func TestRestore_CorruptedStockCanBeRestored(t *testing.T) {
	tests := map[string]struct {
		corrupt                    func(dir string) error
		canBeIgnoredByVerification bool
	}{
		"delete metadata": {
			corrupt: func(dir string) error {
				meta, _, _ := getFileNames(dir)
				return os.Remove(meta)
			},
		},

		"truncate metadata": {
			corrupt: func(dir string) error {
				meta, _, _ := getFileNames(dir)
				return os.Truncate(meta, 20)
			},
		},
		"extra data in metadata": {
			corrupt: func(dir string) error {
				meta, _, _ := getFileNames(dir)
				data, err := os.ReadFile(meta)
				if err != nil {
					return err
				}
				return os.WriteFile(meta, append(data, []byte("extra data")...), 0600)
			},
		},
		"non-parsable metadata": {
			corrupt: func(dir string) error {
				meta, _, _ := getFileNames(dir)
				data, err := os.ReadFile(meta)
				if err != nil {
					return err
				}
				data[0] = data[0] + 1
				return os.WriteFile(meta, data, 0600)
			},
		},
		"extra data in value file": {
			corrupt: func(dir string) error {
				_, value, _ := getFileNames(dir)
				data, err := os.ReadFile(value)
				if err != nil {
					return err
				}
				return os.WriteFile(value, append(data, []byte("extra data")...), 0600)
			},
			// extra data is technically not a problem since it gets overwritten
			canBeIgnoredByVerification: true,
		},
		"extra data in freelist file": {
			corrupt: func(dir string) error {
				_, _, freelist := getFileNames(dir)
				data, err := os.ReadFile(freelist)
				if err != nil {
					return err
				}
				return os.WriteFile(freelist, append(data, []byte("extra data")...), 0600)
			},
		},
		"pending checkpoint": {
			corrupt: func(dir string) error {
				// A pending checkpoint is not a huge issue, but should be removed
				// by the restoration as it indicates a failed checkpoint creation.
				pendingFile := getPendingCheckpointFile(dir)
				return utils.WriteJsonFile(pendingFile, checkpointMetaData{})
			},
			canBeIgnoredByVerification: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			const N = 10_000
			encoder := stock.IntEncoder{}
			dir := t.TempDir()
			stock, err := openStock[int, int](encoder, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}

			// Fill the stock with some data (more than a single buffer size).
			for i := 0; i < N; i++ {
				id, err := stock.New()
				if err != nil {
					t.Fatalf("failed to create item in stock: %v", err)
				}
				if err := stock.Set(id, i); err != nil {
					t.Fatalf("failed to set value in stock: %v", err)
				}
			}

			// Free some elements.
			for i := 0; i < N; i += 100 {
				if err := stock.Delete(i); err != nil {
					t.Fatalf("failed to delete item in stock: %v", err)
				}
			}

			// Create a checkpoint from which the stock should be recovered.
			checkpoint := checkpoint.Checkpoint(1)
			err = errors.Join(
				stock.Prepare(checkpoint),
				stock.Commit(checkpoint),
				stock.Close(),
			)
			if err != nil {
				t.Fatalf("failed to checkpoint and close stock: %v", err)
			}

			backup := t.TempDir()
			if err := copyDirectory(dir, backup); err != nil {
				t.Fatalf("failed to backup stock: %v", err)
			}

			// add some issue to the original data
			if err := test.corrupt(dir); err != nil {
				t.Fatalf("failed to corrupt stock: %v", err)
			}

			if !test.canBeIgnoredByVerification {
				if err := VerifyStock[int, int](dir, encoder); err == nil {
					t.Fatalf("failed to detect corrupted stock")
				}
			}

			// run the restoration
			if err := GetRestorer(dir).Restore(checkpoint); err != nil {
				t.Fatalf("failed to restore stock: %v", err)
			}

			// check that the restoration managed to restore all data
			if err := checkForEqualContent(t, dir, backup); err != nil {
				t.Fatalf("failed to check recovered directory: %v", err)
			}

			// verify recovered content
			if err := VerifyStock[int, int](dir, encoder); err != nil {
				t.Fatalf("failed to verify restored stock: %v", err)
			}

			// check that restored stock can be opened
			stock, err = openStock[int, int](encoder, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}
			if err := stock.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}
		})
	}
}

func TestStock_Restore_FailsOnIoIssues(t *testing.T) {
	tests := map[string]func(t *testing.T, dir string) error{
		"missing committed checkpoint": func(_ *testing.T, dir string) error {
			return os.Remove(getCommittedCheckpointFile(dir))
		},
		"corrupted committed checkpoint": func(_ *testing.T, dir string) error {
			return os.WriteFile(getCommittedCheckpointFile(dir), []byte("corrupted"), 0600)
		},
		"wrong committed checkpoint": func(_ *testing.T, dir string) error {
			path := getCommittedCheckpointFile(dir)
			meta, err := utils.ReadJsonFile[checkpointMetaData](path)
			if err != nil {
				return err
			}
			meta.Checkpoint++
			return utils.WriteJsonFile(path, meta)
		},
		"missing meta file permissions": func(_ *testing.T, dir string) error {
			meta, _, _ := getFileNames(dir)
			return os.Chmod(meta, 0400)
		},
		"missing value file permissions": func(_ *testing.T, dir string) error {
			_, values, _ := getFileNames(dir)
			return os.Chmod(values, 0400)
		},
		"missing freelist permissions": func(_ *testing.T, dir string) error {
			_, _, path := getFileNames(dir)
			return os.Chmod(path, 0400)
		},
		"missing pending checkpoint permissions": func(t *testing.T, dir string) error {
			path := getPendingCheckpointFile(dir)
			if err := utils.WriteJsonFile(path, checkpointMetaData{}); err != nil {
				return err
			}
			info, err := os.Stat(dir)
			if err != nil {
				return err
			}
			t.Cleanup(func() {
				// undo permission change at end of test
				_ = os.Chmod(dir, info.Mode())
			})
			return os.Chmod(dir, 0500)
		},
	}

	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			stock, err := openStock[int, int](stock.IntEncoder{}, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}

			// Create a checkpoint from which the stock should be recovered.
			checkpoint := checkpoint.Checkpoint(1)
			err = errors.Join(
				stock.Prepare(checkpoint),
				stock.Commit(checkpoint),
				stock.Close(),
			)
			if err != nil {
				t.Fatalf("failed to checkpoint and close stock: %v", err)
			}

			modify(t, dir)

			if err := GetRestorer(dir).Restore(checkpoint); err == nil {
				t.Errorf("restoration should fail")
			}
		})
	}
}

func copyDirectory(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, rel)
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		dstFile, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, srcFile)
		return err
	})
}

// checkForEqualContent verifies that the content of two directories is equal.
func checkForEqualContent(t *testing.T, dir1, dir2 string) error {
	t.Helper()
	return errors.Join(
		containsSubsetOfFiles(t, dir1, dir2),
		containsSubsetOfFiles(t, dir2, dir1),
	)
}

func containsSubsetOfFiles(t *testing.T, super, sub string) error {
	return filepath.Walk(sub, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(sub, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(super, rel)

		if !exists(dstPath) {
			t.Fatalf("file %s is missing in %s", rel, super)
		}

		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read file %s: %v", path, err)
		}
		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("failed to read file %s: %v", dstPath, err)
		}
		if a, b := len(want), len(got); a != b {
			t.Fatalf("file %s has different size from %s: %d != %d", path, dstPath, a, b)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("file %s differs from %s", path, dstPath)
		}
		return nil
	})
}

func TestWriteJson_FailsOnMarshalingError(t *testing.T) {
	dir := t.TempDir()
	nonSerializable := make(chan struct{})
	if err := utils.WriteJsonFile(dir, nonSerializable); err == nil {
		t.Errorf("writing non-serializable data to JSON")
	}
}

func FuzzFileStock_RandomOps(f *testing.F) {
	open := func(directory string) (stock.Stock[int, int], error) {
		return openStock[int, int](stock.IntEncoder{}, directory)
	}

	stock.FuzzStockRandomOps(f, open, true)
}

func BenchmarkFileStock_Get(b *testing.B) {
	dir := b.TempDir()
	stock, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		b.Fatalf("failed to open stock")
	}
	defer stock.Close()

	id, err := stock.New()
	if err != nil {
		b.Fatalf("failed to create item in stock")
	}
	if err := stock.Set(id, 12); err != nil {
		b.Fatalf("failed to set value in stock")
	}

	for i := 0; i < b.N; i++ {
		if _, err := stock.Get(id); err != nil {
			b.Fatalf("failed to get value: %v", err)
		}
	}
}

func BenchmarkFileStock_Set(b *testing.B) {
	dir := b.TempDir()
	stock, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		b.Fatalf("failed to open stock")
	}
	defer stock.Close()

	id, err := stock.New()
	if err != nil {
		b.Fatalf("failed to create item in stock")
	}
	for i := 0; i < b.N; i++ {
		if err := stock.Set(id, 12); err != nil {
			b.Fatalf("failed to set value: %v", err)
		}
	}
}

func BenchmarkFileStock_Commit(b *testing.B) {
	dir := b.TempDir()
	stock, err := openStock[int, int](stock.IntEncoder{}, dir)
	if err != nil {
		b.Fatalf("failed to open stock")
	}
	for i := 0; i < 10; i++ {
		id, err := stock.New()
		if err != nil {
			b.Fatalf("failed to create item in stock")
		}
		if err := stock.Set(id, i); err != nil {
			b.Fatalf("failed to set value in stock")
		}
	}
	defer stock.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stock.Prepare(checkpoint.Checkpoint(i + 1)); err != nil {
			b.Fatalf("failed to prepare commit: %v", err)
		}
		if err := stock.Commit(checkpoint.Checkpoint(i + 1)); err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}
