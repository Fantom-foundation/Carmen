package file

import (
	"encoding/json"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"go.uber.org/mock/gomock"
	"os"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
)

func TestFileStock(t *testing.T) {
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "file",
		Open:               openFileStock,
	})
}

func openFileStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	return openStock[int, int](stock.IntEncoder{}, directory)
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
	if err := os.Remove(fmt.Sprintf("%s/values.dat", directory)); err != nil {
		t.Fatalf("cannot delete file: %s", err)
	}

	if _, err := openStock[int, int](stock.IntEncoder{}, directory); err == nil {
		t.Errorf("opening stock should fail")
	}
}

func TestFile_Open_CorruptedValueFile(t *testing.T) {
	testOpenCorruptedFiles(t, "values.dat")
}

func TestFile_Open_CorruptedFreelistFile(t *testing.T) {
	testOpenCorruptedFiles(t, "freelist.dat")
}

func testOpenCorruptedFiles(t *testing.T, filename string) {
	t.Helper()
	directory := t.TempDir()
	s, err := openInitFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
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
	testFileVerifyStockMissingFile(t, "meta.json")
}

func TestFile_VerifyStock_FailReadValues(t *testing.T) {
	testFileVerifyStockMissingFile(t, "values.dat")
}

func TestFile_VerifyStock_FailReadFreelist(t *testing.T) {
	testFileVerifyStockMissingFile(t, "freelist.dat")
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

	if err := os.Truncate(fmt.Sprintf("%s/%s", directory, "values.dat"), 0); err != nil {
		t.Fatalf("failed to delete file %v", err)
	}
	s.freelist.buffer = make([]int, 0, 1) // force to try to load from the file

	if _, err := s.New(); err == nil {
		t.Errorf("getting new ID should fail")
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
	values.EXPECT().Write(gomock.Any(), gomock.Any()).Return(fmt.Errorf("expected error"))
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

	if err := os.Truncate(fmt.Sprintf("%s/%s", directory, "values.dat"), 0); err != nil {
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
	for _, file := range []string{"meta.json", "values.dat", "freelist.dat"} {
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

func FuzzFileStock_RandomOps(f *testing.F) {
	open := func(directory string) (stock.Stock[int, int], error) {
		return openStock[int, int](stock.IntEncoder{}, directory)
	}

	stock.FuzzStockRandomOps(f, open, true)
}
