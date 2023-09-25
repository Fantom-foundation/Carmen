package file

import (
	"encoding/json"
	"fmt"
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
	return OpenStock[int, int](stock.IntEncoder{}, directory)
}

func TestFile_MemoryReporting(t *testing.T) {
	genStock, err := openFileStock(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to open empty stock: %v", err)
	}
	stock, ok := genStock.(*fileStock[int, int])
	if !ok {
		t.Fatalf("factory produced value of wrong type")
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
