package file

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
)

func TestInMemoryStock(t *testing.T) {
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
		t.Fatalf("failed to open empty stock")
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
