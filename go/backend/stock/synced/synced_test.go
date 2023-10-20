package synced

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
)

var configs = []stock.NamedStockFactory{
	{
		ImplementationName: "syncedMemory",
		Open:               openMemoryStock,
	},
	{
		ImplementationName: "syncedFile",
		Open:               openFileStock,
	},
}

func TestSyncedStock(t *testing.T) {
	for _, config := range configs {
		stock.RunStockTests(t, config)
	}
}

func openMemoryStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	nested, err := memory.OpenStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}
	return Sync(nested), nil
}

func openFileStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	nested, err := file.OpenStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}
	return Sync(nested), nil
}

func testCanBeAccessedConcurrently(t *testing.T, factory stock.NamedStockFactory) {
	const N = 10
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	var wg sync.WaitGroup
	var errors [N]error
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := stock.New()
			if err != nil {
				errors[i] = fmt.Errorf("unable to create new item: %v", err)
				return
			}
			if err := stock.Set(id, i); err != nil {
				errors[i] = fmt.Errorf("failed to update item: %v", err)
				return
			}
			if value, err := stock.Get(id); err != nil || value != i {
				errors[i] = fmt.Errorf("failed to load item: %v, %d != %d", err, value, i)
				return
			}
			if err := stock.Delete(id); err != nil {
				errors[i] = fmt.Errorf("failed to free item: %v", err)
				return
			}
		}()
	}

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("error in goroutine %d: %v", i, err)
		}
	}
}

func TestSyncedStock_CanBeAccessedConcurrently(t *testing.T) {
	// This test is expected to be run with the --race option to detect
	// race conditions between concurrent accesses.
	for _, config := range configs {
		t.Run(config.ImplementationName, func(t *testing.T) {
			testCanBeAccessedConcurrently(t, config)
		})
	}
}

func FuzzSyncStock_RandomOps(f *testing.F) {
	open := func(directory string) (stock.Stock[int, int], error) {
		nested, err := memory.OpenStock[int, int](stock.IntEncoder{}, directory)
		return Sync(nested), err
	}

	stock.FuzzStock_RandomOps(f, open, true)
}
