//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

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
		Open:               openSyncedMemoryStock,
	},
	{
		ImplementationName: "syncedFile",
		Open:               openSyncedFileStock,
	},
}

func TestSyncedStock(t *testing.T) {
	for _, config := range configs {
		stock.RunStockTests(t, config)
	}
}

func openSyncedMemoryStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	nested, err := memory.OpenStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}
	return Sync(nested), nil
}

func openSyncedFileStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
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

func TestSyncedStock_WrapAlreadySynced(t *testing.T) {
	stock, err := openSyncedMemoryStock(t, t.TempDir())
	if err != nil {
		t.Fatalf("cannot create stock: %s", stock)
	}

	wrapped := Sync(stock)

	if wrapped != stock {
		t.Errorf("wrapped stock does not match original one")
	}
}

func FuzzSyncStock_RandomOps(f *testing.F) {
	open := func(directory string) (stock.Stock[int, int], error) {
		nested, err := memory.OpenStock[int, int](stock.IntEncoder{}, directory)
		return Sync(nested), err
	}

	stock.FuzzStockRandomOps(f, open, true)
}
