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

package memory

import (
	"testing"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
)

func TestInMemoryStock(t *testing.T) {
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "memory",
		Open:               openInMemoryStock,
	})
}

func openInMemoryStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	return OpenStock[int, int](stock.IntEncoder{}, directory)
}

func TestInMemoryMemoryReporting(t *testing.T) {
	genStock, err := openInMemoryStock(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to open empty stock")
	}
	stock, ok := genStock.(*inMemoryStock[int, int])
	if !ok {
		t.Fatalf("factory produced value of wrong type")
	}
	want := unsafe.Sizeof(*stock) + uintptr(cap(stock.values)+cap(stock.freeList))*unsafe.Sizeof(int(0))
	if got := stock.GetMemoryFootprint().Total(); got != want {
		t.Errorf("invalid empty size reported - wanted %d, got %d", want, got)
	}
}

func FuzzMemoryStock_RandomOps(f *testing.F) {
	open := func(directory string) (stock.Stock[int, int], error) {
		return OpenStock[int, int](stock.IntEncoder{}, directory)
	}

	stock.FuzzStockRandomOps(f, open, false)
}
