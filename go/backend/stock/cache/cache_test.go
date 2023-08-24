package cache

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
)

func TestCachedMemoryStock(t *testing.T) {
	t.Parallel()
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "cache-memory",
		Open:               openCachedMemoryStock,
	})
}

func TestCachedFileStock(t *testing.T) {
	t.Parallel()
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "cache-file",
		Open:               openCachedFileStock,
	})
}

func openCachedMemoryStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	stock, err := memory.OpenStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}
	return CreateCachedStockWithCapacity(stock, 10_000), nil
}

func openCachedFileStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	stock, err := file.OpenStock[int, int](stock.IntEncoder{}, directory)
	if err != nil {
		return nil, err
	}
	return CreateCachedStockWithCapacity(stock, 10_000), nil
}
