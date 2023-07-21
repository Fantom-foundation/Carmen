package shadow

import (
	"errors"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
)

func TestShadowStock(t *testing.T) {
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "shadow",
		Open:               openShadowStock,
	})
}

func openShadowStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	a, errA := file.OpenStock[int, int](stock.IntEncoder{}, directory+"/A")
	b, errB := memory.OpenStock[int, int](stock.IntEncoder{}, directory+"/B")
	return MakeShadowStock(a, b), errors.Join(errA, errB)
}
