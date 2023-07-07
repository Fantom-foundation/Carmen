package stock

import (
	"encoding/binary"
	"testing"
)

type IntEncoder struct{}

func (IntEncoder) GetEncodedSize() int {
	return 4
}

func (IntEncoder) Load(src []byte, value *int) error {
	*value = int(binary.BigEndian.Uint32(src))
	return nil
}

func (IntEncoder) Store(trg []byte, value *int) error {
	binary.BigEndian.PutUint32(trg, uint32(*value))
	return nil
}

type NamedStockFactory struct {
	ImplementationName string
	Open               func(t *testing.T, directory string) (Stock[int, int], error)
}

// RunStockTests runs a set of black-box unit test against a generic Stock
// implementation defined by the given factory. It is intended to be used
// in implementation specific unit test packages to cover basic compliance
// properties as imposed by the Stock interface.
func RunStockTests(t *testing.T, factory NamedStockFactory) {
	wrap := func(test func(*testing.T, NamedStockFactory)) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()
			test(t, factory)
		}
	}
	t.Run("NewCreatesFreshIndexValues", wrap(testNewCreatesFreshIndexValues))
	t.Run("NewCreatesDistinctValues", wrap(testNewCreatesDistinctValues))
	t.Run("LookUpsRetrieveTheSameValue", wrap(testLookUpsRetrieveTheSameValue))
	t.Run("DeletedElementsAreReused", wrap(testDeletedElementsAreReused))
	t.Run("ReusedElementsAreCleared", wrap(testReusedElementsAreCleared))
	t.Run("LargeNumberOfElements", wrap(testLargeNumberOfElements))
	t.Run("ProvidesMemoryFootprint", wrap(testProvidesMemoryFootprint))
	t.Run("CanBeFlushed", wrap(testCanBeFlushed))
	t.Run("CanBeClosed", wrap(testCanBeClosed))
	t.Run("CanBeClosedAndReopened", wrap(testCanBeClosedAndReopened))
}

func testNewCreatesFreshIndexValues(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	index1, _, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}

	index2, _, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	if index1 == index2 {
		t.Errorf("Expected different index values, got %v and %v", index1, index2)
	}
}

func testNewCreatesDistinctValues(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	_, value1, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}

	_, value2, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	if value1 == value2 {
		t.Errorf("Expected different values, got identical values")
	}
}

func testLookUpsRetrieveTheSameValue(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	index1, value1, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	*value1 = 1
	if err := stock.Set(index1, value1); err != nil {
		t.Fatalf("failed to update value for index 1: %v", err)
	}

	index2, value2, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	*value2 = 2
	if err := stock.Set(index2, value2); err != nil {
		t.Fatalf("failed to update value for index 2: %v", err)
	}

	got, err := stock.Get(index1)
	if err != nil || got == nil {
		t.Errorf("failed to obtain value for index %d: got %p, with err %v", index1, got, err)
	}
	if got != nil && *got != 1 {
		t.Errorf("failed to obtain value for index %d: got %d, wanted %d", index1, *got, 1)
	}

	got, err = stock.Get(index2)
	if err != nil || got == nil {
		t.Errorf("failed to obtain value for index %d: got %p, with err %v", index2, got, err)
	}
	if got != nil && *got != 2 {
		t.Errorf("failed to obtain value for index %d: got %d, wanted %d", index2, *got, 2)
	}
}

func testDeletedElementsAreReused(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	seen := map[int]bool{}
	for i := 0; i < 1_000_000; i++ {
		index, _, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new element: %v", err)
		}
		if _, exists := seen[index]; exists {
			return
		}
		seen[index] = true
		if err := stock.Delete(index); err != nil {
			t.Fatalf("failed to delete element with key %v: %v", index, err)
		}
	}
	t.Errorf("stock failed to reuse released index key")
}

func testReusedElementsAreCleared(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	seen := map[int]bool{}
	for i := 0; i < 1_000_000; i++ {
		index, value, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new element: %v", err)
		}
		if *value != 0 {
			t.Fatalf("new element is not zero, got %v", value)
		}
		updated := 52
		if err := stock.Set(index, &updated); err != nil {
			t.Fatalf("failed to udpate value for index %d: %v", index, err)
		}
		if _, exists := seen[index]; exists {
			return
		}
		seen[index] = true
		if err := stock.Delete(index); err != nil {
			t.Fatalf("failed to delete element with key %v: %v", index, err)
		}
	}
	t.Errorf("stock failed to reuse released index key")
}

func testLargeNumberOfElements(t *testing.T, factory NamedStockFactory) {
	const N = 1_000_000
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	indexes := map[int]int{}
	for i := 0; i < N; i++ {
		index, ptr, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new entry: %v", err)
		}
		indexes[i] = index
		*ptr = i
		if err := stock.Set(i, ptr); err != nil {
			t.Fatalf("failed to update value of element with index %d: %v", index, err)
		}
	}

	for i := 0; i < N; i++ {
		ptr, err := stock.Get(indexes[i])
		if err != nil {
			t.Fatalf("failed to locate element: %v", err)
		}
		if ptr == nil {
			t.Fatalf("located index invalid")
		}
		if *ptr != i {
			t.Errorf("invalid value mapped to index %d: wanted %d, got %d", indexes[i], i, *ptr)
		}
	}
}

func testProvidesMemoryFootprint(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if _, _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	footprint := stock.GetMemoryFootprint()
	if footprint == nil {
		t.Fatalf("implementation does not provide memory footprint data")
	}
	if footprint.Total() <= 0 {
		t.Fatalf("implementations claims zero memory footprint")
	}
}

func testCanBeFlushed(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if err := stock.Flush(); err != nil {
		t.Fatalf("failed to flush empty stock: %v", err)
	}
	if _, _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	if err := stock.Flush(); err != nil {
		t.Fatalf("failed to flush non-empty stock: %v", err)
	}
}

func testCanBeClosed(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if _, _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close non-empty stock: %v", err)
	}
}

func testCanBeClosedAndReopened(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	// The first element shall be a deleted element.
	key1, _, err := stock.New()
	if err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}

	// The second element is an element with a value.
	key2, value, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element in stock: %v", err)
	}
	*value = 123
	if err := stock.Set(key2, value); err != nil {
		t.Fatalf("failed to update value: %v", err)
	}

	// The third element is a default-value.
	key3, _, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element in stock: %v", err)
	}

	if err := stock.Delete(key1); err != nil {
		t.Fatalf("failed to delete key from stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close non-empty stock: %v", err)
	}
	stock = nil

	// After re-opening the stock all the information should be present.
	stock, err = factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to reopen stock: %v", err)
	}

	value, err = stock.Get(key2)
	if err != nil {
		t.Fatalf("failed to read value from reopend stock: %v", err)
	}
	if value == nil {
		t.Fatalf("invalid nil value read from reopend stock")
	}
	if *value != 123 {
		t.Fatalf("invalid value read from reopend stock: got %v, wanted 123", *value)
	}

	value, err = stock.Get(key3)
	if err != nil {
		t.Fatalf("failed to read value from reopend stock: %v", err)
	}
	if value == nil {
		t.Fatalf("invalid nil value read from reopend stock")
	}
	if *value != 0 {
		t.Fatalf("invalid value read from reopend stock: got %v, wanted 0", *value)
	}

	keyX, _, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new entry in re-opend stock: %v", err)
	}
	if keyX != key1 {
		t.Errorf("expected key reuse, wanted %d, got %d", key1, keyX)
	}
}
