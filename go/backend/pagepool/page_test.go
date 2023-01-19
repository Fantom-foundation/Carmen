package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
	D = common.Address{0xDD}
)

func TestSortedPageIsMap(t *testing.T) {
	var instance Page[common.Address, uint32]
	var _ common.Map[common.Address, uint32] = &instance
}

func TestPageGetPut(t *testing.T) {

	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.isDirty = false
	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	if val, exists := h.Get(A); !exists || val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(B); !exists || val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(C); !exists || val != 30 {
		t.Errorf("Value is not correct")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	verifyPageSorted(t, h)

	// replace
	h.Put(A, 33)
	if val, exists := h.Get(A); !exists || val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(B, 44)
	if val, exists := h.Get(B); !exists || val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(C, 55)
	if val, exists := h.Get(C); !exists || val != 55 {
		t.Errorf("Value is not correct")
	}

	if size := h.Size(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}

	verifyPageSorted(t, h)

	actualData := make(map[common.Address]uint32, 123)
	h.ForEach(func(k common.Address, v uint32) {
		actualData[k] = v

		if k != A && k != B && k != C {
			t.Errorf("Unexpected key: %v", k)
		}
		if k == A && v != 33 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
		if k == B && v != 44 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
		if k == C && v != 55 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
	})

	if len(actualData) != h.Size() {
		t.Errorf("Wrong number of items received from for-each")
	}
}

func TestPageInverseGetPut(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(C, 30)
	h.Put(B, 20)
	h.Put(A, 10)

	verifyPageSorted(t, h)

	if val, _ := h.Get(A); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(B); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(C); val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.Put(A, 33)
	if val, _ := h.Get(A); val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(B, 44)
	if val, _ := h.Get(B); val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(C, 55)
	if val, _ := h.Get(C); val != 55 {
		t.Errorf("Value is not correct")
	}

	verifyPageSorted(t, h)

	if size := h.Size(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}
}

func TestPageBulk(t *testing.T) {
	h := NewPage[common.Address, uint32](50, common.AddressComparator{})
	h.isDirty = false
	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(50)
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	h.BulkInsert(data)

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	if size := h.Size(); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	for i, entry := range h.GetEntries() {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(h.GetEntries()); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}
}

func TestPageBulkMultipleTimes(t *testing.T) {
	h := NewPage[common.Address, uint32](1000, common.AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(1000 / 2)
	data := make([]common.MapEntry[common.Address, uint32], 0, max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data = append(data, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.BulkInsert(data)

	nextMax := uint32(1000/2 - 1)
	nextData := make([]common.MapEntry[common.Address, uint32], 0, nextMax)
	for i := max; i < nextMax+max; i++ {
		address := common.Address{byte(i + 1)}
		nextData = append(nextData, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.BulkInsert(nextData)

	allData := append(data, nextData...)
	// inserted data must much returned data
	for i, entry := range h.GetEntries() {
		if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
		}
	}

	if size := len(h.GetEntries()); size != int(max+nextMax) {
		t.Errorf("Size does not match: %d != %d", size, max+nextMax)
	}

	// pickup values in order
	arr := make([]common.Address, 0, max)
	h.ForEach(func(k common.Address, v uint32) {
		arr = append(arr, k)
	})
}

func TestPageSize(t *testing.T) {
	h := NewPage[common.Address, uint32](10000, common.AddressComparator{})

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Put(common.AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestPageRemove(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

	if exists := h.Remove(C); exists {
		t.Errorf("Remove from empty map failed")
	}

	h.Put(C, 99)
	if exists := h.Remove(C); !exists {
		t.Errorf("Remove failed:  %v", C)
	}
	if actual, exists := h.Get(C); exists || actual == 99 {
		t.Errorf("Remove failed:  %v -> %v", C, actual)
	}

	h.Put(A, 1)
	h.Put(B, 2)
	h.Put(C, 3)

	if size := h.Size(); size != 3 {
		t.Errorf("Size is not correct: %d ", size)
	}

	h.isDirty = false

	// remove from middle
	if exists := h.Remove(B); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if size := h.Size(); size != 2 {
		t.Errorf("Size is not correct: %d ", size)
	}

	// remove from last
	if exists := h.Remove(C); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if size := h.Size(); size != 1 {
		t.Errorf("Size is not correct: %d ", size)
	}

	if exists := h.Remove(A); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	h.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageClear(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	if size := h.Size(); size != 3 {
		t.Errorf("Size is not correct: %d", size)
	}

	h.isDirty = false
	h.Clear()

	if size := h.Size(); size != 0 {
		t.Errorf("Size is not correct: %d", size)
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	h.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageFindAt(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	if idx, exists := h.FindItem(B); !exists || idx != 1 {
		t.Errorf("Wrong index: %d", idx)
	}

	// insert in reverse order
	h = NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(C, 30)
	h.Put(A, 10)
	h.Put(B, 20)

	if idx, exists := h.FindItem(C); !exists || idx != 2 {
		t.Errorf("Wrong index: %d", idx)
	}
}

func TestPageInsertAt(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	h.InsertAt(1, D, 40)

	expected := []common.Address{A, D, B, C}
	for i, entry := range h.GetEntries() {
		if expected[i] != entry.Key {
			t.Errorf("Keys do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageUpdateAt(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	h.UpdateAt(1, 40)

	expected := []uint32{10, 40, 30}
	for i, entry := range h.GetEntries() {
		if expected[i] != entry.Val {
			t.Errorf("Values do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageGetAt(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	expected := []uint32{10, 20, 30}
	for i := range h.GetEntries() {
		if expected[i] != h.GetAt(i) {
			t.Errorf("Values do not mach: %v != %v", expected[i], h.GetAt(i))
		}
	}
}

func TestPageRemoveAt(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})
	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	h.removeAt(1)

	expected := []uint32{10, 30}
	for i, entry := range h.GetEntries() {
		if expected[i] != entry.Val {
			t.Errorf("Values do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageSetGetNext(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

	h.SetNext(NewPageId(10, 30))

	if next := h.HasNext(); !next {
		t.Errorf("Next not set")
	}

	if next := h.NextPage(); next != NewPageId(10, 30) {
		t.Errorf("Next not set")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	// remove link
	h.RemoveNext()

	if next := h.HasNext(); next {
		t.Errorf("Next should not be set")
	}

	if next := h.NextPage(); next != NewPageId(0, 0) {
		t.Errorf("Next should not be set")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}
}

func verifyPageSorted(t *testing.T, h *Page[common.Address, uint32]) {
	keys := make([]common.Address, 0, h.Size())
	for _, entry := range h.GetEntries() {
		keys = append(keys, entry.Key)
	}
	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})

	keys = make([]common.Address, 0, h.Size())
	h.ForEach(func(k common.Address, v uint32) {
		keys = append(keys, k)
	})

	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})
}
