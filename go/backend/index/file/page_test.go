package file

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestPageGetPut(t *testing.T) {

	h := initPage(5)
	h.isDirty = false
	if _, exists := h.get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	if val, exists := h.get(A); !exists || val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.get(B); !exists || val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.get(C); !exists || val != 30 {
		t.Errorf("Value is not correct")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	verifyPageSorted(t, h)

	// replace
	h.put(A, 33)
	if val, exists := h.get(A); !exists || val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.put(B, 44)
	if val, exists := h.get(B); !exists || val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.put(C, 55)
	if val, exists := h.get(C); !exists || val != 55 {
		t.Errorf("Value is not correct")
	}

	if size := h.sizeKeys(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}

	verifyPageSorted(t, h)

	actualData := make(map[common.Address]uint32, 123)
	h.forEach(func(k common.Address, v uint32) {
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

	if len(actualData) != h.sizeKeys() {
		t.Errorf("Wrong number of items received from for-each")
	}
}

func TestPageInverseGetPut(t *testing.T) {
	h := initPage(5)
	if _, exists := h.get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.put(C, 30)
	h.put(B, 20)
	h.put(A, 10)

	verifyPageSorted(t, h)

	if val, _ := h.get(A); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.get(B); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.get(C); val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.put(A, 33)
	if val, _ := h.get(A); val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.put(B, 44)
	if val, _ := h.get(B); val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.put(C, 55)
	if val, _ := h.get(C); val != 55 {
		t.Errorf("Value is not correct")
	}

	verifyPageSorted(t, h)

	if size := h.sizeKeys(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}
}

func TestPageBulk(t *testing.T) {
	h := initPage(50)
	h.isDirty = false
	if _, exists := h.get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(50)
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	h.bulkInsert(data)

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	if size := h.sizeKeys(); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	for i, entry := range h.getEntries() {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(h.getEntries()); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}
}

func TestPageBulkMultipleTimes(t *testing.T) {
	h := initPage(1000)

	if _, exists := h.get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(1000 / 2)
	data := make([]common.MapEntry[common.Address, uint32], 0, max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data = append(data, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.bulkInsert(data)

	nextMax := uint32(1000/2 - 1)
	nextData := make([]common.MapEntry[common.Address, uint32], 0, nextMax)
	for i := max; i < nextMax+max; i++ {
		address := common.Address{byte(i + 1)}
		nextData = append(nextData, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.bulkInsert(nextData)

	allData := append(data, nextData...)
	// inserted data must much returned data
	for i, entry := range h.getEntries() {
		if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
		}
	}

	if size := len(h.getEntries()); size != int(max+nextMax) {
		t.Errorf("Size does not match: %d != %d", size, max+nextMax)
	}

	// pickup values in order
	arr := make([]common.Address, 0, max)
	h.forEach(func(k common.Address, v uint32) {
		arr = append(arr, k)
	})
}

func TestPageSize(t *testing.T) {
	h := initPage(10000)

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.put(common.AddressFromNumber(i), uint32(i))
	}

	if size := h.sizeKeys(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestPageRemove(t *testing.T) {
	h := initPage(5)

	if exists := h.remove(C); exists {
		t.Errorf("Remove from empty map failed")
	}

	h.put(C, 99)
	if exists := h.remove(C); !exists {
		t.Errorf("Remove failed:  %v", C)
	}
	if actual, exists := h.get(C); exists || actual == 99 {
		t.Errorf("Remove failed:  %v -> %v", C, actual)
	}

	h.put(A, 1)
	h.put(B, 2)
	h.put(C, 3)

	if size := h.sizeKeys(); size != 3 {
		t.Errorf("Size is not correct: %d ", size)
	}

	h.isDirty = false

	// remove from middle
	if exists := h.remove(B); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if size := h.sizeKeys(); size != 2 {
		t.Errorf("Size is not correct: %d ", size)
	}

	// remove from last
	if exists := h.remove(C); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if size := h.sizeKeys(); size != 1 {
		t.Errorf("Size is not correct: %d ", size)
	}

	if exists := h.remove(A); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	h.forEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageClear(t *testing.T) {
	h := initPage(5)

	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	if size := h.sizeKeys(); size != 3 {
		t.Errorf("Size is not correct: %d", size)
	}

	h.isDirty = false
	h.Clear()

	if size := h.sizeKeys(); size != 0 {
		t.Errorf("Size is not correct: %d", size)
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	h.forEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestPageFindAt(t *testing.T) {
	h := initPage(5)
	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	if idx, exists := h.findItem(B); !exists || idx != 1 {
		t.Errorf("Wrong index: %d", idx)
	}

	// insert in reverse order
	h = initPage(5)
	h.put(C, 30)
	h.put(A, 10)
	h.put(B, 20)

	if idx, exists := h.findItem(C); !exists || idx != 2 {
		t.Errorf("Wrong index: %d", idx)
	}
}

func TestPageInsertAt(t *testing.T) {
	h := initPage(5)
	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	h.insertAt(1, D, 40)

	expected := []common.Address{A, D, B, C}
	for i, entry := range h.getEntries() {
		if expected[i] != entry.Key {
			t.Errorf("Keys do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageUpdateAt(t *testing.T) {
	h := initPage(5)
	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	h.updateAt(1, 40)

	expected := []uint32{10, 40, 30}
	for i, entry := range h.getEntries() {
		if expected[i] != entry.Val {
			t.Errorf("Values do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageGetAt(t *testing.T) {
	h := initPage(5)
	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	expected := []uint32{10, 20, 30}
	for i := range h.getEntries() {
		if expected[i] != h.getAt(i) {
			t.Errorf("Values do not mach: %v != %v", expected[i], h.getAt(i))
		}
	}
}

func TestPageRemoveAt(t *testing.T) {
	h := initPage(5)
	h.put(A, 10)
	h.put(B, 20)
	h.put(C, 30)

	h.removeAt(1)

	expected := []uint32{10, 30}
	for i, entry := range h.getEntries() {
		if expected[i] != entry.Val {
			t.Errorf("Values do not mach: %v != %v", expected[i], entry.Key)
		}
	}
}

func TestPageSetGetNext(t *testing.T) {
	h := initPage(5)

	h.setNext(30)

	if next := h.hasNext; !next {
		t.Errorf("Next not set")
	}

	if next := h.next; next != 30 {
		t.Errorf("Next not set")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}

	// remove link
	h.removeNext()

	if next := h.hasNext; next {
		t.Errorf("Next should not be set")
	}

	if next := h.next; next != 0 {
		t.Errorf("Next should not be set")
	}

	if !h.IsDirty() {
		t.Errorf("The dirty flag incorectly set")
	}
}

func verifyPageSorted(t *testing.T, h *Page[common.Address, uint32]) {
	keys := make([]common.Address, 0, h.sizeKeys())
	for _, entry := range h.getEntries() {
		keys = append(keys, entry.Key)
	}
	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})

	keys = make([]common.Address, 0, h.sizeKeys())
	h.forEach(func(k common.Address, v uint32) {
		keys = append(keys, k)
	})

	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})
}

func initPage(capacity int) *Page[common.Address, uint32] {
	sizeBytes := byteSizePage[common.Address, uint32](capacity, common.AddressSerializer{}, common.Identifier32Serializer{})
	return PageFactory[common.Address, uint32](sizeBytes, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})()
}
