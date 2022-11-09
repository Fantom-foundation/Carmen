package pagepool

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestSortedPageIsMap(t *testing.T) {
	var instance Page[common.Address, uint32]
	var _ common.Map[common.Address, uint32] = &instance
}

func TestSortedPageGetPut(t *testing.T) {

	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

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
}

func TestSortedPageInverseGetPut(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(C, 30)
	h.Put(B, 20)
	h.Put(A, 10)

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

	if size := h.Size(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}
}

func TestSortedPageBulk(t *testing.T) {
	h := NewPage[common.Address, uint32](5, common.AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(maxItems)
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
	}

	h.BulkInsert(data)

	if size := h.Size(); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	for i, entry := range h.GetAll() {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(h.GetAll()); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

}

func TestSortedPageBulkMultipleTimes(t *testing.T) {
	maxItems := 100
	h := NewPage[common.Address, uint32](maxItems, common.AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(maxItems / 2)
	data := make([]common.MapEntry[common.Address, uint32], 0, max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data = append(data, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.BulkInsert(data)

	nextMax := uint32(maxItems/2 - 1)
	nextData := make([]common.MapEntry[common.Address, uint32], 0, nextMax)
	for i := max; i < nextMax+max; i++ {
		address := common.Address{byte(i + 1)}
		nextData = append(nextData, common.MapEntry[common.Address, uint32]{address, i + 1})
	}

	h.BulkInsert(nextData)

	allData := append(data, nextData...)
	// inserted data must much returned data
	for i, entry := range h.GetAll() {
		if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
		}
	}

	if size := len(h.GetAll()); size != int(max+nextMax) {
		t.Errorf("Size does not match: %d != %d", size, max+nextMax)
	}

	// pickup values in order
	arr := make([]common.Address, 0, max)
	h.ForEach(func(k common.Address, v uint32) {
		arr = append(arr, k)
	})

	verifySort(t, arr)
}

func TestSortedPageSorting(t *testing.T) {

	h := NewPage[common.Address, uint32](130, common.AddressComparator{})

	// insert random (5..125)
	max := 120
	for i := 0; i < max; i++ {
		r := rand.Intn(max) + 5
		h.Put(common.Address{byte(r)}, uint32(i))
	}

	// deliberately insert at the beginning and end
	h.Put(common.Address{byte(125)}, 66)
	h.Put(common.Address{byte(1)}, 99)

	// pickup values in order
	arr := make([]common.Address, 0, max)
	h.ForEach(func(k common.Address, v uint32) {
		arr = append(arr, k)
	})

	verifySort(t, arr)

	if size := h.Size(); size != len(arr) {
		t.Errorf("Size does not fit: %d", size)
	}

}

func TestSortedPageSize(t *testing.T) {

	h := NewPage[common.Address, uint32](10000, common.AddressComparator{})

	n := rand.Intn(9999)
	for i := uint32(0); i < uint32(n); i++ {
		h.Put(toAddress(i), i)
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestSortedPageRemove(t *testing.T) {

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

}

func verifySort(t *testing.T, arr []common.Address) {
	var prev common.Address
	for i := 0; i < len(arr); i++ {
		if prev.Compare(&arr[i]) >= 0 {
			t.Errorf("Unsorted: %d < %d", prev, arr[i])
		}
		prev = arr[i]
	}
}

func toAddress(num uint32) (address common.Address) {
	addr := binary.BigEndian.AppendUint32([]byte{}, num)
	copy(address[:], addr)
	return
}
