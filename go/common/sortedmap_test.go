package common

import (
	"math/rand"
	"testing"
)

const sortedMapCapacity = 5

func TestSortedMapIsMap(t *testing.T) {
	var instance SortedMap[Address, uint32]
	var _ Map[Address, uint32] = &instance
}

func TestSortedMapGetPut(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

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
		t.Errorf("SizeBytes does not fit: %d", size)
	}
}

func TestSortedMapBulk(t *testing.T) {
	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(102)
	data := make([]MapEntry[Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := Address{byte(i + 1)}
		data[i] = MapEntry[Address, uint32]{address, i + 1}
	}

	h.bulkInsert(data)

	if size := h.Size(); size != int(max) {
		t.Errorf("SizeBytes does not match: %d != %d", size, max)
	}

	// inserted data must much returned data
	for i, entry := range h.GetEntries() {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(h.GetEntries()); size != int(max) {
		t.Errorf("SizeBytes does not match: %d != %d", size, max)
	}

}

func TestSortedMapBulkMultipleTimes(t *testing.T) {
	b := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	if _, exists := b.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	max := uint32(20)
	data := make([]MapEntry[Address, uint32], 0, max)
	for i := uint32(0); i < max; i++ {
		address := Address{byte(i + 1)}
		data = append(data, MapEntry[Address, uint32]{address, i + 1})
	}

	b.bulkInsert(data)

	nextMax := uint32(30)
	nextData := make([]MapEntry[Address, uint32], 0, nextMax)
	for i := max; i < nextMax+max; i++ {
		address := Address{byte(i + 1)}
		nextData = append(nextData, MapEntry[Address, uint32]{address, i + 1})
	}

	b.bulkInsert(nextData)

	allData := append(data, nextData...)
	// inserted data must much returned data
	for i, entry := range b.GetEntries() {
		if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
		}
	}

	if size := len(b.GetEntries()); size != int(max+nextMax) {
		t.Errorf("SizeBytes does not match: %d != %d", size, max+nextMax)
	}

	// pickup values in order
	arr := make([]Address, 0, max)
	b.ForEach(func(k Address, v uint32) {
		arr = append(arr, k)
	})

	AssertArraySorted[Address](t, arr, AddressComparator{})
}

func TestSortedMapInverseGetPut(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

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
		t.Errorf("SizeBytes does not fit: %d", size)
	}
}

func TestSortedMapSorting(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	// insert random (5..125)
	max := 120
	for i := 0; i < max; i++ {
		r := rand.Intn(max) + 5
		h.Put(Address{byte(r)}, uint32(i))
	}

	// deliberately insert at the beginning and end
	h.Put(Address{byte(125)}, 66)
	h.Put(Address{byte(1)}, 99)

	// pickup values in order
	arr := make([]Address, 0, max)
	h.ForEach(func(k Address, v uint32) {
		arr = append(arr, k)
	})

	AssertArraySorted[Address](t, arr, AddressComparator{})

	if size := h.Size(); size != len(arr) {
		t.Errorf("SizeBytes does not fit: %d", size)
	}

}

func TestSortedMapSize(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Put(AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("SizeBytes is not correct: %d != %d", size, n)
	}
}

func TestSortedMapRemove(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	if exists := h.Remove(C); exists {
		t.Errorf("remove from empty map failed")
	}

	h.Put(C, 99)
	if exists := h.Remove(C); !exists {
		t.Errorf("remove failed:  %v ", C)
	}
	if actual, exists := h.Get(C); exists || actual == 99 {
		t.Errorf("remove failed:  %v -> %v", C, actual)
	}

	h.Put(A, 1)
	h.Put(B, 2)
	h.Put(C, 3)

	// remove from middle
	if exists := h.Remove(B); !exists {
		t.Errorf("remove failed:  %v ", B)
	}

	// remove from last
	if exists := h.Remove(C); !exists {
		t.Errorf("remove failed:  %v", B)
	}

	if exists := h.Remove(A); !exists {
		t.Errorf("remove failed:  %v", B)
	}
}
