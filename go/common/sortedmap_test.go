package common

import (
	"math/rand"
	"testing"
)

const sortedMapCapacity = 5

var (
	A = Address{0xAA}
	B = Address{0xBB}
	C = Address{0xCC}
)

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
		t.Errorf("Size does not fit: %d", size)
	}
}

func TestSortedMapBulk(t *testing.T) {
	max := uint32(102)
	data := make([]MapEntry[Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := Address{byte(i + 1)}
		data[i] = MapEntry[Address, uint32]{address, i + 1}
	}

	h := InitSortedMap[Address, uint32](sortedMapCapacity, data, AddressComparator{})

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
		t.Errorf("Size does not fit: %d", size)
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
		t.Errorf("Size does not fit: %d", size)
	}

}

func TestSortedMapSize(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Put(AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestSortedMapRemove(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	if exists := h.Remove(C); exists {
		t.Errorf("Remove from empty map failed")
	}

	h.Put(C, 99)
	if exists := h.Remove(C); !exists {
		t.Errorf("Remove failed:  %v ", C)
	}
	if actual, exists := h.Get(C); exists || actual == 99 {
		t.Errorf("Remove failed:  %v -> %v", C, actual)
	}

	h.Put(A, 1)
	h.Put(B, 2)
	h.Put(C, 3)

	// remove from middle
	if exists := h.Remove(B); !exists {
		t.Errorf("Remove failed:  %v ", B)
	}

	// remove from last
	if exists := h.Remove(C); !exists {
		t.Errorf("Remove failed:  %v", B)
	}

	if exists := h.Remove(A); !exists {
		t.Errorf("Remove failed:  %v", B)
	}
}
