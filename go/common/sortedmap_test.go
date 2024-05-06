// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"math/rand"
	"slices"
	"testing"
)

const sortedMapCapacity = 5

func TestSortedMapIsMap(t *testing.T) {
	var instance SortedMap[Address, uint32]
	var _ Map[Address, uint32] = &instance
}

func TestSortedMapGetPut(t *testing.T) {

	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})

	if _, exists := h.Get(addressA); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(addressA, 10)
	h.Put(addressB, 20)
	h.Put(addressC, 30)

	if val, exists := h.Get(addressA); !exists || val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(addressB); !exists || val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(addressC); !exists || val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.Put(addressA, 33)
	if val, exists := h.Get(addressA); !exists || val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(addressB, 44)
	if val, exists := h.Get(addressB); !exists || val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(addressC, 55)
	if val, exists := h.Get(addressC); !exists || val != 55 {
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

	if _, exists := h.Get(addressA); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(addressC, 30)
	h.Put(addressB, 20)
	h.Put(addressA, 10)

	if val, _ := h.Get(addressA); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(addressB); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(addressC); val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.Put(addressA, 33)
	if val, _ := h.Get(addressA); val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(addressB, 44)
	if val, _ := h.Get(addressB); val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(addressC, 55)
	if val, _ := h.Get(addressC); val != 55 {
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

	cmp := AddressComparator{}
	if !slices.IsSortedFunc(arr, func(a, b Address) int {
		return cmp.Compare(&a, &b)
	}) {
		t.Errorf("array is not sorted: %v", arr)
	}

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

	if exists := h.Remove(addressC); exists {
		t.Errorf("Remove from empty map failed")
	}

	h.Put(addressC, 99)
	if exists := h.Remove(addressC); !exists {
		t.Errorf("Remove failed:  %v ", addressC)
	}
	if actual, exists := h.Get(addressC); exists || actual == 99 {
		t.Errorf("Remove failed:  %v -> %v", addressC, actual)
	}

	h.Put(addressA, 1)
	h.Put(addressB, 2)
	h.Put(addressC, 3)

	// remove from middle
	if exists := h.Remove(addressB); !exists {
		t.Errorf("Remove failed:  %v ", addressB)
	}

	// remove from last
	if exists := h.Remove(addressC); !exists {
		t.Errorf("Remove failed:  %v", addressB)
	}

	if exists := h.Remove(addressA); !exists {
		t.Errorf("Remove failed:  %v", addressB)
	}
}

func TestSortedMap_GetMemoryFootprint(t *testing.T) {
	h := NewSortedMap[Address, uint32](sortedMapCapacity, AddressComparator{})
	h.Put(addressA, 1)

	if h.GetMemoryFootprint().Total() <= 0 {
		t.Errorf("no memory footprint provided")
	}
}
