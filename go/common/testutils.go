package common

import (
	"encoding/binary"
	"sort"
	"testing"
)

func AssertEqualArrays[V Identifier](t *testing.T, first, second []V) {
	sort.Slice(first, func(i, j int) bool { return first[i] < first[j] })
	sort.Slice(second, func(i, j int) bool { return second[i] < second[j] })
	if len(first) != len(second) {
		t.Errorf("array sizes differ, %d != %d", len(first), len(second))
		return
	}
	for i := 0; i < len(first); i++ {
		if first[i] != second[i] {
			t.Errorf("assertValues failed")
		}
	}
}

func AssertArraySorted[T any](t *testing.T, arr []T, comparator Comparator[T]) {
	var prev T
	for i := 0; i < len(arr); i++ {
		if comparator.Compare(&prev, &arr[i]) > 0 {
			t.Errorf("Unsorted: %v < %v", prev, arr[i])
		}
		prev = arr[i]
	}
}

func AddressFromNumber(num int) (address Address) {
	addr := binary.BigEndian.AppendUint32([]byte{}, uint32(num))
	copy(address[:], addr)
	return
}
