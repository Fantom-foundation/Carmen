package eviction

import (
	"reflect"
	"testing"
)

func TestFlatSet_Add(t *testing.T) {
	fs := NewFlatSet(5)

	fs.Add(1)
	fs.Add(2)
	fs.Add(3)
	fs.Add(3)

	if len(fs.entries) != 3 {
		t.Errorf("unexpected entries len")
	}
	if !fs.Contains(1) || !fs.Contains(2) || !fs.Contains(3) {
		t.Errorf("does not contains expected items")
	}
}

func TestFlatSet_RemoveFirst(t *testing.T) {
	fs := NewFlatSet(5)
	fs.Add(1)
	fs.Add(2)
	fs.Add(3)

	fs.Remove(1)

	if !reflect.DeepEqual(fs.entries, []int{3, 2}) {
		t.Errorf("unexpected entries content")
	}
	if fs.Contains(1) || !fs.Contains(2) || !fs.Contains(3) {
		t.Errorf("does not contains expected items")
	}
}

func TestFlatSet_RemoveLast(t *testing.T) {
	fs := NewFlatSet(5)
	fs.Add(1)
	fs.Add(2)
	fs.Add(3)

	fs.Remove(3)

	if !reflect.DeepEqual(fs.entries, []int{1, 2}) {
		t.Errorf("unexpected entries content")
	}
	if !fs.Contains(1) || !fs.Contains(2) || fs.Contains(3) {
		t.Errorf("does not contains expected items")
	}
}

func TestFlatSet_RemoveMiddle(t *testing.T) {
	fs := NewFlatSet(5)
	fs.Add(1)
	fs.Add(2)
	fs.Add(3)

	fs.Remove(2)

	if !reflect.DeepEqual(fs.entries, []int{1, 3}) {
		t.Errorf("unexpected entries content")
	}
	if !fs.Contains(1) || fs.Contains(2) || !fs.Contains(3) {
		t.Errorf("does not contains expected items")
	}
}
