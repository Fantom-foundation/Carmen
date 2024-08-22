package io

import (
	"bytes"
	"strings"
	"testing"
)

func TestPosition_CanBeCreatedAndPrinted(t *testing.T) {
	tests := []struct {
		steps []byte
		print string
	}{
		{nil, ""},
		{[]byte{0}, "0"},
		{[]byte{1}, "1"},
		{[]byte{0, 0}, "0.0"},
		{[]byte{0, 1}, "0.1"},
		{[]byte{1, 0}, "1.0"},
		{[]byte{1, 2, 3}, "1.2.3"},
	}

	for _, test := range tests {
		pos := newPosition(test.steps)
		if pos.String() != test.print {
			t.Errorf("expected %s, got %s", test.print, pos.String())
		}
	}
}

func TestPosition_ChildrenCanBeDerived(t *testing.T) {
	tests := []struct {
		base  []byte
		step  byte
		print string
	}{
		{nil, 1, "1"},
		{nil, 2, "2"},
		{[]byte{1, 2}, 3, "1.2.3"},
	}

	for _, test := range tests {
		pos := newPosition(test.base)
		pos = pos.Child(test.step)
		if pos.String() != test.print {
			t.Errorf("expected %s, got %s", test.print, pos.String())
		}
	}
}

func TestPosition_AreOrdered(t *testing.T) {
	paths := [][]byte{
		nil,
		{1},
		{1, 2},
		{1, 2, 3},
	}

	for _, a := range paths {
		for _, b := range paths {
			aa := newPosition(a)
			bb := newPosition(b)
			want := bytes.Compare(a, b)
			if got := aa.compare(bb); got != want {
				t.Errorf("expected compare(%v,%v)=%d, got %d", aa, bb, got, want)
			}
		}
	}
}

func TestPosition_AreOrderedAndWorkWithSharedPrefixes(t *testing.T) {
	positions := []*position{}
	var position *position
	positions = append(positions, position)
	for i := 0; i < 5; i++ {
		position = position.Child(byte(i))
		positions = append(positions, position)
	}

	for _, a := range positions {
		for _, b := range positions {
			want := strings.Compare(a.String(), b.String())
			if got := a.compare(b); got != want {
				t.Errorf("expected compare(%v,%v)=%d, got %d", a, b, got, want)
			}
		}
	}
}
