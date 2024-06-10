// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPath_DefaultPathIsEmpty(t *testing.T) {
	path := Path{}
	if got, want := path.Length(), 0; got != want {
		t.Errorf("default path is not empty, wanted %d, got %d", want, got)
	}
}

func TestPath_OutOfRange_Index(t *testing.T) {
	path := Path{}
	if got, want := path.Get(-1), Nibble(0); got != want {
		t.Errorf("out of range index should produce empty nibble: %v != %v", got, want)
	}
}

func TestPath_SingleStepPathsAreCorrectlyConstructed(t *testing.T) {
	for n := Nibble(0); n <= Nibble(15); n++ {
		path := SingleStepPath(n)
		if got, want := path.Length(), 1; got != want {
			t.Errorf("invalid length, got %d, want %d", got, want)
		}
		if got, want := path.String(), fmt.Sprintf("%v : 1", n); got != want {
			t.Errorf("invalid print, got %s, want %s", got, want)
		}
	}
}

func TestPath_PathsCanBeCreatedFromNibbles(t *testing.T) {
	tests := []struct {
		nibbles []Nibble
		print   string
	}{
		{[]Nibble{}, "-empty-"},
		{[]Nibble{1, 2, 3}, "123 : 3"},
		{[]Nibble{2, 8, 0xa, 5}, "28a5 : 4"},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.nibbles)
		if got, want := path.String(), test.print; got != want {
			t.Errorf("invalid creation, wanted %s, got %s", want, got)
		}
	}
}

func TestPath_IndividualPositionsCanBeSet(t *testing.T) {
	tests := []struct {
		nibbles     []Nibble
		position    int
		update      int
		print       string
		shouldPanic bool
	}{
		{[]Nibble{}, 0, 2, "-empty-", true},
		{[]Nibble{}, 1, 2, "-empty-", true},
		{[]Nibble{}, -1, 2, "-empty-", true},

		{[]Nibble{1}, 0, 2, "2 : 1", false},
		{[]Nibble{1}, 1, 2, "1 : 1", true},
		{[]Nibble{1}, -1, 2, "1 : 1", true},

		{[]Nibble{1, 2, 3, 4}, 0, 9, "9234 : 4", false},
		{[]Nibble{1, 2, 3, 4}, 1, 9, "1934 : 4", false},
		{[]Nibble{1, 2, 3, 4}, 2, 9, "1294 : 4", false},
		{[]Nibble{1, 2, 3, 4}, 3, 9, "1239 : 4", false},

		{[]Nibble{1, 2, 3, 4, 5}, 0, 9, "92345 : 5", false},
		{[]Nibble{1, 2, 3, 4, 5}, 1, 9, "19345 : 5", false},
		{[]Nibble{1, 2, 3, 4, 5}, 2, 9, "12945 : 5", false},
		{[]Nibble{1, 2, 3, 4, 5}, 3, 9, "12395 : 5", false},
		{[]Nibble{1, 2, 3, 4, 5}, 4, 9, "12349 : 5", false},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.nibbles)
		paniced := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					paniced = true
				}
			}()
			path.Set(test.position, Nibble(test.update))
		}()
		if got, want := path.String(), test.print; got != want {
			t.Errorf("invalid set, wanted %s, got %s", want, got)
		}
		if paniced != test.shouldPanic {
			t.Errorf("invalid panic result, wanted %t, got %t", test.shouldPanic, paniced)
		}
	}
}

func TestPath_AppendingWorks(t *testing.T) {
	tests := []struct {
		path  []Nibble
		print string
	}{
		{[]Nibble{}, "-empty-"},
		{[]Nibble{2}, "2 : 1"},
		{[]Nibble{2, 4}, "24 : 2"},
		{[]Nibble{2, 4, 8}, "248 : 3"},
		{[]Nibble{2, 4, 8, 1}, "2481 : 4"},
	}

	for _, test := range tests {
		path := new(Path)
		for _, n := range test.path {
			path.Append(n)
		}
		if got, want := path.String(), test.print; got != want {
			t.Errorf("invalid append, wanted %s, got %s", want, got)
		}
	}
}

func TestPath_AppendAllWorks(t *testing.T) {
	tests := []struct {
		a, b  []Nibble
		print string
	}{
		{[]Nibble{}, []Nibble{}, "-empty-"},
		{[]Nibble{2}, []Nibble{}, "2 : 1"},
		{[]Nibble{}, []Nibble{2}, "2 : 1"},
		{[]Nibble{1, 2, 3}, []Nibble{4, 5, 6}, "123456 : 6"},
	}

	for _, test := range tests {
		a := CreatePathFromNibbles(test.a)
		b := CreatePathFromNibbles(test.b)
		r := a.AppendAll(&b)
		if got, want := r.String(), test.print; got != want {
			t.Errorf("invalid appendAll, wanted %s, got %s", want, got)
		}
	}
}

func TestPath_PrependingWorks(t *testing.T) {
	tests := []struct {
		path  []Nibble
		print string
	}{
		{[]Nibble{}, "-empty-"},
		{[]Nibble{2}, "2 : 1"},
		{[]Nibble{2, 4}, "42 : 2"},
		{[]Nibble{2, 4, 8}, "842 : 3"},
		{[]Nibble{2, 4, 8, 1}, "1842 : 4"},
	}

	for _, test := range tests {
		path := new(Path)
		for _, n := range test.path {
			path.Prepend(n)
		}
		if got, want := path.String(), test.print; got != want {
			t.Errorf("invalid prepend, wanted %s, got %s", want, got)
		}
	}
}

func TestPath_GetCommonPrefixLength(t *testing.T) {
	tests := []struct {
		a, b   []Nibble
		length int
	}{
		{[]Nibble{}, []Nibble{}, 0},
		{[]Nibble{}, []Nibble{1}, 0},
		{[]Nibble{1}, []Nibble{}, 0},
		{[]Nibble{1}, []Nibble{1}, 1},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2, 3}, 3},
		{[]Nibble{1, 2}, []Nibble{1, 2, 3}, 2},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2}, 2},
		{[]Nibble{0, 0, 0, 2, 0, 0}, []Nibble{0, 0, 0, 1, 0, 0}, 3},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.a)
		if got, want := path.GetCommonPrefixLength(test.b), test.length; got != want {
			t.Errorf("invalid common prefix of %v and %v, wanted %d, got %d", test.a, test.b, want, got)
		}
	}
}

func TestPath_RemoveLast(t *testing.T) {
	tests := []struct {
		path   []Nibble
		remove int
		result string
	}{
		{[]Nibble{}, 0, "-empty-"},
		{[]Nibble{}, 1, "-empty-"},
		{[]Nibble{}, 2, "-empty-"},

		{[]Nibble{1, 2, 3}, 0, "123 : 3"},
		{[]Nibble{1, 2, 3}, 1, "12 : 2"},
		{[]Nibble{1, 2, 3}, 2, "1 : 1"},
		{[]Nibble{1, 2, 3}, 3, "-empty-"},
		{[]Nibble{1, 2, 3}, 4, "-empty-"},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.path)
		path = *path.RemoveLast(test.remove)
		if got, want := path.String(), test.result; got != want {
			t.Errorf("invalid result after removing last %d elements from %v, wanted %s, got %s", test.remove, test.path, want, got)
		}
	}
}

func TestPath_ShiftLeft(t *testing.T) {
	tests := []struct {
		path   []Nibble
		shift  int
		result string
	}{
		{[]Nibble{}, 0, "-empty-"},

		{[]Nibble{1, 2, 3}, 0, "123 : 3"},
		{[]Nibble{1, 2, 3}, 1, "23 : 2"},
		{[]Nibble{1, 2, 3}, 2, "3 : 1"},
		{[]Nibble{1, 2, 3}, 3, "-empty-"},
		{[]Nibble{1, 2, 3}, 4, "-empty-"},

		{[]Nibble{1, 2, 3}, -1, "123 : 3"},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.path)
		path = *path.ShiftLeft(test.shift)
		if got, want := path.String(), test.result; got != want {
			t.Errorf("invalid shift result of %v and %v, wanted %s, got %s", test.path, test.shift, want, got)
		}
	}
}

func TestPath_GetPackedNibbles(t *testing.T) {
	tests := []struct {
		input  []Nibble
		result []byte
	}{
		{[]Nibble{}, []byte{}},
		{[]Nibble{1}, []byte{0x01}},
		{[]Nibble{2}, []byte{0x02}},
		{[]Nibble{1, 2}, []byte{0x12}},
		{[]Nibble{1, 2, 3}, []byte{0x01, 0x23}},
		{[]Nibble{1, 2, 3, 4}, []byte{0x12, 0x34}},
		{[]Nibble{1, 2, 3, 4, 5}, []byte{0x01, 0x23, 0x45}},
		{[]Nibble{5, 4, 3, 2, 1}, []byte{0x05, 0x43, 0x21}},
		{[]Nibble{0xa, 0xb, 0xc, 0xd, 0xe}, []byte{0x0a, 0xbc, 0xde}},
		{[]Nibble{0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, []byte{0xab, 0xcd, 0xef}},
	}

	for _, test := range tests {
		path := CreatePathFromNibbles(test.input)
		got := path.GetPackedNibbles()
		if !bytes.Equal(test.result, got) {
			t.Errorf("unexpected result, wanted %x, got %x", test.result, got)
		}
	}
}

func TestPath_EncodingAndDecoding(t *testing.T) {
	paths := []Path{
		{},
		{[32]byte{1, 2, 4, 5}, 8},
		{[32]byte{
			1, 2, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
			31, 32,
		}, 64},
	}

	var buffer [33]byte
	encoder := PathEncoder{}
	for _, path := range paths {
		encoder.Store(buffer[:], &path)
		restored := Path{}
		encoder.Load(buffer[:], &restored)
		if path != restored {
			t.Fatalf("failed to restore path, wanted %v, got %v", path, restored)
		}
	}
}

func TestPath_IsEqualTo(t *testing.T) {
	tests := []struct {
		a, b []Nibble
		eq   bool
	}{
		{[]Nibble{}, []Nibble{}, true},
		{[]Nibble{1}, []Nibble{}, false},
		{[]Nibble{1}, []Nibble{1}, true},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2, 3}, true},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2, 4}, false},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2, 3, 4}, false},
		{[]Nibble{1, 2, 3}, []Nibble{1, 2}, false},
	}

	for _, test := range tests {
		a := CreatePathFromNibbles(test.a)
		if got, want := a.IsEqualTo(test.b), test.eq; got != want {
			t.Errorf("invalid comparison, wanted %t, got %t", want, got)
		}
	}
}
