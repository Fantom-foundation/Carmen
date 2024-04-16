//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package mpt

import "testing"

func TestNibble_Print(t *testing.T) {
	tests := []struct {
		value Nibble
		print string
	}{
		{Nibble(0), "0"},
		{Nibble(1), "1"},
		{Nibble(2), "2"},
		{Nibble(3), "3"},
		{Nibble(4), "4"},
		{Nibble(5), "5"},
		{Nibble(6), "6"},
		{Nibble(7), "7"},
		{Nibble(8), "8"},
		{Nibble(9), "9"},
		{Nibble(10), "a"},
		{Nibble(11), "b"},
		{Nibble(12), "c"},
		{Nibble(13), "d"},
		{Nibble(14), "e"},
		{Nibble(15), "f"},
		{Nibble(16), "?"},
		{Nibble(255), "?"},
	}

	for _, test := range tests {
		if got, want := test.value.String(), test.print; got != want {
			t.Errorf("invalid print, got %s, wanted %s", got, want)
		}
	}
}

func TestNibbles_GetCommonPrefix(t *testing.T) {
	tests := []struct {
		a, b []byte
		res  int
	}{
		{[]byte{}, []byte{}, 0},
		{[]byte{}, []byte{1}, 0},
		{[]byte{1}, []byte{}, 0},

		{[]byte{1}, []byte{1}, 1},
		{[]byte{1, 2}, []byte{1, 2}, 2},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, 3},

		{[]byte{1, 2, 3}, []byte{1, 2, 3, 4, 5}, 3},
		{[]byte{1, 2, 3, 4, 5}, []byte{1, 2, 3}, 3},

		{[]byte{1, 2, 3}, []byte{1, 3, 2}, 1},
		{[]byte{1, 2, 3}, []byte{3, 2, 1}, 0},
	}

	for _, test := range tests {
		a := make([]Nibble, len(test.a))
		for i, cur := range test.a {
			a[i] = Nibble(cur)
		}
		b := make([]Nibble, len(test.b))
		for i, cur := range test.b {
			b[i] = Nibble(cur)
		}
		if got, want := GetCommonPrefixLength(a, b), test.res; got != want {
			t.Errorf("invalid common prefix length of %v and %v, got %d, wanted %d", a, b, got, want)
		}
	}
}

func TestNibbles_IsPrefixOf(t *testing.T) {
	tests := []struct {
		a, b []byte
		res  bool
	}{
		{[]byte{}, []byte{}, true},
		{[]byte{}, []byte{1}, true},
		{[]byte{1}, []byte{}, false},

		{[]byte{1}, []byte{1}, true},
		{[]byte{1, 2}, []byte{1, 2}, true},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, true},

		{[]byte{1, 2, 3}, []byte{1, 2, 3, 4, 5}, true},
		{[]byte{1, 2, 3, 4, 5}, []byte{1, 2, 3}, false},

		{[]byte{1, 2, 3}, []byte{1, 3, 2}, false},
		{[]byte{1, 2, 3}, []byte{3, 2, 1}, false},
	}

	for _, test := range tests {
		a := make([]Nibble, len(test.a))
		for i, cur := range test.a {
			a[i] = Nibble(cur)
		}
		b := make([]Nibble, len(test.b))
		for i, cur := range test.b {
			b[i] = Nibble(cur)
		}
		if got, want := IsPrefixOf(a, b), test.res; got != want {
			t.Errorf("invalid is-prefix-of result for %v and %v, got %t, wanted %t", a, b, got, want)
		}
	}
}
