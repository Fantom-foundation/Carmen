package s4

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
