package s4

import (
	"fmt"
	"testing"
)

func TestPath_DefaultPathIsEmpty(t *testing.T) {
	path := Path{}
	if got, want := path.Length(), 0; got != want {
		t.Errorf("default path is not empty, wanted %d, got %d", want, got)
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

func TestPath_AppendingWorks(t *testing.T) {
	tests := []struct {
		path  *Path
		print string
	}{
		{new(Path), "-empty-"},
		{new(Path).Append(Nibble(2)), "2 : 1"},
		{new(Path).Append(Nibble(2)).Append(Nibble(4)), "24 : 2"},
		{new(Path).Append(Nibble(2)).Append(Nibble(4)).Append(Nibble(0xC)), "24c : 3"},
	}

	for _, test := range tests {
		if got, want := test.path.String(), test.print; got != want {
			t.Errorf("invalid concatentation, wanted %s, got %s", want, got)
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
		if err := encoder.Store(buffer[:], &path); err != nil {
			t.Fatalf("failed to encode path %v: %v", path, err)
		}
		restored := Path{}
		if err := encoder.Load(buffer[:], &restored); err != nil {
			t.Fatalf("failed to decode path %v: %v", path, err)
		}
		if path != restored {
			t.Fatalf("failed to restore path, wanted %v, got %v", path, restored)
		}
	}
}
