package mpt

import "testing"

func TestNodePath_IsValidMapKey(t *testing.T) {
	// this just needs to compile to pass the test
	var _ map[NodePath]bool
}

func TestNodePath_DefaultIsInvalid(t *testing.T) {
	path := NodePath{}
	if path.IsValid() {
		t.Errorf("default value should not be valid")
	}
}

func TestNodePath_EmptyPathIsValid(t *testing.T) {
	path := EmptyPath()
	if !path.IsValid() {
		t.Errorf("empty path should not be valid")
	}
}

func TestNodePath_EmptyPathHasLengthZero(t *testing.T) {
	path := EmptyPath()
	if got, want := path.Length(), 0; got != want {
		t.Errorf("unexpected length of empty path, wanted %d, got %d", want, got)
	}
}

func TestNodePath_StepsCanBeAppended(t *testing.T) {
	path := EmptyPath()

	p1 := path.Child(Nibble(1))
	if got, want := p1.Length(), 1; got != want {
		t.Errorf("unexpected length, wanted %d, got %d", want, got)
	}
	if got, want := p1.Get(0), Nibble(1); got != want {
		t.Errorf("unexpected element, wanted %v, got %v", want, got)
	}
	if got, want := p1.String(), "[1]"; got != want {
		t.Errorf("unexpected print, wanted %v, got %v", want, got)
	}

	p2 := p1.Child(Nibble(2))
	if got, want := p2.Length(), 2; got != want {
		t.Errorf("unexpected length, wanted %d, got %d", want, got)
	}
	if got, want := p2.Get(0), Nibble(1); got != want {
		t.Errorf("unexpected element, wanted %v, got %v", want, got)
	}
	if got, want := p2.Get(1), Nibble(2); got != want {
		t.Errorf("unexpected element, wanted %v, got %v", want, got)
	}
	if got, want := p2.String(), "[1,2]"; got != want {
		t.Errorf("unexpected print, wanted %v, got %v", want, got)
	}
}

func TestNodePath_AppendingBeyondTheMaximumLengthResultsInInvalidPath(t *testing.T) {
	path := CreateNodePath(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
	if !path.IsValid() {
		t.Errorf("failed to create path of maximum length")
	}
	if want, got := 14, path.Length(); want != got {
		t.Errorf("invalid length, wanted %d, got %d", want, got)
	}
	next := path.Child(15)
	if next.IsValid() {
		t.Error("too long path should be invalid")
	}
}

func TestNodePath_ToString(t *testing.T) {
	tests := []struct {
		path   NodePath
		result string
	}{
		{NodePath{}, "-invalid-"},
		{CreateNodePath(), "[]"},
		{CreateNodePath(2), "[2]"},
		{CreateNodePath(2, 7), "[2,7]"},
		{CreateNodePath(2, 7, 0xa), "[2,7,a]"},
	}

	for _, test := range tests {
		if got, want := test.path.String(), test.result; got != want {
			t.Errorf("unexpected print, wanted %s, got %s", want, got)
		}
	}
}
