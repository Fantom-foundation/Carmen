package mpt

import "testing"

func TestNodePath_DefaultIsEmptyPath(t *testing.T) {
	path := NodePath{}
	if want, got := 0, path.Length(); want != got {
		t.Errorf("default path is not empty, wanted %d, got %d", want, got)
	}
}

func TestNodePath_EmptyPathHasLengthZero(t *testing.T) {
	path := EmptyPath()
	if got, want := path.Length(), 0; got != want {
		t.Errorf("unexpected length of empty path, wanted %d, got %d", want, got)
	}
}

func TestNodePath_AppendIsNondestructiveUpdate(t *testing.T) {
	path := EmptyPath()

	p1 := path.Child(1)
	p12 := p1.Child(2)
	p13 := p1.Child(3)

	if got, want := p1.String(), "[1]"; got != want {
		t.Errorf("unexpected path, wanted %s, got %s", want, got)
	}

	if got, want := p12.String(), "[1,2]"; got != want {
		t.Errorf("unexpected path, wanted %s, got %s", want, got)
	}

	if got, want := p13.String(), "[1,3]"; got != want {
		t.Errorf("unexpected path, wanted %s, got %s", want, got)
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

func TestNodePath_ToString(t *testing.T) {
	tests := []struct {
		path   NodePath
		result string
	}{
		{NodePath{}, "[]"},
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
