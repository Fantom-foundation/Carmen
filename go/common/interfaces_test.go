package common

import "testing"

func Test_MapEntry_String(t *testing.T) {
	e := MapEntry[int, int]{10, 20}

	if got, want := e.String(), "Entry: 10 -> 20"; got != want {
		t.Errorf("provided string does not match: %s != %s", got, want)
	}
}
