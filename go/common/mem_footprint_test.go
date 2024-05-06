// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func expectSubstr(t *testing.T, str, substring string) {
	if !strings.Contains(str, substring) {
		t.Errorf("expected %v to contain substring %v", str, substring)
	}
}

func TestMemoryFootprintIsFormatable(t *testing.T) {
	fp := NewMemoryFootprint(12)
	fp.AddChild("left", NewMemoryFootprint(50*1024))
	fp.AddChild("right", NewMemoryFootprint(10*1024*1024+200*1024))

	print := fmt.Sprintf("%v", fp)
	expectSubstr(t, print, "10.2 MB .")
	expectSubstr(t, print, "50.0 KB ./left")
	expectSubstr(t, print, "10.2 MB ./right")
}

func TestMemoryFootprintContainsNote(t *testing.T) {
	fp := NewMemoryFootprint(12)
	fp.SetNote("Hello")

	if !strings.Contains(fp.String(), "Hello") {
		t.Errorf("note not printed")
	}
}

func TestMemoryFootprintValue(t *testing.T) {
	fp := NewMemoryFootprint(12)

	if got, want := fp.Value(), 12; got != uintptr(want) {
		t.Errorf("value does not match: %d != %d", got, want)
	}
}

func TestMemoryFootprint_Recursive(t *testing.T) {
	fp := NewMemoryFootprint(12)
	fp.AddChild("x", fp)

	if got, want := fp.Total(), 12; got != uintptr(want) {
		t.Errorf("value does not match: %d != %d", got, want)
	}
}

func TestMemoryFootprint_ChildNil(t *testing.T) {
	fp := NewMemoryFootprint(12)
	fp.AddChild("x", nil)

	if got, want := fp.Total(), 12; got != uintptr(want) {
		t.Errorf("value does not match: %d != %d", got, want)
	}
}

func TestMemoryFootprintPrintsComponentsInOrder(t *testing.T) {
	fp := NewMemoryFootprint(4)
	fp.AddChild("b", NewMemoryFootprint(5))
	fp.AddChild("a", NewMemoryFootprint(6))
	fp.AddChild("c", NewMemoryFootprint(7))

	print := fmt.Sprintf("%v", fp)
	match, err := regexp.MatchString(`6.*a[\S\s]*5.*b[\S\s]*7.*c`, print)
	if err != nil {
		t.Fatalf("Failed to match regex: %v", err)
	}
	if !match {
		t.Errorf("Entries not in order:\n%v", print)
	}
}

func TestMemoryFootprintPrintsDataInPostfixOrder(t *testing.T) {
	fp := NewMemoryFootprint(4)
	fp.AddChild("a", NewMemoryFootprint(5))

	print := fmt.Sprintf("%v", fp)
	match, err := regexp.MatchString(`5.*a[\S\s]*9.*\.`, print)
	if err != nil {
		t.Fatalf("Failed to match regex: %v", err)
	}
	if !match {
		t.Errorf("Parent is not after componentes:\n%v", print)
	}
}
func TestMemoryFootprintPrintIsAligned(t *testing.T) {
	fp := NewMemoryFootprint(12)
	fp.AddChild("a", NewMemoryFootprint(1))
	fp.AddChild("b", NewMemoryFootprint(10*1024))
	fp.AddChild("c", NewMemoryFootprint(100*1024*1024+200*1024))
	fp.AddChild("d", NewMemoryFootprint(1022))

	print := fmt.Sprintf("%v", fp)
	expectSubstr(t, print, " 100.2 MB .")
	expectSubstr(t, print, "   1.0  B ./a")
	expectSubstr(t, print, "  10.0 KB ./b")
	expectSubstr(t, print, " 100.2 MB ./c")
	expectSubstr(t, print, "1022.0  B ./d")
}
