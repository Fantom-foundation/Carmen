// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package tribool

import (
	"fmt"
	"testing"
)

func TestTribool_TrueFalseUnknown_Categories(t *testing.T) {
	tests := []struct {
		name string
		tb   Tribool
		want struct {
			unknown  bool
			trueVal  bool
			falseVal bool
		}
	}{
		{
			name: "Unknown",
			tb:   Unknown(),
			want: struct {
				unknown  bool
				trueVal  bool
				falseVal bool
			}{true, false, false},
		},
		{
			name: "False",
			tb:   False(),
			want: struct {
				unknown  bool
				trueVal  bool
				falseVal bool
			}{false, false, true},
		},
		{
			name: "True",
			tb:   True(),
			want: struct {
				unknown  bool
				trueVal  bool
				falseVal bool
			}{false, true, false},
		},
	}

	for i := 3; i <= 0xFF; i++ {
		tests = append(tests, struct {
			name string
			tb   Tribool
			want struct {
				unknown  bool
				trueVal  bool
				falseVal bool
			}
		}{
			name: fmt.Sprintf("unknown value: %d", i),
			tb:   Tribool{value: byte(i)},
			want: struct {
				unknown  bool
				trueVal  bool
				falseVal bool
			}{true, false, false},
		})

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, want := tt.tb.Unknown(), tt.want.unknown; got != want {
				t.Errorf("unexpected tribool: got: Unknown() = %v, want %v", got, want)
			}
			if got, want := tt.tb.True(), tt.want.trueVal; got != want {
				t.Errorf("unexpected tribool: got: True() = %v, want %v", got, want)
			}
			if got, want := tt.tb.False(), tt.want.falseVal; got != want {
				t.Errorf("unexpected tribool: got: False() = %v, want %v", got, want)
			}
		})
	}
}

func TestTribool_All_non_True_False_Is_Unknown(t *testing.T) {
	for i := 3; i <= 0xFF; i++ {
		b := Tribool{value: byte(i)}
		if got, want := b.Unknown(), true; got != want {
			t.Errorf("unexpected tribool: got: %v, want %v (%d)", got, want, b.value)
		}
	}
}

func TestTribool_All_Elements_Are_Comparable(t *testing.T) {
	items := []Tribool{Unknown(), False(), True()}
	for i, a := range items {
		for j, b := range items {
			if got, want := a == b, i == j; got != want {
				t.Errorf("unexpected tribool [%v, %v] comparision: got: %v, want %v", a, b, got, want)
			}
		}
	}
}

func TestTribool_Default_Is_Unknown(t *testing.T) {
	def := Tribool{}
	if got, want := def, Unknown(); got != want {
		t.Errorf("unexpected tribool: got: %v, want %v", got, want)
	}
}

func TestTribool_Create_From_Bool(t *testing.T) {
	tests := map[string]struct {
		val  bool
		want Tribool
	}{
		"true": {
			val:  true,
			want: True(),
		},
		"false": {
			val:  false,
			want: False(),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got, want := New(tt.val), tt.want; got != want {
				t.Errorf("unexpected tribool: got: %v, want %v", got, want)
			}
		})
	}
}

func TestTriboolString(t *testing.T) {
	tests := []struct {
		name string
		tb   Tribool
		want string
	}{
		{
			name: "Unknown",
			tb:   Unknown(),
			want: "unknown",
		},
		{
			name: "False",
			tb:   False(),
			want: "false",
		},
		{
			name: "True",
			tb:   True(),
			want: "true",
		},
	}

	for i := 3; i <= 0xFF; i++ {
		tests = append(tests, struct {
			name string
			tb   Tribool
			want string
		}{
			name: "unknown value",
			tb:   Tribool{value: byte(i)},
			want: "unknown",
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, want := tt.tb.String(), tt.want; got != want {
				t.Errorf("got: String() = %v, want %v", got, want)
			}
		})
	}
}
