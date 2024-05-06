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
	"errors"
	"fmt"
	"testing"
)

func TestConstError_IsError(t *testing.T) {
	var _ error = ConstError("bla")
}

func TestConstError_CanBeTestedForWithErrorsIs(t *testing.T) {
	target := ConstError("target")
	tests := []struct {
		err            error
		containsTarget bool
	}{
		{nil, false},
		{target, true},
		{fmt.Errorf("unrelated"), false},
		{fmt.Errorf("unrelated"), false},
		{fmt.Errorf("%w: detail", target), true},
		{fmt.Errorf("%w: more detail", fmt.Errorf("%w: detail", target)), true},
		{errors.Join(), false},
		{errors.Join(target), true},
		{errors.Join(fmt.Errorf("unrelated")), false},
		{errors.Join(target, fmt.Errorf("unrelated")), true},
		{errors.Join(fmt.Errorf("unrelated"), target), true},
	}

	for _, test := range tests {
		if want, got := test.containsTarget, errors.Is(test.err, target); want != got {
			t.Errorf("unexpected result for %v, wanted %t, got %t", test.err, want, got)
		}
	}
}
