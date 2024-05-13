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
	"slices"
	"testing"
)

func TestDistribution_ReturnRandValues(t *testing.T) {
	const nums = 1000
	for _, disType := range []distribution{Sequential, Uniform, Exponential} {
		for _, size := range []int{nums, 2 * nums, 5 * nums} {
			dis := disType.GetDistribution(size)
			t.Run(fmt.Sprintf("distribution_%s_%d", dis.Label, size), func(t *testing.T) {
				dis := dis
				t.Parallel()
				data := make([]uint32, 0, nums)
				for i := 0; i < nums; i++ {
					data = append(data, dis.GetNext())
				}
				// test that at least 30% of values is not the same
				slices.Sort(data)
				window := int(nums * 0.3)
				for i := 0; i < len(data)-window; i++ {
					if data[i] == data[i+window] {
						t.Errorf("random array contains too many euqal values: %v", data)
					}
				}
			})
		}
	}
}
