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
				//t.Parallel()
				data := make([]uint32, 0, nums)
				for i := 0; i < nums; i++ {
					data = append(data, dis.GetNext())
				}
				// test that at least 30% of values is not the same
				slices.Sort(data)
				if data[0] == data[nums*0.3] {
					t.Errorf("random array contains too many euqal values: %v", data)
				}
			})
		}
	}
}
