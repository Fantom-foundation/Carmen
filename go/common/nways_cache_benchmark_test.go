package common

import (
	"fmt"
	"testing"
)

func BenchmarkNWaysEvictions(b *testing.B) {
	cacheSize := 100_000
	for ways := 2; ways < 128; ways *= 2 {
		c := NewNWaysCache[int, int](cacheSize, ways)
		b.Run(fmt.Sprintf("N-ways cache N %d", ways), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.Set(i, i)
			}
		})
	}
}
