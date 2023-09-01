package common

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkNWaysEvictions(b *testing.B) {
	cacheSize := 100_000
	for ways := 2; ways <= 32; ways *= 2 {
		c := NewNWaysCache[int, int](cacheSize, ways)
		b.Run(fmt.Sprintf("N-ways cache N %d", ways), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.Set(i, i)
			}
		})
	}
}

func BenchmarkNWaysParallelWrites(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	const SIZE = 1024
	keys := generateRandomKeys(N)
	b.StartTimer()

	for ways := 2; ways <= 32; ways *= 2 {
		c := NewNWaysCache[int, int](SIZE, ways)
		b.Run(fmt.Sprintf("ways %d", ways), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				pos := rand.Intn(N)
				for pb.Next() {
					pos++
					c.Set(keys[pos%N], pos)
				}
			})
		})
	}
}

func BenchmarkNWaysParallelReads(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	keys := generateRandomKeys(N)
	b.StartTimer()

	for ways := 2; ways <= 32; ways *= 2 {
		b.StopTimer()
		c := NewNWaysCache[int, int](N, ways)
		for i := 0; i < N; i++ {
			c.Set(keys[i], i)
		}
		b.StartTimer()
		b.Run(fmt.Sprintf("ways %d", ways), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				pos := rand.Intn(N)
				for pb.Next() {
					pos++
					iSink, _ = c.Get(keys[pos%N])
				}
			})
		})
	}
}
