package common

import (
	"fmt"
	"math/rand"
	"testing"
)

var isink int

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

func BenchmarkNWaysSequentialWrites(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	const SIZE = 1024
	keys := generateRandomKeys(N)
	c := NewNWaysCache[int, int](SIZE, 16)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		c.Set(keys[i%N], i)
	}
}

func BenchmarkNWaysParallelWrites(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	const SIZE = 1024
	keys := generateRandomKeys(N)
	c := NewNWaysCache[int, int](SIZE, 16)
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		pos := rand.Intn(N)
		for pb.Next() {
			pos++
			c.Set(keys[pos%N], pos)
		}
	})
}

func BenchmarkNWaysSequentialReads(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	keys := generateRandomKeys(N)
	c := NewNWaysCache[int, int](N, 16)
	for i := 0; i < N; i++ {
		c.Set(keys[i], i)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		iSink, _ = c.Get(keys[i%N])
	}
}

func BenchmarkNWaysParallelReads(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	keys := generateRandomKeys(N)
	c := NewNWaysCache[int, int](N, 16)
	for i := 0; i < N; i++ {
		c.Set(keys[i], i)
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		pos := rand.Intn(N)
		for pb.Next() {
			pos++
			isink, _ = c.Get(keys[pos%N])
		}
	})
}

func generateRandomKeys(count int) []int {
	keys := make([]int, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, rand.Intn(1024*count))
	}
	return keys
}
