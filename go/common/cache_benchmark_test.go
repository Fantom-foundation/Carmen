package common

import (
	"fmt"
	"testing"
)

var iSink int

func BenchmarkIntIdMissLatency(b *testing.B) {
	cacheSize := 100_000
	for name, c := range initCaches(cacheSize) {
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			b.StopTimer()
			for i := 0; i < cacheSize; i++ {
				c.Set(i, i)
			}
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				var exists bool
				if iSink, exists = c.Get(i + cacheSize); exists {
					b.Fatalf("value should not be in the cache")
				}
			}
		})
	}
}

func BenchmarkIntIdHitLatency(b *testing.B) {
	cacheSize := 100_000
	for name, c := range initCaches(cacheSize) {
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			b.StopTimer()
			for i := 0; i < cacheSize; i++ {
				c.Set(i, i)
			}
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				var exists bool
				if iSink, exists = c.Get(i % cacheSize); !exists {
					b.Fatalf("value should be in the cache")
				}
			}

		})
	}
}

func BenchmarkIntIdCacheEvictions(b *testing.B) {
	cacheSize := 100_000 // there will be millions iterations - i.e. evictions will happen
	for name, c := range initCaches(cacheSize) {
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.Set(i, i)
			}
		})
	}
}
