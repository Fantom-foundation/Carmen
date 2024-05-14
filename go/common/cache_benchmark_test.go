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
	"math/rand"
	"testing"
)

var iSink int

func BenchmarkCacheMissLatency(b *testing.B) {
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

func BenchmarkCacheHitLatency(b *testing.B) {
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

func BenchmarkCacheEvictions(b *testing.B) {
	cacheSize := 100_000 // there will be millions iterations - i.e. evictions will happen
	for name, c := range initCaches(cacheSize) {
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.Set(i, i)
			}
		})
	}
}

func BenchmarkCacheSingleThreadWrites(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	const SIZE = 1024
	keys := generateRandomKeys(N)
	b.StartTimer()

	for name, c := range initCaches(SIZE) {
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.Set(keys[i%N], i)
			}
		})
	}
}

func BenchmarkCacheSingleThreadReads(b *testing.B) {
	b.StopTimer()
	const N = 100_000
	keys := generateRandomKeys(N)
	b.StartTimer()

	for name, c := range initCaches(N) {
		b.StopTimer()
		for i := 0; i < N; i++ {
			c.Set(keys[i], i)
		}
		b.StartTimer()
		b.Run(fmt.Sprintf("cache %s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				iSink, _ = c.Get(keys[i%N])
			}
		})
	}
}

func generateRandomKeys(count int) []int {
	keys := make([]int, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, rand.Intn(1024*count))
	}
	return keys
}
