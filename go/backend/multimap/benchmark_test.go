// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package multimap

import (
	"fmt"
	"testing"
)

// initial number of values inserted into the Store before the benchmark
var initialSizes = []int{1 << 20, 1 << 24, 1 << 30}

const collisions = 1 << 10 // collisions is the number of the same keys to insert
var arraySink []uint64

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getMultiMapFactories(b) {
		for _, initialSize := range initialSizes {
			s := fac.getMultiMap(b.TempDir())
			b.Run(fmt.Sprintf("MultiMap %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
				initMapContent(b, s, initialSize, collisions)

				// insert values
				for i := 0; i < b.N; i++ {
					if err := s.Add(uint32(i%collisions), uint64(i)); err != nil {
						b.Fatalf("error: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkGetAll(b *testing.B) {
	for _, fac := range getMultiMapFactories(b) {
		for _, initialSize := range initialSizes {
			s := fac.getMultiMap(b.TempDir())
			b.Run(fmt.Sprintf("MultiMap %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
				initMapContent(b, s, initialSize, collisions)

				// insert values
				for i := 0; i < b.N; i++ {
					data, err := s.GetAll(uint32(i % collisions))
					if err != nil {
						b.Fatalf("error: %v", err)
					}
					arraySink = data
				}
			})
		}
	}
}

// initMapContent inserts keys into the input map. The key to insert is
// computed by module the number of collisions
func initMapContent(b *testing.B, s MultiMap[uint32, uint64], size, collisions int) {
	b.StopTimer()
	for i := 0; i < size; i++ {
		if err := s.Add(uint32(i%collisions), uint64(i)); err != nil {
			b.Fatalf("error: %v", err)
		}
	}
	b.StartTimer()
}
