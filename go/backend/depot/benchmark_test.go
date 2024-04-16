//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package depot_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

// Benchmark of isolated Depots
// Use sub-benchmarks to test individual implementations with different parameters.
// The name of benchmark is in form:
// BenchmarkWrite/Depot_File_initialSize_16777216_dist_Exponential
// where "File" is used Store implementation, "16777216" is the initial amount of items
// in the Store on the benchmark start and "Exponential" is a probability distribution
// with which are items (indexes) to write chosen.
// To run the benchmark for File-based impls and 2^24 initial items use regex like:
//     go test ./backend/depot -bench=/.*File.*_16777216

// initial number of values inserted into the Depot before the benchmark
var initialSizes = []int{1 << 20, 1 << 24}

// number of values updated before each measured hash recalculation
var updateSizes = []int{100}

const branchingFactor = 32
const hashItems = 256

var sinkBytes []byte
var sinkHash common.Hash

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getDepotsFactories(b, branchingFactor, hashItems) {
		for _, initialSize := range initialSizes {
			s := fac.getDepot(b.TempDir())
			b.Run(fmt.Sprintf("Depot %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
				initDepotContent(b, s, initialSize)
				benchmarkInsert(b, s)
			})
			_ = s.Close()
		}
	}
}

func benchmarkInsert(b *testing.B, depot depot.Depot[uint32]) {
	for i := 0; i < b.N; i++ {
		err := depot.Set(uint32(i), toValue(uint32(i)))
		if err != nil {
			b.Fatalf("failed to set depot item; %s", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	for _, fac := range getDepotsFactories(b, branchingFactor, hashItems) {
		for _, initialSize := range initialSizes {
			s := fac.getDepot(b.TempDir())
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Depot %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initDepotContent(b, s, initialSize)
						initialized = true
					}
					benchmarkRead(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkRead(b *testing.B, dist common.Distribution, depot depot.Depot[uint32]) {
	for i := 0; i < b.N; i++ {
		value, err := depot.Get(dist.GetNext())
		if err != nil {
			b.Fatalf("failed to read item from depot; %s", err)
		}
		sinkBytes = value // prevent compiler to optimize it out
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, fac := range getDepotsFactories(b, branchingFactor, hashItems) {
		for _, initialSize := range initialSizes {
			s := fac.getDepot(b.TempDir())
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Depot %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initDepotContent(b, s, initialSize)
						initialized = true
					}
					benchmarkWrite(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkWrite(b *testing.B, dist common.Distribution, depot depot.Depot[uint32]) {
	for i := 0; i < b.N; i++ {
		err := depot.Set(dist.GetNext(), toValue(uint32(i)))
		if err != nil {
			b.Fatalf("failed to set depot item; %s", err)
		}
	}
}

func BenchmarkHash(b *testing.B) {
	for _, fac := range getDepotsFactories(b, branchingFactor, hashItems) {
		for _, initialSize := range initialSizes {
			s := fac.getDepot(b.TempDir())
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Depot %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initDepotContent(b, s, initialSize)
							initialized = true
						}
						benchmarkHash(b, dist, updateSize, s)
					})
				}
			}
			_ = s.Close()
		}
	}
}

func benchmarkHash(b *testing.B, dist common.Distribution, updateSize int, depot depot.Depot[uint32]) {
	for i := 0; i < b.N; i++ {
		b.StopTimer() // don't measure the update
		for ii := 0; ii < updateSize; ii++ {
			err := depot.Set(dist.GetNext(), toValue(rand.Uint32()))
			if err != nil {
				b.Fatalf("failed to set depot item; %s", err)
			}
		}
		b.StartTimer()

		hash, err := depot.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash depot; %s", err)
		}
		sinkHash = hash // prevent compiler to optimize it out
	}
}

func BenchmarkWriteAndHash(b *testing.B) {
	for _, fac := range getDepotsFactories(b, branchingFactor, hashItems) {
		for _, initialSize := range initialSizes {
			s := fac.getDepot(b.TempDir())
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Depot %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initDepotContent(b, s, initialSize)
							initialized = true
						}
						benchmarkWriteAndHash(b, dist, updateSize, s)
					})
				}
			}
			_ = s.Close()
		}
	}
}

func benchmarkWriteAndHash(b *testing.B, dist common.Distribution, updateSize int, depot depot.Depot[uint32]) {
	for i := 0; i < b.N; i++ {
		for ii := 0; ii < updateSize; ii++ {
			err := depot.Set(dist.GetNext(), toValue(rand.Uint32()))
			if err != nil {
				b.Fatalf("failed to set depot item; %s", err)
			}
		}

		hash, err := depot.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash depot; %s", err)
		}
		sinkHash = hash // prevent compiler to optimize it out
	}
}

func toValue(i uint32) []byte {
	value := make([]byte, 4+i%128)
	binary.BigEndian.PutUint32(value[:], i)
	return value
}

func initDepotContent(b *testing.B, depot depot.Depot[uint32], dbSize int) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < dbSize; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := depot.Set(uint32(i), []byte{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set depot item; %s", err)
		}
	}
	_, err := depot.GetStateHash()
	if err != nil {
		b.Fatalf("failed to get depot hash; %s", err)
	}
	b.StartTimer()
}
