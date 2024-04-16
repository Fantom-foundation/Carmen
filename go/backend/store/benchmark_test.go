//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package store_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

// Benchmark of isolated Stores
// Use sub-benchmarks to test individual implementations with different parameters.
// The name of benchmark is in form:
// BenchmarkWrite/Store_File_initialSize_16777216_dist_Exponential
// where "File" is used Store implementation, "16777216" is the initial amount of items
// in the Store on the benchmark start and "Exponential" is a probability distribution
// with which are items (indexes) to write chosen.
// To run the benchmark for File-based impls and 2^24 initial items use regex like:
//     go test ./backend/store -bench=/.*File.*_16777216

// benchmark stores parameters (different from test stores parameters)
const (
	BmBranchingFactor = 32
	BmPageSize        = 1 << 12 // = 4 KiB
	BmPoolSize        = 100000
)

// initial number of values inserted into the Store before the benchmark
var initialSizes = []int{1 << 20, 1 << 24, 1 << 30}

// number of values updated before each measured hash recalculation
var updateSizes = []int{100}

var sinkValue common.Value
var sinkHash common.Hash

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getStoresFactories[common.Value](b, common.ValueSerializer{}, BmBranchingFactor, BmPageSize, BmPoolSize) {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b.TempDir())
			b.Run(fmt.Sprintf("Store %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
				initStoreContent(b, s, initialSize)

				for i := 0; i < b.N; i++ {
					err := s.Set(uint32(i), toValue(uint32(i)))
					if err != nil {
						b.Fatalf("failed to set store item; %s", err)
					}
				}
			})
			_ = s.Close()
		}
	}
}

func BenchmarkRead(b *testing.B) {
	for _, fac := range getStoresFactories[common.Value](b, common.ValueSerializer{}, BmBranchingFactor, BmPageSize, BmPoolSize) {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b.TempDir())
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initStoreContent(b, s, initialSize)
						initialized = true
					}

					for i := 0; i < b.N; i++ {
						value, err := s.Get(dist.GetNext())
						if err != nil {
							b.Fatalf("failed to read item from store; %s", err)
						}
						sinkValue = value // prevent compiler to optimize it out
					}
				})
			}
			_ = s.Close()
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, fac := range getStoresFactories[common.Value](b, common.ValueSerializer{}, BmBranchingFactor, BmPageSize, BmPoolSize) {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b.TempDir())
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initStoreContent(b, s, initialSize)
						initialized = true
					}

					for i := 0; i < b.N; i++ {
						err := s.Set(dist.GetNext(), toValue(uint32(i)))
						if err != nil {
							b.Fatalf("failed to set store item; %s", err)
						}
					}
				})
			}
			_ = s.Close()
		}
	}
}

func BenchmarkHash(b *testing.B) {
	for _, fac := range getStoresFactories[common.Value](b, common.ValueSerializer{}, BmBranchingFactor, BmPageSize, BmPoolSize) {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b.TempDir())
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Store %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initStoreContent(b, s, initialSize)
							initialized = true
						}

						for i := 0; i < b.N; i++ {
							b.StopTimer() // don't measure the update
							for ii := 0; ii < updateSize; ii++ {
								err := s.Set(dist.GetNext(), toValue(rand.Uint32()))
								if err != nil {
									b.Fatalf("failed to set store item; %s", err)
								}
							}
							b.StartTimer()

							hash, err := s.GetStateHash()
							if err != nil {
								b.Fatalf("failed to hash store; %s", err)
							}
							sinkHash = hash // prevent compiler to optimize it out
						}
					})
				}
			}
			_ = s.Close()
		}
	}
}

func BenchmarkWriteAndHash(b *testing.B) {
	for _, fac := range getStoresFactories[common.Value](b, common.ValueSerializer{}, BmBranchingFactor, BmPageSize, BmPoolSize) {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b.TempDir())
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Store %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initStoreContent(b, s, initialSize)
							initialized = true
						}

						for i := 0; i < b.N; i++ {
							for ii := 0; ii < updateSize; ii++ {
								err := s.Set(dist.GetNext(), toValue(rand.Uint32()))
								if err != nil {
									b.Fatalf("failed to set store item; %s", err)
								}
							}

							hash, err := s.GetStateHash()
							if err != nil {
								b.Fatalf("failed to hash store; %s", err)
							}
							sinkHash = hash // prevent compiler to optimize it out
						}
					})
				}
			}
			_ = s.Close()
		}
	}
}

func toValue(i uint32) common.Value {
	value := common.Value{}
	binary.BigEndian.PutUint32(value[:], i)
	return value
}

func initStoreContent(b *testing.B, s store.Store[uint32, common.Value], initialSize int) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < initialSize; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := s.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
	_, err := s.GetStateHash()
	if err != nil {
		b.Fatalf("failed to get store hash; %s", err)
	}
	b.StartTimer()
}
