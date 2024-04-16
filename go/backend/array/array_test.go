//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.
//

package array_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/array"
	"github.com/Fantom-foundation/Carmen/go/backend/array/pagedarray"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const (
	PageSize = 2 * 32
	PoolSize = 10
)

var emptyAddress common.Address

func getArrayFactories[I common.Identifier, V any](valSerializer common.Serializer[V], pageSize int, poolSize int) map[string]func(t *testing.T) array.Array[I, V] {
	return map[string]func(t *testing.T) array.Array[I, V]{
		"pagedArray": func(t *testing.T) array.Array[I, V] {
			arr, err := pagedarray.NewArray[I, V](t.TempDir(), valSerializer, pageSize, poolSize)
			if err != nil {
				t.Fatalf("cannot init array: %e", err)
			}
			t.Cleanup(func() {
				_ = arr.Close()
			})
			return arr
		},
	}

}

func TestArrayGetSet(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000, 12345} {
		for name, factory := range getArrayFactories[uint32, common.Address](common.AddressSerializer{}, PageSize, PoolSize) {
			t.Run(fmt.Sprintf("array %s size %d", name, size), func(t *testing.T) {
				arr := factory(t)
				flags := fill(t, arr, size)

				for index, flag := range flags {
					actual, err := arr.Get(uint32(index))
					if err != nil {
						t.Errorf("cannot get value: %e", err)
					}

					if flag && actual != common.AddressFromNumber(index) {
						t.Errorf("value should be: %d and not: %d", index, actual)
					}

					if !flag && actual != emptyAddress {
						t.Errorf("value should be empty, not: %d", actual)
					}
				}
			})
		}
	}
}

// fill inits the input array with values at random indexes.
// It returns true/false flag array saying if there should be a value at the index,
// the values will be filled with the value matching the index.
// The array will have elements at the number of indexes equal to the input size, while
// the array will have the input size, i.e. the remaining positions will be empty
func fill[I common.Identifier](t *testing.T, arr array.Array[I, common.Address], size int) []bool {
	dimension := 100 * size
	indexes := make([]bool, dimension)
	for i := 0; i < dimension; i++ {
		indexes[i] = i < size
	}

	rand.Shuffle(len(indexes), func(i, j int) { indexes[i], indexes[j] = indexes[j], indexes[i] })
	for i := 0; i < dimension; i++ {
		if indexes[i] {
			if err := arr.Set(I(i), common.AddressFromNumber(i)); err != nil {
				t.Fatalf("cannot fill array: %e", err)
			}
		}
	}

	return indexes
}
