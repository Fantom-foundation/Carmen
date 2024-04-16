//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package file

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// running all these options takes long, run with a longer timeout, e.g. " -timeout 360m"
var numItems = []int{1 << 23, 1 << 24, 1 << 25}

// BenchmarkInsertOneByOne inserts many keys in the index key by key.
func BenchmarkInsertOneByOne(b *testing.B) {
	for _, items := range numItems {
		keys := genRandKeys[uint32](items)
		b.Run(fmt.Sprintf("items %d", items), func(b *testing.B) {
			idx := createFileIndex[common.SlotIdxKey[uint32], uint32](b, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
			for i := 0; i < b.N; i++ {
				for _, key := range keys {
					if _, err := idx.GetOrAdd(key); err != nil {
						b.Fatalf("cannot insert key: %s", err)
					}
				}
			}
		})
	}
}

// BenchmarkBulkInsert performs insert of keys by batches
func BenchmarkBulkInsert(b *testing.B) {
	for _, items := range numItems {
		keys := genRandKeys[uint32](items)
		b.Run(fmt.Sprintf("items %d", items), func(b *testing.B) {
			idx := createFileIndex[common.SlotIdxKey[uint32], uint32](b, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
			for i := 0; i < b.N; i++ {
				if err := idx.bulkInsert(keys); err != nil {
					b.Fatalf("cannot insert key: %s", err)
				}
			}
		})
	}

}

func nextRandKey[I common.Identifier]() common.SlotIdxKey[I] {
	nextIndex := rand.Uint32()
	var nextKey common.Key
	return common.SlotIdxKey[I]{AddressIdx: I(nextIndex), Key: nextKey}
}

func genRandKeys[I common.Identifier](size int) []common.SlotIdxKey[I] {
	keys := make([]common.SlotIdxKey[I], 0, size)
	for numItem := 0; numItem < size; numItem++ {
		keys = append(keys, nextRandKey[I]())
	}
	return keys
}

func createFileIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) *Index[K, I] {
	idx, err := NewIndex[K, I](b.TempDir(), keySerializer, indexSerializer, hasher, comparator)
	if err != nil {
		b.Fatalf("failed to init file index; %s", err)
	}
	b.Cleanup(func() {
		if err := idx.Close(); err != nil {
			b.Fatalf("cannot close index: %s", err)
		}
	})

	return idx
}
