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

package index_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var updateKeysSizes = []int{100}

var hashSink common.Hash

// hashWrapper wraps an instance of the hash index to have serializers and the hash index available at hand
type hashWrapper[K comparable] struct {
	serializer common.Serializer[K]
	hashIdx    *indexhash.IndexHash[K]
}

// BenchmarkHashTree benchmarks only computation of the hash for the index
func BenchmarkHashIndex(b *testing.B) {
	serializer := common.KeySerializer{}
	hw := hashWrapper[common.Key]{serializer, indexhash.NewIndexHash[common.Key](serializer)}
	for _, updateHashSize := range updateKeysSizes {
		b.Run(fmt.Sprintf("IndexHash updsteSize %d", updateHashSize), func(b *testing.B) {
			hw.benchmarkHash(b, updateHashSize)
		})
	}
}

func (hw hashWrapper[K]) benchmarkHash(b *testing.B, updateKeys int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for ii := 0; ii < updateKeys; ii++ {
			hw.hashIdx.AddKey(hw.toKey(uint32(ii)))
		}
		b.StartTimer()

		h, err := hw.hashIdx.Commit()
		if err != nil {
			b.Fatalf("Error to hash %s", err)
		}
		hashSink = h
	}
}

// toKey converts the key from an input uint32 to the generic Key
func (hw hashWrapper[K]) toKey(key uint32) K {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint32(keyBytes, key)
	return hw.serializer.FromBytes(keyBytes)
}
