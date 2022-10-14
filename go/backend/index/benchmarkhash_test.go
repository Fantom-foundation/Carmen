package index_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index/hashindex"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

// var updateKeysSizes = []int{0x1p20, 0x1p24, 0x1p30}
var updateKeysSizes = []int{0x1p20, 0x1p24}

var hashSink interface{}

// hashWrapper wraps an instance of the hash index to have serializers and the hash index available at hand
type hashWrapper[K comparable] struct {
	serializer common.Serializer[K]
	hashIdx    *hashindex.HashIndex[K]
}

// BenchmarkHashTree benchmarks only computation of the hash for the index
func BenchmarkHashIndex(b *testing.B) {
	serializer := common.KeySerializer{}
	hw := hashWrapper[common.Key]{serializer, hashindex.NewHashIndex[common.Key](serializer)}
	for _, updateHashSize := range updateKeysSizes {
		b.Run(fmt.Sprintf("HashIndex updsteSize %d", updateHashSize), func(b *testing.B) {
			hw.benchmarkHash(b, updateHashSize)
		})
	}
}

func (hw hashWrapper[K]) benchmarkHash(b *testing.B, updateKeys int) {
	b.StopTimer()
	for i := 0; i < updateKeys; i++ {
		hw.hashIdx.AddKey(hw.toKey(uint32(i)))
	}
	b.StartTimer()

	h, err := hw.hashIdx.Commit()
	if err != nil {
		b.Fatalf("Error to hash %s", err)
	}
	hashSink = h
}

// toKey converts the key from an input uint32 to the generic Key
func (hw hashWrapper[K]) toKey(key uint32) K {
	keyBytes := binary.BigEndian.AppendUint32([]byte{}, key)
	return hw.serializer.FromBytes(keyBytes)
}
