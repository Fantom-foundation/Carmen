//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package indexhash

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// KeyBufferInitialCapacity is a capacity of dirty keys list to prevent frequent allocations
const KeyBufferInitialCapacity = 1000

// IndexHash is a cache of accumulated keys and their recursive hash. It is expected that it stores the keys that are
// not yet included in the hash. The keys shall be cleared and the hash updated once in a while. The hash is set to
// 32 byte empty array 0x0...0 at first. Then the keys are being added into the structure. Upon a request, the keys are
// used to compute a recursive hash as  H = H1(H2(H3(..., key3), key2), key1) and the list of keys is cleared.
// This process repeats as many times as needed.
type IndexHash[K comparable] struct {
	hash       common.Hash
	keys       []K
	serializer common.Serializer[K]
}

// NewIndexHash initialises with empty hash and empty keys.
func NewIndexHash[K comparable](serializer common.Serializer[K]) *IndexHash[K] {
	return &IndexHash[K]{
		hash:       common.Hash{},
		keys:       make([]K, 0, KeyBufferInitialCapacity),
		serializer: serializer,
	}
}

// InitIndexHash creates a new instance with the initial hash
func InitIndexHash[K comparable](hash common.Hash, serializer common.Serializer[K]) *IndexHash[K] {
	return &IndexHash[K]{
		hash:       hash,
		keys:       make([]K, 0, KeyBufferInitialCapacity),
		serializer: serializer,
	}
}

// AddKey accumulates a key to be hashed as part of the commit.
func (hi *IndexHash[K]) AddKey(key K) {
	hi.keys = append(hi.keys, key)
}

// Commit computes recursively hash of the accumulated keys and clears the accumulated keys.
// The hash is computed as h := hash(h, key) for all keys.
func (hi *IndexHash[K]) Commit() (common.Hash, error) {
	h := sha256.New()
	hashTmp := hi.hash[:]
	for _, key := range hi.keys {
		h.Reset()

		_, err := h.Write(hashTmp)
		if err != nil {
			return common.Hash{}, err
		}

		_, err = h.Write(hi.serializer.ToBytes(key))
		if err != nil {
			return common.Hash{}, err
		}

		hashTmp = h.Sum(nil)
	}

	hi.hash = *(*common.Hash)(hashTmp)
	hi.keys = hi.keys[0:0]

	return hi.hash, nil
}

func (hi *IndexHash[K]) Clear() {
	hi.hash = common.Hash{}
	hi.keys = hi.keys[0:0]
}

// GetMemoryFootprint provides the size of the structure in memory in bytes
func (hi *IndexHash[K]) GetMemoryFootprint() *common.MemoryFootprint {
	var k K
	return common.NewMemoryFootprint(unsafe.Sizeof(*hi) + uintptr(len(hi.keys))*unsafe.Sizeof(k))
}
