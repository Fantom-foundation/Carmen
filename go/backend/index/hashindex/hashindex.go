package hashindex

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// KeysBufferCap is a capacity of dirty keys list to prevent frequent allocations
const KeysBufferCap = 1000

// HashIndex is a cache of accumulated keys and their recursive hash. It is expected that it stores the keys that are
// not yet included in the hash. The keys shall be cleared and the hash updated once in a while. The hash is set to
// 32 byte empty array 0x0...0 at first. Then the keys are being added into the structure. Upon a request, the keys are
// used to compute a recursive hash as  H = H1(H2(H3(..., key3), key2), key1) and the list of keys is cleared.
// This process repeats as many times as needed.
type HashIndex[K comparable] struct {
	hash           common.Hash
	keys           []K
	serializer     common.Serializer[K]
	hashSerializer common.HashSerializer
}

// NewHashIndex initialises with empty hash and empty keys.
func NewHashIndex[K comparable](serializer common.Serializer[K]) *HashIndex[K] {
	return &HashIndex[K]{
		hash:           common.Hash{},
		keys:           make([]K, 0, KeysBufferCap),
		serializer:     serializer,
		hashSerializer: common.HashSerializer{},
	}
}

// InitHashIndex creates a new instance with the initial hash
func InitHashIndex[K comparable](hash common.Hash, serializer common.Serializer[K]) *HashIndex[K] {
	return &HashIndex[K]{
		hash:           hash,
		keys:           make([]K, 0, KeysBufferCap),
		serializer:     serializer,
		hashSerializer: common.HashSerializer{},
	}
}

// AddKey accumulates a key to be hashed as part of the commit.
func (hi *HashIndex[K]) AddKey(key K) {
	hi.keys = append(hi.keys, key)
}

// Commit computes recursively hash of the accumulated keys and clears the accumulated keys.
// The hash is computed as h := hash(h, key) for all keys.
func (hi *HashIndex[K]) Commit() (common.Hash, error) {
	h := sha256.New()
	hashTmp := hi.hashSerializer.ToBytes(hi.hash)
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

	hi.hash = hi.hashSerializer.FromBytes(hashTmp)
	hi.keys = make([]K, 0, KeysBufferCap)

	return hi.hash, nil
}
