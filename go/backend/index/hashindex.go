package index

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type HashIndex[K comparable] struct {
	hash       []byte
	keys       []K
	serializer common.Serializer[K]
}

func NewHashIndex[K comparable](serializer common.Serializer[K]) *HashIndex[K] {
	return &HashIndex[K]{
		hash:       []byte{},
		keys:       []K{},
		serializer: serializer,
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
	hashTmp := hi.hash
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

	hi.hash = hashTmp
	hi.keys = []K{}

	return common.BytesToHash(hi.hash), nil
}
