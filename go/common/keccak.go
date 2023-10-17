package common

import (
	"sync"

	"golang.org/x/crypto/sha3"
)

var keccakHasherPool = sync.Pool{New: func() any { return sha3.NewLegacyKeccak256() }}

func Keccak256(data []byte) Hash {
	hasher := keccakHasherPool.Get().(keccakHasher)
	hasher.Reset()
	hasher.Write(data)
	var res Hash
	hasher.Read(res[:])
	keccakHasherPool.Put(hasher)
	return res
}

type keccakHasher interface {
	Reset()
	Write(in []byte) (int, error)
	Read(out []byte) (int, error)
}
