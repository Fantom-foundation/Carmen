package common

/*
#include "keccak.h"
*/
import "C"

import (
	"sync"
	"unsafe"

	"golang.org/x/crypto/sha3"
)

func Keccak256(data []byte) Hash {
	return keccak256Cpp(data)
}

var keccakHasherPool = sync.Pool{New: func() any { return sha3.NewLegacyKeccak256() }}

func keccak256Go(data []byte) Hash {
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

var emptyKeccak256Hash = keccak256Go([]byte{})

func keccak256Cpp(data []byte) Hash {

	if len(data) == 0 {
		return emptyKeccak256Hash
	}
	res := C.carmen_keccak256(unsafe.Pointer(&data[0]), C.size_t(len(data)))
	return Hash(res)
}
