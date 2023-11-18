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

func keccak256AddressCpp(addr Address) Hash {
	return Hash(C.carmen_keccak256_20byte(
		C.uint64_t(
			uint64(addr[7])<<56|uint64(addr[6])<<48|uint64(addr[5])<<40|uint64(addr[4])<<32|
				uint64(addr[3])<<24|uint64(addr[2])<<16|uint64(addr[1])<<8|uint64(addr[0])<<0),
		C.uint64_t(
			uint64(addr[15])<<56|uint64(addr[14])<<48|uint64(addr[13])<<40|uint64(addr[12])<<32|
				uint64(addr[11])<<24|uint64(addr[10])<<16|uint64(addr[9])<<8|uint64(addr[8])<<0),
		C.uint32_t(
			uint64(addr[19])<<24|uint64(addr[18])<<16|uint64(addr[17])<<8|uint64(addr[16])<<0),
	))
	//return Hash(C.carmen_keccak256_20byte(unsafe.Pointer(&addr)))
}
