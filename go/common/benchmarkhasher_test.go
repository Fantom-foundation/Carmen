// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"crypto/sha256"
	"fmt"
	"hash/maphash"
	"testing"
)

var (
	globalHash = sha256.New()
	sink       []byte
	intSink    uint64
)

// numBytes upper bound of number of bytes to hash
const numBytes = 1 << 21

// numKeys upper bound of number of 32B keys to hash
const numKeys = 1 << 12

// step expressed as 2^n exponent
const step = 3

// BenchmarkNewHashEveryLoop hashes a number of bytes and creates a new hash every loop
func BenchmarkNewHashEveryLoop(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("NewHashEveryLoop dataSize %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				h := sha256.New()
				h.Write(data)
				sink = h.Sum(nil)
			}
		})
	}
}

// BenchmarkOneHashAllLoops hashes a number of bytes and creates one local hasher for all loops
func BenchmarkOneHashAllLoops(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		localHash := sha256.New()
		b.Run(fmt.Sprintf("OneHashAllLoops dataSize %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				localHash.Reset()
				localHash.Write(data)
				sink = localHash.Sum(nil)
			}
		})
	}
}

// BenchmarkOneGlobalHash hashes a number of bytes and uses a global variable hasher
func BenchmarkOneGlobalHash(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("OneGlobalHash dataSize %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				globalHash.Reset()
				globalHash.Write(data)
				sink = globalHash.Sum(nil)
			}
		})
	}
}

// BenchmarkHashKeyChain hashes a number of keys
func BenchmarkHashKeyChain(b *testing.B) {
	for i := 1; i <= numKeys; i = i << step {
		keys := make([]Key, i)
		hash := make([]byte, len(Hash{}))
		localHash := sha256.New()
		b.Run(fmt.Sprintf("HashKeyChain dataSize %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				for _, key := range keys {
					localHash.Reset()
					localHash.Write(hash)
					localHash.Write(key[:])
					hash = localHash.Sum(nil)
				}
				sink = hash
			}
		})
	}
}

// BenchmarkHashKeyChain hashes a number of keys
func BenchmarkKeccak256(b *testing.B) {
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("Keccak256 dataSize: %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				hash := GetKeccak256Hash(data)
				sink = hash[:]
			}
		})
	}
}

// BenchmarkMapHash hashes a number of bytes testing performance of a map (non-cryptographical) hash
func BenchmarkMapHash(b *testing.B) {
	hashSeed := maphash.MakeSeed()
	for i := 1; i <= numBytes; i = i << step {
		data := make([]byte, i)
		b.Run(fmt.Sprintf("MapHash EveryLoop dataSize: %d", i), func(b *testing.B) {
			for i := 1; i <= b.N; i++ {
				var h maphash.Hash
				h.SetSeed(hashSeed)
				_, _ = h.Write(data)
				hash := h.Sum64()
				intSink = hash
			}
		})
	}
}

// BenchmarkMapHashFromBytes hashes a map (non-cryptographical) hash using the build-in library
func BenchmarkMapHashAllBytes(b *testing.B) {
	key := KeySerializer{}
	data := GetKeccak256Hash(key.ToBytes(Key{})).ToBytes()
	hashSeed := maphash.MakeSeed()

	for i := 1; i <= b.N; i++ {
		var h maphash.Hash
		h.SetSeed(hashSeed)
		_, _ = h.Write(data)
		hash := h.Sum64()
		intSink = hash
	}
}

func BenchmarkMapHashComputeEach32Byte(b *testing.B) {
	key := KeySerializer{}
	data := GetKeccak256Hash(key.ToBytes(Key{}))

	for j := 1; j <= b.N; j++ {

		hash := uint64(17)
		hash = hash*prime + uint64(data[0])
		hash = hash*prime + uint64(data[1])
		hash = hash*prime + uint64(data[2])
		hash = hash*prime + uint64(data[3])
		hash = hash*prime + uint64(data[4])
		hash = hash*prime + uint64(data[5])
		hash = hash*prime + uint64(data[6])
		hash = hash*prime + uint64(data[7])
		hash = hash*prime + uint64(data[8])
		hash = hash*prime + uint64(data[9])
		hash = hash*prime + uint64(data[10])
		hash = hash*prime + uint64(data[11])
		hash = hash*prime + uint64(data[12])
		hash = hash*prime + uint64(data[13])
		hash = hash*prime + uint64(data[14])
		hash = hash*prime + uint64(data[15])
		hash = hash*prime + uint64(data[16])
		hash = hash*prime + uint64(data[17])
		hash = hash*prime + uint64(data[18])
		hash = hash*prime + uint64(data[19])
		hash = hash*prime + uint64(data[20])
		hash = hash*prime + uint64(data[21])
		hash = hash*prime + uint64(data[22])
		hash = hash*prime + uint64(data[23])
		hash = hash*prime + uint64(data[24])
		hash = hash*prime + uint64(data[25])
		hash = hash*prime + uint64(data[26])
		hash = hash*prime + uint64(data[27])
		hash = hash*prime + uint64(data[28])
		hash = hash*prime + uint64(data[29])
		hash = hash*prime + uint64(data[30])
		hash = hash*prime + uint64(data[31])

		intSink = hash
	}
}

func BenchmarkMapHashCompute32BytesInLoop(b *testing.B) {
	key := KeySerializer{}
	data := GetKeccak256Hash(key.ToBytes(Key{}))

	for j := 1; j <= b.N; j++ {

		hash := uint64(17)
		for i := 0; i < 32; i++ {
			hash = hash*prime + uint64(data[i])
		}

		intSink = hash
	}
}

func BenchmarkMapHashComputeEach32ByteShifts(b *testing.B) {
	key := KeySerializer{}
	data := GetKeccak256Hash(key.ToBytes(Key{})).ToBytes()

	for j := 1; j <= b.N; j++ {

		a := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
			uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56

		b := uint64(data[8]) | uint64(data[9])<<8 | uint64(data[10])<<16 | uint64(data[11])<<24 |
			uint64(data[12])<<32 | uint64(data[13])<<40 | uint64(data[14])<<48 | uint64(data[15])<<56

		c := uint64(data[16]) | uint64(data[17])<<8 | uint64(data[18])<<16 | uint64(data[19])<<24 |
			uint64(data[20])<<32 | uint64(data[21])<<40 | uint64(data[22])<<48 | uint64(data[23])<<56

		d := uint64(data[24]) | uint64(data[25])<<8 | uint64(data[26])<<16 | uint64(data[27])<<24 |
			uint64(data[28])<<32 | uint64(data[29])<<40 | uint64(data[30])<<48 | uint64(data[31])<<56

		hash := uint64(17)
		hash = hash*prime + a
		hash = hash*prime + b
		hash = hash*prime + c
		hash = hash*prime + d

		intSink = hash
	}
}
