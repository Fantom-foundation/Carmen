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
	"encoding/binary"
	"golang.org/x/exp/rand"
	"testing"
)

var hashSink Hash

func BenchmarkHashNoCache(b *testing.B) {
	hasher := NewCachedHasher[Address](0, AddressSerializer{})
	for i := 1; i <= b.N; i++ {
		var addr Address
		addr[i%20]++
		hashSink = hasher.Hash(addr)
	}
}

func BenchmarkHashNoCacheRunParallel(b *testing.B) {
	hasher := NewCachedHasher[Address](0, AddressSerializer{})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var addr Address
			hashSink = hasher.Hash(addr)
		}
	})
}

func BenchmarkAddressHitLatency(b *testing.B) {
	b.StopTimer()
	cacheSize := 1000
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})
	addresses := make([]Address, 0, cacheSize)
	for i := 0; i < cacheSize; i++ {
		arr := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		var addr Address
		copy(addr[:], arr)
		hasher.Hash(addr) // warm-up cache
		addresses = append(addresses, addr)
	}
	b.StartTimer()

	// all addresses will be in the cache (after 'cacheSize' iterations)
	for i := 1; i <= b.N; i++ {
		hashSink = hasher.Hash(addresses[i%cacheSize])
	}
}

func BenchmarkAddressMissLatency(b *testing.B) {
	address := Address{}
	hasher := NewCachedHasher[Address](1024, AddressSerializer{})
	for i := 1; i <= b.N; i++ {
		binary.BigEndian.PutUint64(address[:], uint64(i))
		hashSink = hasher.Hash(address)
	}
}

func BenchmarkKeyHitLatency(b *testing.B) {
	b.StopTimer()
	cacheSize := 1000
	hasher := NewCachedHasher[Key](cacheSize, KeySerializer{})
	keys := make([]Key, 0, cacheSize)
	for i := 0; i < cacheSize; i++ {
		arr := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		var key Key
		copy(key[:], arr)
		hasher.Hash(key) // warm-up cache
		keys = append(keys, key)
	}
	b.StartTimer()

	// all keys will be in the cache (after 'cacheSize' iterations)
	for i := 1; i <= b.N; i++ {
		hashSink = hasher.Hash(keys[i%cacheSize])
	}
}

func BenchmarkKeyMissLatency(b *testing.B) {
	key := Key{}
	hasher := NewCachedHasher[Key](1024, KeySerializer{})
	for i := 1; i <= b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		hashSink = hasher.Hash(key)
	}
}

func BenchmarkAddressesHitsRunParallel(b *testing.B) {
	b.StopTimer()
	cacheSize := 1000
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})
	addresses := make([]Address, 0, cacheSize)
	for i := 0; i < b.N; i++ {
		arr := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		var addr Address
		copy(addr[:], arr)
		hasher.Hash(addr) // warm-up cache
		addresses = append(addresses, addr)
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		var pos int
		for pb.Next() {
			hashSink = hasher.Hash(addresses[pos])
			pos++
		}
	})
}

func BenchmarkAddressesMissRunParallel(b *testing.B) {
	hasher := NewCachedHasher[Address](1024, AddressSerializer{})
	b.RunParallel(func(pb *testing.PB) {
		pos := rand.Int63()
		var address Address
		for pb.Next() {
			binary.BigEndian.PutUint64(address[:], uint64(pos))
			hashSink = hasher.Hash(address)
			pos++
		}
	})
}
