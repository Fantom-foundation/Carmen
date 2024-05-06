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
	"testing"
	"unsafe"

	"golang.org/x/crypto/sha3"
)

func TestHashPassThrough(t *testing.T) {
	cacheSize := 1000
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})
	var adr Address
	for i := 0; i < 2*cacheSize; i++ {
		if got, want := hasher.Hash(adr), GetHash(sha3.NewLegacyKeccak256(), adr[:]); got != want {
			t.Errorf("hashes do not match: %v != %v", got, want)
		}
		adr[i%20]++
	}
}

func TestHashPassThroughRunParallel(t *testing.T) {
	cacheSize := 100
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})
	for i := 0; i < 10_000*cacheSize; i++ {
		var addr Address
		go func(addr Address) {
			if got, want := hasher.Hash(addr), GetHash(sha3.NewLegacyKeccak256(), addr[:]); got != want {
				t.Errorf("hashes do not match: %v != %v", got, want)
			}
		}(addr)
		addr[i%20]++
	}
}

func TestHash_NonCached_PassThrough(t *testing.T) {
	cacheSize := 0
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})
	var adr Address
	adr[0]++
	if got, want := hasher.Hash(adr), GetHash(sha3.NewLegacyKeccak256(), adr[:]); got != want {
		t.Errorf("hashes do not match: %v != %v", got, want)
	}
}

func TestMemoryFootprint(t *testing.T) {
	cacheSize := 1000
	hasher := NewCachedHasher[Address](cacheSize, AddressSerializer{})

	// fully utilised cache
	var adr Address
	for i := 0; i < cacheSize; i++ {
		if got, want := hasher.Hash(adr), GetHash(sha3.NewLegacyKeccak256(), adr[:]); got != want {
			t.Errorf("hashes do not match: %v != %v", got, want)
		}
		adr[i%20]++
	}

	var h Hash
	if got, want := hasher.GetMemoryFootprint().Total(), uintptr(cacheSize)*(unsafe.Sizeof(h)+unsafe.Sizeof(adr)); got < want {
		t.Errorf("no memory foodprint provided")
	}

	if hasher.GetMemoryFootprint().GetChild("cache") == nil {
		t.Errorf("memory footprint missing")
	}

	if got, want := hasher.GetMemoryFootprint().GetChild("cache").Total(), uintptr(cacheSize)*(unsafe.Sizeof(h)+unsafe.Sizeof(adr)); got < want {
		t.Errorf("no memory foodprint provided")
	}
}
