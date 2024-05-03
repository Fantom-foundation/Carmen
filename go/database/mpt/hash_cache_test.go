//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package mpt

import (
	"math/rand"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestAddressHasher_ProducesCorrectHashes(t *testing.T) {
	hasher := NewAddressHasher()
	for _, test := range getRandomAddresses(100) {
		want := common.Keccak256(test[:])
		got, _ := hasher.Hash(test)
		if want != got {
			t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
		}
	}
}

func TestAddressHasher_HitsAndMissesAreIndictedCorretly(t *testing.T) {
	hasher := NewAddressHasher()
	if _, hit := hasher.Hash(common.Address{}); hit {
		t.Errorf("first access should not be a hit")
	}
	if _, hit := hasher.Hash(common.Address{}); !hit {
		t.Errorf("second access should be a hit")
	}
}

func TestAddressHasher_AccessesAreRaceConditionFree(t *testing.T) {
	tests := getRandomAddresses(100)
	hasher := NewAddressHasher()
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for _, test := range tests {
				want := common.Keccak256(test[:])
				got, _ := hasher.Hash(test)
				if want != got {
					t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
				}
			}
		}()
	}
	wg.Wait()
}

func TestAddressHasher_GetMemoryFootprint(t *testing.T) {
	hasher := NewAddressHasher()
	want := hashCacheSize*uint64(unsafe.Sizeof(cachedHasherEntry[common.Address]{})) + uint64(unsafe.Sizeof(genericHasher[common.Address]{}))
	got := uint64(hasher.GetMemoryFootprint().Total())
	if want != got {
		t.Errorf("unexpected size, wanted %d, got %d", want, got)
	}
}

func TestKeyHasher_ProducesCorrectHashes(t *testing.T) {
	hasher := NewKeyHasher()
	for _, test := range getRandomKeys(100) {
		want := common.Keccak256(test[:])
		got, _ := hasher.Hash(test)
		if want != got {
			t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
		}
	}
}

func TestKeyHasher_AccessesAreRaceConditionFree(t *testing.T) {
	tests := getRandomKeys(100)
	hasher := NewKeyHasher()
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for _, test := range tests {
				want := common.Keccak256(test[:])
				got, _ := hasher.Hash(test)
				if want != got {
					t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
				}
			}
		}()
	}
	wg.Wait()
}
func TestKeyHasher_GetMemoryFootprint(t *testing.T) {
	hasher := NewKeyHasher()
	want := hashCacheSize*uint64(unsafe.Sizeof(cachedHasherEntry[common.Key]{})) + uint64(unsafe.Sizeof(genericHasher[common.Key]{}))
	got := uint64(hasher.GetMemoryFootprint().Total())
	if want != got {
		t.Errorf("unexpected size, wanted %d, got %d", want, got)
	}
}

func TestNewHitMissTrackingCache_ProducesCorrectHashes(t *testing.T) {
	hasher := NewHitMissTrackingCache(NewKeyHasher())
	for _, test := range getRandomKeys(100) {
		want := common.Keccak256(test[:])
		got, _ := hasher.Hash(test)
		if want != got {
			t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
		}
	}
}

func TestNewHitMissTrackingCache_AccessesAreRaceConditionFree(t *testing.T) {
	tests := getRandomKeys(100)
	hasher := NewHitMissTrackingCache(NewKeyHasher())
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for _, test := range tests {
				want := common.Keccak256(test[:])
				got, _ := hasher.Hash(test)
				if want != got {
					t.Errorf("invalid hash of %x, wanted %x, got %x", test, want, got)
				}
			}
		}()
	}
	wg.Wait()
}

func TestNewHitMissTrackingCache_GetMemoryFootprint(t *testing.T) {
	hasher := NewHitMissTrackingCache(NewKeyHasher())
	want := hashCacheSize*uint64(unsafe.Sizeof(cachedHasherEntry[common.Key]{})) +
		uint64(unsafe.Sizeof(genericHasher[common.Key]{})) +
		uint64(unsafe.Sizeof(HitMissTrackingCachedHasher[common.Key]{}))

	fp := hasher.GetMemoryFootprint()
	got := uint64(fp.Total())
	if want != got {
		t.Errorf("unexpected size, wanted %d, got %d", want, got)
	}
	if !strings.Contains(fp.String(), "hit ratio") {
		t.Errorf("no hit ratio reported in `%s`", fp.String())
	}
}

func TestNewHitMissTrackingCache_CountsHitsAndMissesCorrectly(t *testing.T) {
	hasher := NewHitMissTrackingCache(NewAddressHasher())

	if got, want := hasher.hits.Load(), uint64(0); got != want {
		t.Errorf("initial hit counter invalid, want %d, got %d", want, got)
	}
	if got, want := hasher.misses.Load(), uint64(0); got != want {
		t.Errorf("initial miss counter invalid, want %d, got %d", want, got)
	}

	if _, hit := hasher.Hash(common.Address{}); hit {
		t.Errorf("first access should not be a hit")
	}

	if got, want := hasher.hits.Load(), uint64(0); got != want {
		t.Errorf("hit counter invalid, want %d, got %d", want, got)
	}
	if got, want := hasher.misses.Load(), uint64(1); got != want {
		t.Errorf("miss counter invalid, want %d, got %d", want, got)
	}

	if _, hit := hasher.Hash(common.Address{}); !hit {
		t.Errorf("second access should be a hit")
	}

	if got, want := hasher.hits.Load(), uint64(1); got != want {
		t.Errorf("hit counter invalid, want %d, got %d", want, got)
	}
	if got, want := hasher.misses.Load(), uint64(1); got != want {
		t.Errorf("miss counter invalid, want %d, got %d", want, got)
	}
}

func getRandomAddresses(number int) []common.Address {
	r := rand.New(rand.NewSource(99))
	list := []common.Address{}
	for i := 0; i < number; i++ {
		cur := common.Address{}
		r.Read(cur[:])
		list = append(list, cur)
	}
	return list
}

func getRandomKeys(number int) []common.Key {
	r := rand.New(rand.NewSource(99))
	list := []common.Key{}
	for i := 0; i < number; i++ {
		cur := common.Key{}
		r.Read(cur[:])
		list = append(list, cur)
	}
	return list
}

func BenchmarkHashCache_KeyHasherMiss(b *testing.B) {
	hasher := NewKeyHasher()
	key := common.Key{}
	for i := 0; i < b.N; i++ {
		key[31] = byte(i)
		key[30] = byte(i >> 8)
		key[29] = byte(i >> 16)
		key[28] = byte(i >> 24)
		_, hit := hasher.Hash(key)
		if hit {
			b.Fatalf("all accesses should be misses")
		}
	}
}

func BenchmarkHashCache_KeyHasherHit(b *testing.B) {
	hasher := NewKeyHasher()
	key := common.Key{}
	for i := 0; i < b.N; i++ {
		_, hit := hasher.Hash(key)
		if i > 0 && !hit {
			b.Fatalf("all accesses should be hits")
		}
	}
}
