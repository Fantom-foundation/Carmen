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
	"fmt"
	"sync/atomic"
)

// MissHitTrackingCache is a cache wrapping another cache while tracking
// the number of missed and existing entries.
// Ech time the method Get() of the wrapped cache is executed,
// this implementation checks whether the item was present or not, and increments respective counters.
// The report of miss/hit ration is provided as part of the memory footprint report.
type MissHitTrackingCache[K comparable, V any] struct {
	cache  Cache[K, V]
	misses atomic.Uint64
	hits   atomic.Uint64
}

// NewMissHitTrackingCache creates a new cache that is tracking
// the number of missed and existing entries.
// Actual data are stored and retrieved by wrapping the input cache.
func NewMissHitTrackingCache[K comparable, V any](cache Cache[K, V]) *MissHitTrackingCache[K, V] {
	return &MissHitTrackingCache[K, V]{
		cache: cache,
	}
}

func (c *MissHitTrackingCache[K, V]) Iterate(callback func(K, V) bool) {
	c.cache.Iterate(callback)
}

func (c *MissHitTrackingCache[K, V]) IterateMutable(callback func(K, *V) bool) {
	c.cache.IterateMutable(callback)
}

func (c *MissHitTrackingCache[K, V]) Get(key K) (V, bool) {
	v, exists := c.cache.Get(key)
	if exists {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}

	return v, exists
}

func (c *MissHitTrackingCache[K, V]) Set(key K, val V) (evictedKey K, evictedValue V, evicted bool) {
	return c.cache.Set(key, val)
}

func (c *MissHitTrackingCache[K, V]) GetOrSet(key K, val V) (current V, present bool, evictedKey K, evictedValue V, evicted bool) {
	return c.cache.GetOrSet(key, val)
}

func (c *MissHitTrackingCache[K, V]) Remove(key K) (original V, exists bool) {
	return c.cache.Remove(key)
}

func (c *MissHitTrackingCache[K, V]) Clear() {
	c.cache.Clear()
}

// GetMemoryFootprint provides the size of the cache in memory in bytes
// If V is a pointer type, it needs to provide the size of a referenced value.
// If the size is different for individual values, use GetDynamicMemoryFootprint instead.
func (c *MissHitTrackingCache[K, V]) GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint {
	mf := c.cache.GetMemoryFootprint(referencedValueSize)
	mf.SetNote(c.getHitRatioReport())
	return mf
}

// GetDynamicMemoryFootprint provides the size of the cache in memory in bytes for values,
// which reference dynamic amount of memory - like slices.
func (c *MissHitTrackingCache[K, V]) GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint {
	mf := c.cache.GetDynamicMemoryFootprint(valueSizeProvider)
	mf.SetNote(c.getHitRatioReport())
	return mf
}

// getHitRatioReport reports about the cache miss/hit ration, when the tracking of this statistic is enabled.
// When this statistics is disabled, the result of this method should not be considered.
func (c *MissHitTrackingCache[K, V]) getHitRatioReport() string {
	hits := c.hits.Load()
	misses := c.misses.Load()
	hitRatio := float32(hits) / float32(hits+misses)
	return fmt.Sprintf("(misses: %d, hits: %d, hitRatio: %f)", misses, hits, hitRatio)
}
