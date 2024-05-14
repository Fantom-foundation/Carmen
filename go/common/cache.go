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

// Cache implements a memory overlay for the key-value pair.
// The keys can be set and obtained from the cache. The keys
// accumulate in the cache until the cache is full, i.e. it reaches its capacity.
// When this happens, a new key causes eviction of another key.
type Cache[K any, V any] interface {
	// Get returns a value from the cache or false. If the value exists, its number of use is updated
	Get(key K) (V, bool)
	// Set associates a key to the cache.
	// If the key is already present, the value is updated and the key marked as
	// used. If the value is not present, a new entry is added to this
	// cache. This causes another entry to be removed if the cache size is exceeded.
	Set(key K, val V) (evictedKey K, evictedValue V, evicted bool)

	// Remove deletes the key from the map and returns the deleted value
	Remove(key K) (original V, exists bool)

	// GetOrSet tries to locate the input key in the cache. IF the key exists, its value is returned.
	// If the key does not exist, the input value is stored under this key.
	// When the key is stored in this cache, another key and value may be evicted.
	// This method returns true if the key was present in the cache. It also returns if another key was evicted due
	// to inserting this key.
	GetOrSet(key K, val V) (current V, present bool, evictedKey K, evictedValue V, evicted bool)

	// Iterate goes over all cached entries and calls the callback for each key-value pair in the cache.
	// When the callback returns false, iteration is interrupted.
	Iterate(callback func(K, V) bool)

	// IterateMutable goes over all cached entries and calls the callback for each key-value pair in the cache.
	// When the callback returns false, iteration is interrupted.
	// This method provides a pointer to the value allowing for its possible modifications.
	// The value should be potentially modified in-place while keeping ownership of the pointer to the callback method.
	IterateMutable(callback func(K, *V) bool)

	// Clear removes all elements from the cahce.
	Clear()

	// GetMemoryFootprint provides the size of the cache in bytes
	// If V is a pointer type, it needs to provide the size of a referenced value.
	// If the size is different for individual values, use GetDynamicMemoryFootprint instead.
	GetMemoryFootprint(referencedValueSize uintptr) *MemoryFootprint

	// GetDynamicMemoryFootprint provides the size of the cache in bytes for values.
	// It provides a callback method to calculate the size of each value individually.
	// This is useful in situations where each value has a different size, for instance, slices
	// or nested structs.
	GetDynamicMemoryFootprint(valueSizeProvider func(V) uintptr) *MemoryFootprint
}
