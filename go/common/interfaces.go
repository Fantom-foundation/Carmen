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
	"io"
)

// Flusher is any type that can be flushed.
type Flusher interface {
	Flush() error
}

type FlushAndCloser interface {
	Flusher
	io.Closer
}

type MemoryFootprintProvider interface {
	GetMemoryFootprint() *MemoryFootprint
}

type Hasher[K any] interface {
	Hash(*K) uint64
}

// Iterator is an interface for standard iterator
type Iterator[K any] interface {

	//HasNext returns true if there is still at least one more item in the underlying collection.
	HasNext() bool

	//Next returns a next element in the input collection.
	Next() K
}

// Map associates keys to values
type Map[K comparable, V any] interface {

	// Get returns a value associated with the key
	Get(key K) (val V, exists bool)

	// Put associates a new value to the key.
	Put(key K, val V)

	// Remove deletes a key from the map, returning the value
	Remove(key K) (exists bool)

	// ForEach iterates all stored key/value pairs
	// It returns
	ForEach(callback func(K, V))

	// Size returns number of elements
	Size() int

	// Clear removes all data from the map
	Clear()
}

// ErrMap associates keys and values,
// and it may return error when adding, iteration or removing fails
type ErrMap[K comparable, V any] interface {

	// Get returns a value associated with the key
	Get(key K) (val V, exists bool, err error)

	// GetOrAdd either returns a value stored under input key, or it associates the input value
	// when the key is not stored yet.
	// It returns true if the key was present, or false otherwise.
	GetOrAdd(key K, newVal V) (val V, exists bool, err error)

	// Put associates a new value to the key.
	Put(key K, val V) error

	// Remove deletes a key from the map, returning the value
	Remove(key K) (exists bool, err error)

	// ForEach iterates all stored key/value pairs
	// It returns
	ForEach(callback func(K, V)) error

	// Size returns number of elements
	Size() int

	// Clear removes all data from the map
	Clear() error
}

// MapEntry wraps a map key-value par
type MapEntry[K comparable, V any] struct {
	Key K
	Val V
}

func (e MapEntry[K, V]) String() string {
	return fmt.Sprintf("Entry: %v -> %v", e.Key, e.Val)
}
