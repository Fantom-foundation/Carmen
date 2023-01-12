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

// MultiMap associates keys and values,
// and it may return error when adding, iteration or removing fails.
// Multimap may associate more values to the same key
// and also return more values for the same key
type MultiMap[K comparable, V any] interface {
	Map[K, V]

	//Add associates the input value with the key
	// it may associate more values with the same key
	Add(key K, val V)

	// RemoveAll removes all values for the give key
	RemoveAll(key K)

	// RemoveVal removes single value associated with the given key
	// It returns true if the value has existed
	RemoveVal(key K, val V) bool

	// GetAll returns all values associated with the given key
	GetAll(key K) []V
}

// ErrMultiMap associates keys and values,
// and it may return error when adding, iteration or removing fails.
// Multimap may associate more values to the same key
// and also return more values for the same key
type ErrMultiMap[K comparable, V any] interface {
	ErrMap[K, V]

	//Add associates the input value with the key
	// it may associate more values with the same key
	Add(key K, val V) error

	// RemoveVal removes single value associated with the given key
	// It returns true if the value has existed
	RemoveVal(key K, val V) (bool, error)

	// RemoveAll removes all values for the give key
	RemoveAll(key K) error

	// GetAll returns all values associated with the given key
	GetAll(key K) ([]V, error)
}

// BulkInsert is a map extension that has an extra method to fill this collection with initial key-value pairs
// This method does not assure any properties on the underlaying map
// such as uniqueness of the keys, their sort, etc. hold.
// It is expected to be used for initial insertion of data from another source
// that already has expected properties, such as uniqueness, or sort of the keys
type BulkInsert[K comparable, V any] interface {
	// BulkInsert only inserts the input key-value pairs at the map collection.
	// It should be used only to insert keys that already have properties expected from
	// the resulting map, i.e. uniqueness of the keys, potentially their order.
	// It should be used for fast filling the map from a data source that already
	// has expected properties and thus their computation by standard iterative Put
	// would be redundant.
	BulkInsert(data []MapEntry[K, V]) error

	// GetEntries returns a slice with all entries from the map.
	// If possible, it should provide direct data, not their copy, for the fastest possible access
	GetEntries() ([]MapEntry[K, V], error)
}

// BulkInsertMap is a union of ErrMap, MemoryFootprintProvider and BulkInsert
type BulkInsertMap[K comparable, V any] interface {
	ErrMap[K, V]
	ErrMultiMap[K, V]
	MemoryFootprintProvider
	BulkInsert[K, V]
}

// MapEntry wraps a map key-value par
type MapEntry[K comparable, V any] struct {
	Key K
	Val V
}

func (e MapEntry[K, V]) String() string {
	return fmt.Sprintf("Entry: %v -> %v", e.Key, e.Val)
}

// BulkInsertMapFactory creates a new BulkInsertMap with the given parameters
type BulkInsertMapFactory[K comparable, V any] func(bucket, capacity int) BulkInsertMap[K, V]
