package index

import "github.com/Fantom-foundation/Carmen/go/common"

// Index is an append-only index for a set of values, mapping each added
// new element to a unique ordinal number.
//
// The type parameter K, the key type, can be any type that can
// be hashed and compared. The type I is the type used for the
// ordinal numbers.
type Index[K comparable, I common.Identifier] interface {
	GetOrAdd(key K) (I, error)
	Contains(key K) bool

	GetStateHash() common.Hash
	Close() error
}
