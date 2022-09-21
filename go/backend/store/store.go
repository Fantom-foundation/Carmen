package store

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Store is a mutable key/value store. It provides mutation/lookup support, as well as
// global state hashing support to obtain a quick hash for the entire content.
//
// The type I is the type used for the ordinal numbers,
// the type V for the store values - needs to be serializable.
type Store[I common.Identifier, V common.Serializable] interface {
	Set(id I, value V) error
	Get(id I, itemToOverride V) bool

	GetStateHash() common.Hash
	Close() error
}
