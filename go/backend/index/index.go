package index

import "github.com/Fantom-foundation/Carmen/go/common"

type Index[K common.Serializable, I common.Identifier] interface {
	GetOrAdd(key K) (I, error)
	Contains(key K) bool

	GetStateHash() common.Hash
	Close() error
}
