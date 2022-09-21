package store

import "github.com/Fantom-foundation/Carmen/go/common"

type Store[I common.Identifier, V common.Serializable] interface {
	Set(id I, value V) error
	Get(id I) V
	Contains(id I) bool

	GetStateHash() common.Hash
	Close() error
}
