package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// State interfaces provides access to accounts and smart contract storage mmemory
type State interface {

	// GetBalance provides balance for the input account address
	GetBalance(address common.Address) (common.Balance, error)

	// SetBalance provides balance for the input account address
	SetBalance(address common.Address, balance common.Balance) error

	// GetNonce returns nonce of the account for the  input account address
	GetNonce(address common.Address) (common.Nonce, error)

	// SetNonce updates nonce of the account for the  input account address
	SetNonce(address common.Address, nonce common.Nonce) error

	// GetStorage returns the memory slot for the account address (i.e. the contract) and the memory location key
	GetStorage(address common.Address, key common.Key) (common.Value, error)

	// SetStorage updates the memory slot for the account address (i.e. the contract) and the memory location key
	SetStorage(address common.Address, key common.Key, value common.Value) error

	// GetHash hashes the storage
	GetHash() (common.Hash, error)
}

// Service manages dependencies to other interfaces to build this service
type Service[I common.Identifier] struct {
	addressIndex index.Index[common.Address, I]
	valueIndex   index.Index[common.Value, I]
	slotIndex    index.Index[common.Address, I]
	nonces       store.Store[I, common.Nonce]
	balances     store.Store[I, common.Balance]
	storage      store.Store[I, common.Value]
}

func (s *Service[I]) GetBalance(address common.Address) (balance common.Balance, err error) {
	var idx I
	if idx, err = s.addressIndex.GetOrAdd(address); err == nil {
		balance = s.balances.Get(idx)
	}
	return
}

func (s *Service[I]) SetBalance(address common.Address, balance common.Balance) (err error) {
	var idx I
	if idx, err = s.addressIndex.GetOrAdd(address); err == nil {
		err = s.balances.Set(idx, balance)
	}
	return
}

func (s *Service[I]) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	var idx I
	if idx, err = s.addressIndex.GetOrAdd(address); err == nil {
		nonce = s.nonces.Get(idx)
	}
	return
}

func (s *Service[I]) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	var idx I
	if idx, err = s.addressIndex.GetOrAdd(address); err == nil {
		err = s.nonces.Set(idx, nonce)
	}
	return
}

//
//func (s *Service[K, I, V]) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
//	var idx I
//	if idx, err = s.addressIndex.GetOrAdd(address); err == nil {
//		nonce = s.nonces.Get(idx)
//	}
//	return
//}
//
//func (s *Service[K, I, V]) SetStorage(address common.Address, key common.Key, value common.Value) (err error) {
//
//}
//
//func (s *Service[K, I, V]) GetHash() (hash common.Hash, err error) {
//
//}
