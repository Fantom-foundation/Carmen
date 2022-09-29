package state

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// State interfaces provides access to accounts and smart contract values memory
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

	// GetHash hashes the values
	GetHash() (common.Hash, error)
}

// Service manages dependencies to other interfaces to build this service
type Service[I common.Identifier] struct {
	addressIndex   index.Index[common.Address, I]
	keyIndex       index.Index[common.Key, I]
	slotIndex      index.Index[common.SlotIdx[I], I]
	noncesStore    store.Store[I, common.Nonce]
	balancesStore  store.Store[I, common.Balance]
	valuesStore    store.Store[I, common.Value]
	hashSerializer common.HashSerializer
}

// New creates a new instance of this service
func New[I common.Identifier](
	addressIndex index.Index[common.Address, I],
	keyIndex index.Index[common.Key, I],
	slotIndex index.Index[common.SlotIdx[I], I],
	noncesStore store.Store[I, common.Nonce],
	balancesStore store.Store[I, common.Balance],
	valuesStore store.Store[I, common.Value]) *Service[I] {

	return &Service[I]{addressIndex, keyIndex, slotIndex, noncesStore, balancesStore, valuesStore, common.HashSerializer{}}
}

func (s *Service[I]) GetBalance(address common.Address) (balance common.Balance, err error) {
	if idx, err := s.addressIndex.GetOrAdd(address); err == nil {
		balance, err = s.balancesStore.Get(idx)
	}
	return
}

func (s *Service[I]) SetBalance(address common.Address, balance common.Balance) (err error) {
	if idx, err := s.addressIndex.GetOrAdd(address); err == nil {
		err = s.balancesStore.Set(idx, balance)
	}
	return
}

func (s *Service[I]) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	if idx, err := s.addressIndex.GetOrAdd(address); err == nil {
		nonce, err = s.noncesStore.Get(idx)
	}
	return
}

func (s *Service[I]) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	if idx, err := s.addressIndex.GetOrAdd(address); err == nil {
		err = s.noncesStore.Set(idx, nonce)
	}
	return
}

func (s *Service[I]) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	if slotIdx, err := s.mapStorage(address, key); err == nil {
		value, err = s.valuesStore.Get(slotIdx)
	}
	return
}

func (s *Service[I]) SetStorage(address common.Address, key common.Key, value common.Value) (err error) {
	if slotIdx, err := s.mapStorage(address, key); err == nil {
		err = s.valuesStore.Set(slotIdx, value)
	}
	return
}

func (s *Service[I]) GetHash() (hash common.Hash, err error) {
	h := sha256.New()

	if hash, err = s.addressIndex.GetStateHash(); err != nil {
		return
	}
	if _, err = h.Write(s.hashSerializer.ToBytes(hash)); err != nil {
		return
	}
	if hash, err = s.keyIndex.GetStateHash(); err != nil {
		return
	}
	if _, err = h.Write(s.hashSerializer.ToBytes(hash)); err != nil {
		return
	}
	if hash, err = s.slotIndex.GetStateHash(); err != nil {
		return
	}
	if _, err = h.Write(s.hashSerializer.ToBytes(hash)); err != nil {
		return
	}

	if hash, err = s.balancesStore.GetStateHash(); err != nil {
		return
	}
	if _, err = h.Write(s.hashSerializer.ToBytes(hash)); err != nil {
		return
	}

	if hash, err = s.noncesStore.GetStateHash(); err != nil {
		return
	}
	if _, err = h.Write(s.hashSerializer.ToBytes(hash)); err != nil {
		return
	}

	if hash, err = s.valuesStore.GetStateHash(); err != nil {
		return
	}

	hash = s.hashSerializer.FromBytes(h.Sum(nil))
	return
}

// mapStorage finds mapping from address and the values key to the values slot
func (s *Service[I]) mapStorage(address common.Address, key common.Key) (slotIdx I, err error) {
	if addressIdx, err := s.addressIndex.GetOrAdd(address); err == nil {
		if keyIdx, err := s.keyIndex.GetOrAdd(key); err == nil {
			slotIdx, err = s.slotIndex.GetOrAdd(common.SlotIdx[I]{addressIdx, keyIdx})
		}
	}

	return
}
