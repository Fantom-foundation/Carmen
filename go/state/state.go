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

// NewService creates a new instance of this service
func NewService[I common.Identifier](
	addressIndex index.Index[common.Address, I],
	keyIndex index.Index[common.Key, I],
	slotIndex index.Index[common.SlotIdx[I], I],
	noncesStore store.Store[I, common.Nonce],
	balancesStore store.Store[I, common.Balance],
	valuesStore store.Store[I, common.Value]) *Service[I] {

	return &Service[I]{addressIndex, keyIndex, slotIndex, noncesStore, balancesStore, valuesStore, common.HashSerializer{}}
}

func (s *Service[I]) GetBalance(address common.Address) (balance common.Balance, err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Get(idx)
}

func (s *Service[I]) SetBalance(address common.Address, balance common.Balance) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Set(idx, balance)
}

func (s *Service[I]) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Get(idx)
}

func (s *Service[I]) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Set(idx, nonce)
}

func (s *Service[I]) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	slotIdx, err := s.mapStorage(address, key)
	if err != nil {
		return
	}
	return s.valuesStore.Get(slotIdx)
}

func (s *Service[I]) SetStorage(address common.Address, key common.Key, value common.Value) (err error) {
	slotIdx, err := s.mapStorage(address, key)
	if err != nil {
		return
	}
	return s.valuesStore.Set(slotIdx, value)
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
	addressIdx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	keyIdx, err := s.keyIndex.GetOrAdd(key)
	if err != nil {
		return
	}
	return s.slotIndex.GetOrAdd(common.SlotIdx[I]{addressIdx, keyIdx})
}
