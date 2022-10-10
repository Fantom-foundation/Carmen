package state

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// GoState manages dependencies to other interfaces to build this service
type GoState struct {
	addressIndex   index.Index[common.Address, uint32]
	keyIndex       index.Index[common.Key, uint32]
	slotIndex      index.Index[common.SlotIdx[uint32], uint32]
	noncesStore    store.Store[uint32, common.Nonce]
	balancesStore  store.Store[uint32, common.Balance]
	valuesStore    store.Store[uint32, common.Value]
	hashSerializer common.HashSerializer
}

// NewGoState creates a new instance of this service
func NewGoState(
	addressIndex index.Index[common.Address, uint32],
	keyIndex index.Index[common.Key, uint32],
	slotIndex index.Index[common.SlotIdx[uint32], uint32],
	noncesStore store.Store[uint32, common.Nonce],
	balancesStore store.Store[uint32, common.Balance],
	valuesStore store.Store[uint32, common.Value]) *GoState {

	return &GoState{addressIndex, keyIndex, slotIndex, noncesStore, balancesStore, valuesStore, common.HashSerializer{}}
}

func (s *GoState) GetBalance(address common.Address) (balance common.Balance, err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Get(idx)
}

func (s *GoState) SetBalance(address common.Address, balance common.Balance) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Set(idx, balance)
}

func (s *GoState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Get(idx)
}

func (s *GoState) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Set(idx, nonce)
}

func (s *GoState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	slotIdx, err := s.mapStorage(address, key)
	if err != nil {
		return
	}
	return s.valuesStore.Get(slotIdx)
}

func (s *GoState) SetStorage(address common.Address, key common.Key, value common.Value) (err error) {
	slotIdx, err := s.mapStorage(address, key)
	if err != nil {
		return
	}
	return s.valuesStore.Set(slotIdx, value)
}

func (s *GoState) GetHash() (hash common.Hash, err error) {
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
func (s *GoState) mapStorage(address common.Address, key common.Key) (slotIdx uint32, err error) {
	addressIdx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	keyIdx, err := s.keyIndex.GetOrAdd(key)
	if err != nil {
		return
	}
	return s.slotIndex.GetOrAdd(common.SlotIdx[uint32]{addressIdx, keyIdx})
}
