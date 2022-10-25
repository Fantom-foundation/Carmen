package state

import (
	"crypto/sha256"

	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// GoState manages dependencies to other interfaces to build this service
type GoState struct {
	addressIndex  index.Index[common.Address, uint32]
	keyIndex      index.Index[common.Key, uint32]
	slotIndex     index.Index[common.SlotIdx[uint32], uint32]
	accountsStore store.Store[uint32, common.AccountState]
	noncesStore   store.Store[uint32, common.Nonce]
	balancesStore store.Store[uint32, common.Balance]
	valuesStore   store.Store[uint32, common.Value]
	codesDepot    depot.Depot[uint32]
}

// NewGoState creates a new instance of this service
func NewGoState(
	addressIndex index.Index[common.Address, uint32],
	keyIndex index.Index[common.Key, uint32],
	slotIndex index.Index[common.SlotIdx[uint32], uint32],
	accountsStore store.Store[uint32, common.AccountState],
	noncesStore store.Store[uint32, common.Nonce],
	balancesStore store.Store[uint32, common.Balance],
	valuesStore store.Store[uint32, common.Value],
	codesDepot depot.Depot[uint32]) *GoState {

	return &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot}
}

func (s *GoState) CreateAccount(address common.Address) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.accountsStore.Set(idx, common.Exists)
}

func (s *GoState) GetAccountState(address common.Address) (state common.AccountState, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Unknown, nil
		}
		return
	}
	return s.accountsStore.Get(idx)
}

func (s *GoState) DeleteAccount(address common.Address) (err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		return
	}
	return s.accountsStore.Set(idx, common.Deleted)
}

func (s *GoState) GetBalance(address common.Address) (balance common.Balance, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Balance{}, nil
		}
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
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Nonce{}, nil
		}
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
	addressIdx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	keyIdx, err := s.keyIndex.Get(key)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	slotIdx, err := s.slotIndex.Get(common.SlotIdx[uint32]{addressIdx, keyIdx})
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	return s.valuesStore.Get(slotIdx)
}

func (s *GoState) SetStorage(address common.Address, key common.Key, value common.Value) (err error) {
	addressIdx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	keyIdx, err := s.keyIndex.GetOrAdd(key)
	if err != nil {
		return
	}
	slotIdx, err := s.slotIndex.GetOrAdd(common.SlotIdx[uint32]{addressIdx, keyIdx})
	if err != nil {
		return
	}
	return s.valuesStore.Set(slotIdx, value)
}

func (s *GoState) GetCode(address common.Address) (value []byte, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil, nil
		}
		return
	}
	return s.codesDepot.Get(idx)
}

func (s *GoState) SetCode(address common.Address, code []byte) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.codesDepot.Set(idx, code)
}

func (s *GoState) GetHash() (hash common.Hash, err error) {
	sources := []common.HashProvider{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.balancesStore,
		s.noncesStore,
		s.valuesStore,
		s.accountsStore,
		s.codesDepot,
	}

	h := sha256.New()
	for _, source := range sources {
		if hash, err = source.GetStateHash(); err != nil {
			return
		}
		if _, err = h.Write(hash[:]); err != nil {
			return
		}
	}
	copy(hash[:], h.Sum(nil))
	return hash, nil
}

func (s *GoState) Flush() error {
	return s.Flush()
}

func (s *GoState) Close() error {
	return s.Close()
}
