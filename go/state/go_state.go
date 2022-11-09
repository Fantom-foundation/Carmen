package state

import (
	"crypto/sha256"
	"hash"
	"io"

	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

const (
	HashTreeFactor = 32
	PageSize       = 1 << 12
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
	cleanup       []func()
	hasher        hash.Hash
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
	if err == index.ErrNotFound {
		err = nil
	}
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

func (s *GoState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	// TODO: consider retrieving cached hashes
	code, err := s.GetCode(address)
	if err != nil {
		return
	}
	if s.hasher == nil {
		s.hasher = sha3.NewLegacyKeccak256()
	}
	hash = common.GetHash(s.hasher, code)
	return
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
	flushables := []common.Flusher{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
	}

	var last error = nil
	for _, flushable := range flushables {
		if err := flushable.Flush(); err != nil {
			last = err
		}
	}
	return last
}

func (s *GoState) Close() error {
	closeables := []io.Closer{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
	}

	var last error = nil
	for _, closeable := range closeables {
		if err := closeable.Close(); err != nil {
			last = err
		}
	}

	if s.cleanup != nil {
		for _, clean := range s.cleanup {
			clean()
		}
	}
	return last
}
