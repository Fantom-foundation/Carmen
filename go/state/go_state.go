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
	addressIndex    index.Index[common.Address, uint32]
	slotIndex       index.Index[common.SlotIdx[uint32], uint32]
	accountsStore   store.Store[uint32, common.AccountState]
	noncesStore     store.Store[uint32, common.Nonce]
	balancesStore   store.Store[uint32, common.Balance]
	valuesStore     store.Store[uint32, common.Value]
	codesDepot      depot.Depot[uint32]
	codeHashesStore store.Store[uint32, common.Hash]
	cleanup         []func()
	hasher          hash.Hash
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

func (s *GoState) DeleteAccount(address common.Address) error {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil
		}
		return err
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
	slotIdx, err := s.slotIndex.Get(common.SlotIdx[uint32]{addressIdx, key})
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
	slotIdx, err := s.slotIndex.GetOrAdd(common.SlotIdx[uint32]{addressIdx, key})
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

func (s *GoState) GetCodeSize(address common.Address) (size int, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return 0, nil
		}
		return
	}
	return s.codesDepot.GetSize(idx)
}

func (s *GoState) SetCode(address common.Address, code []byte) (err error) {
	var codeHash common.Hash
	if code != nil { // codeHash is zero for empty code
		if s.hasher == nil {
			s.hasher = sha3.NewLegacyKeccak256()
		}
		codeHash = common.GetHash(s.hasher, code)
	}

	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	err = s.codesDepot.Set(idx, code)
	if err != nil {
		return
	}
	return s.codeHashesStore.Set(idx, codeHash)
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func (s *GoState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return emptyCodeHash, nil
		}
		return
	}
	hash, err = s.codeHashesStore.Get(idx)
	if err != nil {
		return hash, err
	}
	// Stores use the default value in cases where there is no value present. Thus,
	// when returning a zero hash, we need to check whether it is indeed the case
	// that this is the hash of the code or whether we should actually return the
	// hash of the empty code.
	if (hash == common.Hash{}) {
		size, err := s.GetCodeSize(address)
		if err != nil {
			return hash, err
		}
		if size == 0 {
			return emptyCodeHash, nil
		}
	}
	return hash, nil
}

func (s *GoState) GetHash() (hash common.Hash, err error) {
	sources := []common.HashProvider{
		s.addressIndex,
		s.slotIndex,
		s.balancesStore,
		s.noncesStore,
		s.valuesStore,
		s.accountsStore,
		s.codesDepot,
		// codeHashesStore omitted intentionally
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

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(0)
	mf.AddChild("addressIndex", s.addressIndex.GetMemoryFootprint())
	mf.AddChild("slotIndex", s.slotIndex.GetMemoryFootprint())
	mf.AddChild("accountsStore", s.accountsStore.GetMemoryFootprint())
	mf.AddChild("noncesStore", s.noncesStore.GetMemoryFootprint())
	mf.AddChild("balancesStore", s.balancesStore.GetMemoryFootprint())
	mf.AddChild("valuesStore", s.valuesStore.GetMemoryFootprint())
	mf.AddChild("codesDepot", s.codesDepot.GetMemoryFootprint())
	mf.AddChild("codeHashesStore", s.codeHashesStore.GetMemoryFootprint())
	return mf
}

func (s *GoState) Flush() error {
	flushables := []common.Flusher{
		s.addressIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
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
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
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
