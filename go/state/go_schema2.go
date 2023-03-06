package state

import (
	"crypto/sha256"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
	"hash"
	"io"
)

// GoSchema2 implementation of a state utilizes a schema where Addresses are indexed,
// but slot keys are not.
type GoSchema2 struct {
	addressIndex    index.Index[common.Address, uint32]
	slotIndex       index.Index[common.SlotIdxKey[uint32], uint32]
	accountsStore   store.Store[uint32, common.AccountState]
	noncesStore     store.Store[uint32, common.Nonce]
	balancesStore   store.Store[uint32, common.Balance]
	valuesStore     store.Store[uint32, common.Value]
	codesDepot      depot.Depot[uint32]
	codeHashesStore store.Store[uint32, common.Hash]
	addressToSlots  multimap.MultiMap[uint32, uint32]
	hasher          hash.Hash
}

func (s *GoSchema2) createAccount(address common.Address) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	err = s.accountsStore.Set(idx, common.Exists)
	if err != nil {
		return
	}
	return s.clearAccount(idx)
}

func (s *GoSchema2) Exists(address common.Address) (bool, error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	state, err := s.accountsStore.Get(idx)
	return state == common.Exists, err
}

func (s *GoSchema2) deleteAccount(address common.Address) error {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil
		}
		return err
	}
	err = s.accountsStore.Set(idx, common.Unknown)
	if err != nil {
		return err
	}
	return s.clearAccount(idx)
}

func (s *GoSchema2) clearAccount(idx uint32) error {
	slotIdxs, err := s.addressToSlots.GetAll(idx)
	if err != nil {
		return err
	}
	for _, slotIdx := range slotIdxs {
		if err := s.valuesStore.Set(slotIdx, common.Value{}); err != nil {
			return err
		}
	}
	return s.addressToSlots.RemoveAll(idx)
}

func (s *GoSchema2) GetBalance(address common.Address) (balance common.Balance, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Balance{}, nil
		}
		return
	}
	return s.balancesStore.Get(idx)
}

func (s *GoSchema2) setBalance(address common.Address, balance common.Balance) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Set(idx, balance)
}

func (s *GoSchema2) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Nonce{}, nil
		}
		return
	}
	return s.noncesStore.Get(idx)
}

func (s *GoSchema2) setNonce(address common.Address, nonce common.Nonce) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Set(idx, nonce)
}

func (s *GoSchema2) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	addressIdx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	slotIdx, err := s.slotIndex.Get(common.SlotIdxKey[uint32]{addressIdx, key})
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	return s.valuesStore.Get(slotIdx)
}

func (s *GoSchema2) setStorage(address common.Address, key common.Key, value common.Value) error {
	addressIdx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return err
	}
	slotIdx, err := s.slotIndex.GetOrAdd(common.SlotIdxKey[uint32]{addressIdx, key})
	if err != nil {
		return err
	}
	err = s.valuesStore.Set(slotIdx, value)
	if err != nil {
		return err
	}
	if value == (common.Value{}) {
		err = s.addressToSlots.Remove(addressIdx, slotIdx)
	} else {
		err = s.addressToSlots.Add(addressIdx, slotIdx)
	}
	return err
}

func (s *GoSchema2) GetCode(address common.Address) (value []byte, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil, nil
		}
		return
	}
	return s.codesDepot.Get(idx)
}

func (s *GoSchema2) GetCodeSize(address common.Address) (size int, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return 0, nil
		}
		return
	}
	return s.codesDepot.GetSize(idx)
}

func (s *GoSchema2) setCode(address common.Address, code []byte) (err error) {
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

func (s *GoSchema2) GetCodeHash(address common.Address) (hash common.Hash, err error) {
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

func (s *GoSchema2) GetHash() (hash common.Hash, err error) {
	sources := []common.HashProvider{
		s.addressIndex,
		s.slotIndex,
		s.balancesStore,
		s.noncesStore,
		s.valuesStore,
		s.accountsStore,
		s.codesDepot,
		// codeHashesStore omitted intentionally
		// addressToSlots omitted intentionally
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

func (s *GoSchema2) Flush() (lastErr error) {
	flushables := []common.Flusher{
		s.addressIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
		s.addressToSlots,
	}

	for _, flushable := range flushables {
		if err := flushable.Flush(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

func (s *GoSchema2) Close() (lastErr error) {
	closeables := []io.Closer{
		s.addressIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
		s.addressToSlots,
	}

	for _, closeable := range closeables {
		if err := closeable.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoSchema2) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(0)
	mf.AddChild("addressIndex", s.addressIndex.GetMemoryFootprint())
	mf.AddChild("slotIndex", s.slotIndex.GetMemoryFootprint())
	mf.AddChild("accountsStore", s.accountsStore.GetMemoryFootprint())
	mf.AddChild("noncesStore", s.noncesStore.GetMemoryFootprint())
	mf.AddChild("balancesStore", s.balancesStore.GetMemoryFootprint())
	mf.AddChild("valuesStore", s.valuesStore.GetMemoryFootprint())
	mf.AddChild("codesDepot", s.codesDepot.GetMemoryFootprint())
	mf.AddChild("codeHashesStore", s.codeHashesStore.GetMemoryFootprint())
	mf.AddChild("addressToSlots", s.addressToSlots.GetMemoryFootprint())
	return mf
}