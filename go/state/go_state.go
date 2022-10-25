package state

import (
	"crypto/sha256"
	"io"

	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	indexldb "github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	indexmem "github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	storeldb "github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	storemem "github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	HashTreeFactor = 3
	PageSize       = 32 * 32
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
}

func NewGoInMemoryState() (State, error) {
	var addressIndex index.Index[common.Address, uint32] = indexmem.NewIndex[common.Address, uint32](common.AddressSerializer{})
	var keyIndex index.Index[common.Key, uint32] = indexmem.NewIndex[common.Key, uint32](common.KeySerializer{})
	var slotIndex index.Index[common.SlotIdx[uint32], uint32] = indexmem.NewIndex[common.SlotIdx[uint32], uint32](common.SlotIdxSerializer32{})
	accountsStore, err := storemem.NewStore[uint32, common.AccountState](common.AccountStateSerializer{}, PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	noncesStore, err := storemem.NewStore[uint32, common.Nonce](common.NonceSerializer{}, PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	balancesStore, err := storemem.NewStore[uint32, common.Balance](common.BalanceSerializer{}, PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	valuesStore, err := storemem.NewStore[uint32, common.Value](common.ValueSerializer{}, PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	codesDepot, err := memory.NewDepot[uint32](PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	return &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot, nil}, nil
}

func NewGoLevelDbState(directory string) (State, error) {
	db, err := leveldb.OpenFile(directory, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := indexldb.NewIndex[common.Address, uint32](db, common.AddressKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := indexldb.NewIndex[common.Key, uint32](db, common.KeyKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := indexldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	accountsStore, err := storeldb.NewStore[uint32, common.AccountState](db, common.AccountKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, htmemory.CreateHashTreeFactory(HashTreeFactor), PageSize)
	if err != nil {
		return nil, err
	}
	noncesStore, err := storeldb.NewStore[uint32, common.Nonce](db, common.NonceKey, common.NonceSerializer{}, common.Identifier32Serializer{}, htmemory.CreateHashTreeFactory(HashTreeFactor), PageSize)
	if err != nil {
		return nil, err
	}
	balancesStore, err := storeldb.NewStore[uint32, common.Balance](db, common.BalanceKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, htmemory.CreateHashTreeFactory(HashTreeFactor), PageSize)
	if err != nil {
		return nil, err
	}
	valuesStore, err := storeldb.NewStore[uint32, common.Value](db, common.ValueKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htmemory.CreateHashTreeFactory(HashTreeFactor), PageSize)
	if err != nil {
		return nil, err
	}
	codesDepot, err := memory.NewDepot[uint32](PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	cleanup := []func(){
		func() { db.Close() },
	}
	return &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot, cleanup}, nil
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
