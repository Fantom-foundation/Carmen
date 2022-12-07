package state

import (
	"io"
	"os"
	"path/filepath"

	cachedDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/cache"
	fileDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/file"
	ldbDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	cachedIndex "github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	indexmem "github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	cachedStore "github.com/Fantom-foundation/Carmen/go/backend/store/cache"
	ldbstore "github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	storemem "github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// CacheCapacity is the size of the cache expressed as the number of cached keys
const CacheCapacity = 1 << 20 // 2 ^ 20 keys -> 32MB for 32-bytes keys

// TransactBufferMB is the size of buffer before the transaction is flushed expressed in MBs
const TransactBufferMB = 128 * opt.MiB

// PoolSize is the maximum amount of data pages loaded in memory for the paged file store
const PoolSize = 100000

// The number of codes grouped together in depots to form one leaf node of the hash tree.
const CodeHashGroupSize = 4

// NewMemory creates in memory implementation
// (path parameter for compatibility with other state factories, can be left empty)
func NewMemory() (State, error) {
	addressIndex := indexmem.NewIndex[common.Address, uint32](common.AddressSerializer{})
	slotIndex := indexmem.NewIndex[common.SlotIdx[uint32], uint32](common.SlotIdxSerializer32{})
	keyIndex := indexmem.NewIndex[common.Key, uint32](common.KeySerializer{})

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

	codesDepot, err := memory.NewDepot[uint32](CodeHashGroupSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := storemem.NewStore[uint32, common.Hash](common.HashSerializer{}, PageSize, hashtree.GetNoHashFactory())
	if err != nil {
		return nil, err
	}

	state := &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot, codeHashesStore, nil, nil}
	return state, nil
}

// NewLeveLIndexFileStore creates LevelDB Index and File Store implementations
func NewLeveLIndexFileStore(path string) (State, error) {
	indexPath, storePath, err := createSubDirs(path)
	if err != nil {
		return nil, err
	}

	db, err := common.OpenLevelDb(indexPath, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountStorePath := storePath + string(filepath.Separator) + "accounts"
	if err = os.Mkdir(accountStorePath, 0777); err != nil {
		return nil, err
	}
	accountsStore, err := pagedfile.NewStore[uint32, common.AccountState](accountStorePath, common.AccountStateSerializer{}, PageSize, htfile.CreateHashTreeFactory(accountStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	noncesStorePath := storePath + string(filepath.Separator) + "nonces"
	if err = os.Mkdir(noncesStorePath, 0777); err != nil {
		return nil, err
	}
	noncesStore, err := pagedfile.NewStore[uint32, common.Nonce](noncesStorePath, common.NonceSerializer{}, PageSize, htfile.CreateHashTreeFactory(noncesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	balancesStorePath := storePath + string(filepath.Separator) + "balances"
	if err = os.Mkdir(balancesStorePath, 0777); err != nil {
		return nil, err
	}
	balancesStore, err := pagedfile.NewStore[uint32, common.Balance](balancesStorePath, common.BalanceSerializer{}, PageSize, htfile.CreateHashTreeFactory(balancesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.Mkdir(valuesStorePath, 0777); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}

	codesPath := storePath + string(filepath.Separator) + "codes"
	if err = os.Mkdir(codesPath, 0777); err != nil {
		return nil, err
	}
	codesDepot, err := fileDepot.NewDepot[uint32](codesPath, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(codesPath, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStorePath := storePath + string(filepath.Separator) + "codeHashes"
	if err = os.Mkdir(codeHashesStorePath, 0777); err != nil {
		return nil, err
	}
	codeHashesStore, err := pagedfile.NewStore[uint32, common.Hash](codeHashesStorePath, common.HashSerializer{}, PageSize, hashtree.GetNoHashFactory(), PoolSize)
	if err != nil {
		return nil, err
	}

	state := &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot, codeHashesStore, cleanUpByClosing(db), nil}

	return state, nil
}

// NewCachedLeveLIndexFileStore creates Cached LevelDB Index and File Store implementations
func NewCachedLeveLIndexFileStore(path string) (State, error) {
	indexPath, storePath, err := createSubDirs(path)
	if err != nil {
		return nil, err
	}

	db, err := common.OpenLevelDb(indexPath, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountStorePath := storePath + string(filepath.Separator) + "accounts"
	if err = os.Mkdir(accountStorePath, 0777); err != nil {
		return nil, err
	}
	accountsStore, err := pagedfile.NewStore[uint32, common.AccountState](accountStorePath, common.AccountStateSerializer{}, PageSize, htfile.CreateHashTreeFactory(accountStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	noncesStorePath := storePath + string(filepath.Separator) + "nonces"
	if err = os.Mkdir(noncesStorePath, 0777); err != nil {
		return nil, err
	}
	noncesStore, err := pagedfile.NewStore[uint32, common.Nonce](noncesStorePath, common.NonceSerializer{}, PageSize, htfile.CreateHashTreeFactory(noncesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	balancesStorePath := storePath + string(filepath.Separator) + "balances"
	if err = os.Mkdir(balancesStorePath, 0777); err != nil {
		return nil, err
	}
	balancesStore, err := pagedfile.NewStore[uint32, common.Balance](balancesStorePath, common.BalanceSerializer{}, PageSize, htfile.CreateHashTreeFactory(balancesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}

	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.Mkdir(valuesStorePath, 0777); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}

	codesPath := storePath + string(filepath.Separator) + "codes"
	if err = os.Mkdir(codesPath, 0777); err != nil {
		return nil, err
	}
	codesDepot, err := fileDepot.NewDepot[uint32](codesPath, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(codesPath, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStorePath := storePath + string(filepath.Separator) + "codeHashes"
	if err = os.Mkdir(codeHashesStorePath, 0777); err != nil {
		return nil, err
	}
	codeHashesStore, err := pagedfile.NewStore[uint32, common.Hash](codeHashesStorePath, common.HashSerializer{}, PageSize, hashtree.GetNoHashFactory(), PoolSize)
	if err != nil {
		return nil, err
	}

	state := &GoState{
		cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
		cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
		cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
		cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
		cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
		cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
		cleanUpByClosing(db), nil}

	return state, nil
}

// NewCachedTransactLeveLIndexFileStore creates Cached and Transactional LevelDB Index and File Store implementations
func NewCachedTransactLeveLIndexFileStore(path string) (State, error) {
	indexPath, storePath, err := createSubDirs(path)
	if err != nil {
		return nil, err
	}

	opts := opt.Options{WriteBuffer: TransactBufferMB}
	db, err := common.OpenLevelDb(indexPath, &opts)
	if err != nil {
		return nil, err
	}
	tx, err := db.OpenTransaction()

	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](tx, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](tx, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](tx, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountStorePath := storePath + string(filepath.Separator) + "accounts"
	if err = os.Mkdir(accountStorePath, 0777); err != nil {
		return nil, err
	}
	accountsStore, err := pagedfile.NewStore[uint32, common.AccountState](accountStorePath, common.AccountStateSerializer{}, PageSize, htfile.CreateHashTreeFactory(accountStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	noncesStorePath := storePath + string(filepath.Separator) + "nonces"
	if err = os.Mkdir(noncesStorePath, 0777); err != nil {
		return nil, err
	}
	noncesStore, err := pagedfile.NewStore[uint32, common.Nonce](noncesStorePath, common.NonceSerializer{}, PageSize, htfile.CreateHashTreeFactory(noncesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	balancesStorePath := storePath + string(filepath.Separator) + "balances"
	if err = os.Mkdir(balancesStorePath, 0777); err != nil {
		return nil, err
	}
	balancesStore, err := pagedfile.NewStore[uint32, common.Balance](balancesStorePath, common.BalanceSerializer{}, PageSize, htfile.CreateHashTreeFactory(balancesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}

	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.Mkdir(valuesStorePath, 0777); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}

	codesPath := storePath + string(filepath.Separator) + "codes"
	if err = os.Mkdir(codesPath, 0777); err != nil {
		return nil, err
	}
	codesDepot, err := fileDepot.NewDepot[uint32](codesPath, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(codesPath, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStorePath := storePath + string(filepath.Separator) + "codeHashes"
	if err = os.Mkdir(codeHashesStorePath, 0777); err != nil {
		return nil, err
	}
	codeHashesStore, err := pagedfile.NewStore[uint32, common.Hash](codeHashesStorePath, common.HashSerializer{}, PageSize, hashtree.GetNoHashFactory(), PoolSize)
	if err != nil {
		return nil, err
	}

	cleanup := []func(){
		func() {
			_ = tx.Commit()
			_ = db.Close()
		},
	}

	state := &GoState{
		cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
		cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
		cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
		cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
		cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
		cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
		cleanup, nil}

	return state, nil
}

// NewLeveLIndexAndStore creates Index and Store both backed up by the leveldb
func NewLeveLIndexAndStore(path string) (State, error) {
	db, err := common.OpenLevelDb(path, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, common.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	valueHashTreeFactory := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}

	codesDepot, err := ldbDepot.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), PageSize)
	if err != nil {
		return nil, err
	}

	state := &GoState{addressIndex, keyIndex, slotIndex, accountsStore, noncesStore, balancesStore, valuesStore, codesDepot, codeHashesStore, cleanUpByClosing(db), nil}

	return state, nil
}

// NewCachedLeveLIndexAndStore creates Index and Store both backed up by the leveldb
func NewCachedLeveLIndexAndStore(path string) (State, error) {
	db, err := common.OpenLevelDb(path, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, common.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	valueHashTreeFactory := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}

	codesDepot, err := ldbDepot.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), PageSize)
	if err != nil {
		return nil, err
	}

	state := &GoState{
		cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
		cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
		cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
		cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
		cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
		cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
		cleanUpByClosing(db), nil}

	return state, nil
}

// NewTransactCachedLeveLIndexAndStore creates Index and Store both backed up by the leveldb
func NewTransactCachedLeveLIndexAndStore(path string) (State, error) {
	opts := opt.Options{WriteBuffer: TransactBufferMB}
	db, err := common.OpenLevelDb(path, &opts)
	if err != nil {
		return nil, err
	}
	tx, err := db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](tx, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](tx, common.SlotLocIndexKey, common.SlotIdxSerializer32{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	keyIndex, err := ldb.NewIndex[common.Key, uint32](tx, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}

	accountHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](tx, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](tx, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](tx, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}
	valueHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](tx, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, PageSize)
	if err != nil {
		return nil, err
	}

	depotHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.DepotCodeKey, HashTreeFactor)
	codesDepot, err := ldbDepot.NewDepot[uint32](tx, common.DepotCodeKey, common.Identifier32Serializer{}, depotHashTreeFactory, CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](tx, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), PageSize)
	if err != nil {
		return nil, err
	}

	cleanup := []func(){
		func() {
			_ = tx.Commit()
			_ = db.Close()
		},
	}

	state := &GoState{
		cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
		cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
		cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
		cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
		cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
		cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
		cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
		cleanup, nil}

	return state, nil
}

// createSubDirs creates two subdirectories of the given for the Store and the Index
func createSubDirs(rootPath string) (indexPath, storePath string, err error) {
	indexPath = rootPath + string(filepath.Separator) + "index"
	if err = os.Mkdir(indexPath, 0777); err != nil {
		return
	}

	storePath = rootPath + string(filepath.Separator) + "store"
	err = os.Mkdir(storePath, 0777)

	return
}

// cleanUpByClosing provides a clean-up function, which ensure closing the resource on the state clean-up
func cleanUpByClosing(db io.Closer) []func() {
	return []func(){
		func() {
			_ = db.Close()
		},
	}
}
