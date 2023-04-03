package state

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	archldb "github.com/Fantom-foundation/Carmen/go/backend/archive/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/sqlite"

	"github.com/Fantom-foundation/Carmen/go/backend/index/file"

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
	mapbtree "github.com/Fantom-foundation/Carmen/go/backend/multimap/btreemem"
	mapldb "github.com/Fantom-foundation/Carmen/go/backend/multimap/ldb"
	mapmem "github.com/Fantom-foundation/Carmen/go/backend/multimap/memory"
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

// CodeHashGroupSize represents the number of codes grouped together in depots to form one leaf node of the hash tree.
const CodeHashGroupSize = 4

type ArchiveType int

const (
	NoArchive      ArchiveType = 0
	LevelDbArchive ArchiveType = 1
	SqliteArchive  ArchiveType = 2
)

func (a ArchiveType) String() string {
	switch a {
	case NoArchive:
		return "NoArchive"
	case LevelDbArchive:
		return "LevelDbArchive"
	case SqliteArchive:
		return "SqliteArchive"
	}
	return "unknown"
}

type StateSchema uint8

const defaultSchema StateSchema = 1

func GetAllSchemas() []StateSchema {
	return []StateSchema{1, 2, 3}
}

// Parameters struct defining configuration parameters for state instances.
type Parameters struct {
	Directory string
	Schema    StateSchema
	Archive   ArchiveType
}

// NewGoMemoryState creates in memory implementation
// (path parameter for compatibility with other state factories, can be left empty)
func NewGoMemoryState(params Parameters) (State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	addressIndex := indexmem.NewIndex[common.Address, uint32](common.AddressSerializer{})
	accountsStore, err := storemem.NewStore[uint32, common.AccountState](common.AccountStateSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	noncesStore, err := storemem.NewStore[uint32, common.Nonce](common.NonceSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	balancesStore, err := storemem.NewStore[uint32, common.Balance](common.BalanceSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	codesDepot, err := memory.NewDepot[uint32](CodeHashGroupSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := storemem.NewStore[uint32, common.Hash](common.HashSerializer{}, common.PageSize, hashtree.GetNoHashFactory())
	if err != nil {
		return nil, err
	}

	var schema GoSchema
	switch params.Schema {
	case 1:
		slotIndex := indexmem.NewIndex[common.SlotIdx[uint32], uint32](common.SlotIdx32Serializer{})
		keyIndex := indexmem.NewIndex[common.Key, uint32](common.KeySerializer{})
		valuesStore, err := storemem.NewStore[uint32, common.Value](common.ValueSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
		if err != nil {
			return nil, err
		}
		addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

		schema = &GoSchema1{
			addressIndex,
			keyIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 2:
		slotIndex := indexmem.NewIndex[common.SlotIdxKey[uint32], uint32](common.SlotIdx32KeySerializer{})
		valuesStore, err := storemem.NewStore[uint32, common.Value](common.ValueSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
		if err != nil {
			return nil, err
		}
		addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

		schema = &GoSchema2{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 3:
		slotIndex := indexmem.NewIndex[common.SlotIdxKey[uint32], uint32](common.SlotIdx32KeySerializer{})
		reincarnationsStore, err := storemem.NewStore[uint32, common.Reincarnation](common.ReincarnationSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
		if err != nil {
			return nil, err
		}
		valuesStore, err := storemem.NewStore[uint32, common.SlotReincValue](common.SlotReincValueSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
		if err != nil {
			return nil, err
		}

		schema = &GoSchema3{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			reincarnationsStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			nil,
		}
	default:
		return nil, fmt.Errorf("the go implementation only supports schemas 1,2,3")
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup})
	return state, nil
}

// NewGoFileState creates File based Index and Store implementations
func NewGoFileState(params Parameters) (State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	indexPath, storePath, err := createSubDirs(params.Directory)
	if err != nil {
		return nil, err
	}
	addressIndexPath := indexPath + string(filepath.Separator) + "addresses"
	if err = os.MkdirAll(addressIndexPath, 0700); err != nil {
		return nil, err
	}
	addressIndex, err := file.NewIndex[common.Address, uint32](addressIndexPath, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	if err != nil {
		return nil, err
	}
	slotsIndexPath := indexPath + string(filepath.Separator) + "slots"
	if err = os.MkdirAll(slotsIndexPath, 0700); err != nil {
		return nil, err
	}
	accountStorePath := storePath + string(filepath.Separator) + "accounts"
	if err = os.MkdirAll(accountStorePath, 0700); err != nil {
		return nil, err
	}
	accountsStore, err := pagedfile.NewStore[uint32, common.AccountState](accountStorePath, common.AccountStateSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(accountStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	noncesStorePath := storePath + string(filepath.Separator) + "nonces"
	if err = os.MkdirAll(noncesStorePath, 0700); err != nil {
		return nil, err
	}
	noncesStore, err := pagedfile.NewStore[uint32, common.Nonce](noncesStorePath, common.NonceSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(noncesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	balancesStorePath := storePath + string(filepath.Separator) + "balances"
	if err = os.MkdirAll(balancesStorePath, 0700); err != nil {
		return nil, err
	}
	balancesStore, err := pagedfile.NewStore[uint32, common.Balance](balancesStorePath, common.BalanceSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(balancesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	codesPath := storePath + string(filepath.Separator) + "codes"
	if err = os.MkdirAll(codesPath, 0700); err != nil {
		return nil, err
	}
	codesDepot, err := fileDepot.NewDepot[uint32](codesPath, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(codesPath, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStorePath := storePath + string(filepath.Separator) + "codeHashes"
	if err = os.MkdirAll(codeHashesStorePath, 0700); err != nil {
		return nil, err
	}
	codeHashesStore, err := pagedfile.NewStore[uint32, common.Hash](codeHashesStorePath, common.HashSerializer{}, common.PageSize, hashtree.GetNoHashFactory(), PoolSize)
	if err != nil {
		return nil, err
	}
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}

	var schema GoSchema
	switch params.Schema {
	case 1:
		slotIndex, err := file.NewIndex[common.SlotIdx[uint32], uint32](slotsIndexPath, common.SlotIdx32Serializer{}, common.Identifier32Serializer{}, common.SlotIdx32Hasher{}, common.SlotIdx32Comparator{})
		if err != nil {
			return nil, err
		}
		keysIndexPath := indexPath + string(filepath.Separator) + "keys"
		if err = os.MkdirAll(keysIndexPath, 0700); err != nil {
			return nil, err
		}
		keyIndex, err := file.NewIndex[common.Key, uint32](keysIndexPath, common.KeySerializer{}, common.Identifier32Serializer{}, common.KeyHasher{}, common.KeyComparator{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapbtree.NewMultiMap[uint32, uint32](common.Identifier32Serializer{}, common.Identifier32Serializer{}, common.Uint32Comparator{}, common.Uint32Comparator{})

		schema = &GoSchema1{
			addressIndex,
			keyIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 2:
		slotIndex, err := file.NewIndex[common.SlotIdxKey[uint32], uint32](slotsIndexPath, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
		if err != nil {
			return nil, err
		}
		keysIndexPath := indexPath + string(filepath.Separator) + "keys"
		if err = os.MkdirAll(keysIndexPath, 0700); err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapbtree.NewMultiMap[uint32, uint32](common.Identifier32Serializer{}, common.Identifier32Serializer{}, common.Uint32Comparator{}, common.Uint32Comparator{})

		schema = &GoSchema2{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 3:
		slotIndex, err := file.NewIndex[common.SlotIdxKey[uint32], uint32](slotsIndexPath, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
		if err != nil {
			return nil, err
		}
		reincStorePath := indexPath + string(filepath.Separator) + "reinc"
		if err = os.MkdirAll(reincStorePath, 0700); err != nil {
			return nil, err
		}
		reincarnationsStore, err := pagedfile.NewStore[uint32, common.Reincarnation](reincStorePath, common.ReincarnationSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(reincStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.SlotReincValue](valuesStorePath, common.SlotReincValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}

		schema = &GoSchema3{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			reincarnationsStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			nil,
		}
	default:
		return nil, fmt.Errorf("the go implementation only supports schemas 1,2,3")
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup})
	return state, nil
}

// NewGoCachedFileState creates File based Index and Store implementations
func NewGoCachedFileState(params Parameters) (State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	indexPath, storePath, err := createSubDirs(params.Directory)
	if err != nil {
		return nil, err
	}
	addressIndexPath := indexPath + string(filepath.Separator) + "addresses"
	if err = os.MkdirAll(addressIndexPath, 0700); err != nil {
		return nil, err
	}
	addressIndex, err := file.NewIndex[common.Address, uint32](addressIndexPath, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	if err != nil {
		return nil, err
	}
	slotsIndexPath := indexPath + string(filepath.Separator) + "slots"
	if err = os.MkdirAll(slotsIndexPath, 0700); err != nil {
		return nil, err
	}
	accountStorePath := storePath + string(filepath.Separator) + "accounts"
	if err = os.MkdirAll(accountStorePath, 0700); err != nil {
		return nil, err
	}
	accountsStore, err := pagedfile.NewStore[uint32, common.AccountState](accountStorePath, common.AccountStateSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(accountStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	noncesStorePath := storePath + string(filepath.Separator) + "nonces"
	if err = os.MkdirAll(noncesStorePath, 0700); err != nil {
		return nil, err
	}
	noncesStore, err := pagedfile.NewStore[uint32, common.Nonce](noncesStorePath, common.NonceSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(noncesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	balancesStorePath := storePath + string(filepath.Separator) + "balances"
	if err = os.MkdirAll(balancesStorePath, 0700); err != nil {
		return nil, err
	}
	balancesStore, err := pagedfile.NewStore[uint32, common.Balance](balancesStorePath, common.BalanceSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(balancesStorePath, HashTreeFactor), PoolSize)
	if err != nil {
		return nil, err
	}
	codesPath := storePath + string(filepath.Separator) + "codes"
	if err = os.MkdirAll(codesPath, 0700); err != nil {
		return nil, err
	}
	codesDepot, err := fileDepot.NewDepot[uint32](codesPath, common.Identifier32Serializer{}, htfile.CreateHashTreeFactory(codesPath, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStorePath := storePath + string(filepath.Separator) + "codeHashes"
	if err = os.MkdirAll(codeHashesStorePath, 0700); err != nil {
		return nil, err
	}
	codeHashesStore, err := pagedfile.NewStore[uint32, common.Hash](codeHashesStorePath, common.HashSerializer{}, common.PageSize, hashtree.GetNoHashFactory(), PoolSize)
	if err != nil {
		return nil, err
	}
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}

	var schema GoSchema
	switch params.Schema {
	case 1:
		slotIndex, err := file.NewIndex[common.SlotIdx[uint32], uint32](slotsIndexPath, common.SlotIdx32Serializer{}, common.Identifier32Serializer{}, common.SlotIdx32Hasher{}, common.SlotIdx32Comparator{})
		if err != nil {
			return nil, err
		}
		keysIndexPath := indexPath + string(filepath.Separator) + "keys"
		if err = os.MkdirAll(keysIndexPath, 0700); err != nil {
			return nil, err
		}
		keyIndex, err := file.NewIndex[common.Key, uint32](keysIndexPath, common.KeySerializer{}, common.Identifier32Serializer{}, common.KeyHasher{}, common.KeyComparator{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapbtree.NewMultiMap[uint32, uint32](common.Identifier32Serializer{}, common.Identifier32Serializer{}, common.Uint32Comparator{}, common.Uint32Comparator{})

		schema = &GoSchema1{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			addressToSlots,
			nil,
		}
	case 2:
		slotIndex, err := file.NewIndex[common.SlotIdxKey[uint32], uint32](slotsIndexPath, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapbtree.NewMultiMap[uint32, uint32](common.Identifier32Serializer{}, common.Identifier32Serializer{}, common.Uint32Comparator{}, common.Uint32Comparator{})

		schema = &GoSchema2{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdxKey[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			addressToSlots,
			nil,
		}
	case 3:
		slotIndex, err := file.NewIndex[common.SlotIdxKey[uint32], uint32](slotsIndexPath, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
		if err != nil {
			return nil, err
		}
		reincStorePath := indexPath + string(filepath.Separator) + "reinc"
		if err = os.MkdirAll(reincStorePath, 0700); err != nil {
			return nil, err
		}
		reincarnationsStore, err := pagedfile.NewStore[uint32, common.Reincarnation](reincStorePath, common.ReincarnationSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(reincStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := pagedfile.NewStore[uint32, common.SlotReincValue](valuesStorePath, common.SlotReincValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
		if err != nil {
			return nil, err
		}

		schema = &GoSchema3{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdxKey[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Reincarnation](reincarnationsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.SlotReincValue](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			nil,
		}
	default:
		return nil, fmt.Errorf("the go implementation only supports schemas 1,2,3")
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup})
	return state, nil
}

// NewGoLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func NewGoLeveLIndexAndStoreState(params Parameters) (State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	db, err := common.OpenLevelDb(params.Directory, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, common.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	codesDepot, err := ldbDepot.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), common.PageSize)
	if err != nil {
		return nil, err
	}

	var schema GoSchema
	switch params.Schema {
	case 1:
		slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32Serializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		schema = &GoSchema1{
			addressIndex,
			keyIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 2:
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		schema = &GoSchema2{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			addressToSlots,
			nil,
		}
	case 3:
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		reincarnationsStore, err := ldbstore.NewStore[uint32, common.Reincarnation](db, common.ReincarnationStoreKey, common.ReincarnationSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ReincarnationStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.SlotReincValue](db, common.ValueStoreKey, common.SlotReincValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}

		schema = &GoSchema3{
			addressIndex,
			slotIndex,
			accountsStore,
			noncesStore,
			balancesStore,
			reincarnationsStore,
			valuesStore,
			codesDepot,
			codeHashesStore,
			nil,
		}
	default:
		return nil, fmt.Errorf("the go implementation only supports schemas 1,2,3")
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup, cleanUpByClosing(db)})
	return state, nil
}

// NewGoCachedLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func NewGoCachedLeveLIndexAndStoreState(params Parameters) (State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	db, err := common.OpenLevelDb(params.Directory, nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, common.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, common.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	codesDepot, err := ldbDepot.NewDepot[uint32](db, common.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), common.PageSize)
	if err != nil {
		return nil, err
	}

	var schema GoSchema
	switch params.Schema {
	case 1:
		slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32Serializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		keyIndex, err := ldb.NewIndex[common.Key, uint32](db, common.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		schema = &GoSchema1{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.Key, uint32](keyIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdx[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			addressToSlots,
			nil,
		}
	case 2:
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		schema = &GoSchema2{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdxKey[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Value](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			addressToSlots,
			nil,
		}
	case 3:
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, common.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		reincarnationsStore, err := ldbstore.NewStore[uint32, common.Reincarnation](db, common.ReincarnationStoreKey, common.ReincarnationSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ReincarnationStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.SlotReincValue](db, common.ValueStoreKey, common.SlotReincValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}

		schema = &GoSchema3{
			cachedIndex.NewIndex[common.Address, uint32](addressIndex, CacheCapacity),
			cachedIndex.NewIndex[common.SlotIdxKey[uint32], uint32](slotIndex, CacheCapacity),
			cachedStore.NewStore[uint32, common.AccountState](accountsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Nonce](noncesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Balance](balancesStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.Reincarnation](reincarnationsStore, CacheCapacity),
			cachedStore.NewStore[uint32, common.SlotReincValue](valuesStore, CacheCapacity),
			cachedDepot.NewDepot[uint32](codesDepot, CacheCapacity, CacheCapacity),
			cachedStore.NewStore[uint32, common.Hash](codeHashesStore, CacheCapacity),
			nil,
		}
	default:
		return nil, fmt.Errorf("the go implementation only supports schemas 1,2,3")
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup, cleanUpByClosing(db)})
	return state, nil
}

// createSubDirs creates two subdirectories of the given for the Store and the Index
func createSubDirs(rootPath string) (indexPath, storePath string, err error) {
	indexPath = rootPath + string(filepath.Separator) + "index"
	if err = os.MkdirAll(indexPath, 0700); err != nil {
		return
	}

	storePath = rootPath + string(filepath.Separator) + "store"
	if err = os.MkdirAll(storePath, 0700); err != nil {
		return
	}

	return
}

// cleanUpByClosing provides a clean-up function, which ensure closing the resource on the state clean-up
func cleanUpByClosing(db io.Closer) func() {
	return func() {
		_ = db.Close()
	}
}

func openArchive(params Parameters) (archive archive.Archive, cleanup func(), err error) {

	getArchivePath := func() (string, error) {
		path := params.Directory + string(filepath.Separator) + "archive"
		return path, os.MkdirAll(path, 0700)
	}

	switch params.Archive {

	case NoArchive:
		return nil, nil, nil

	case LevelDbArchive:
		path, err := getArchivePath()
		if err != nil {
			return nil, nil, err
		}
		db, err := common.OpenLevelDb(path, nil)
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() { _ = db.Close() }
		arch, err := archldb.NewArchive(db)
		return arch, cleanup, err

	case SqliteArchive:
		path, err := getArchivePath()
		if err != nil {
			return nil, nil, err
		}
		arch, err := sqlite.NewArchive(path + string(filepath.Separator) + "archive.sqlite")
		return arch, nil, err
	}
	return nil, nil, fmt.Errorf("unknown archive type: %v", params.Archive)
}
