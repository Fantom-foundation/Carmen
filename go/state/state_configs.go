package state

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	archldb "github.com/Fantom-foundation/Carmen/go/backend/archive/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/sqlite"
	"io"
	"os"
	"path/filepath"

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

// Parameters struct defining configuration parameters for state instances.
type Parameters struct {
	Directory   string
	WithArchive bool
}

// NewGoMemoryState creates in memory implementation
// (path parameter for compatibility with other state factories, can be left empty)
func NewGoMemoryState(params Parameters) (State, error) {
	if params.WithArchive {
		return nil, fmt.Errorf("archive mode not supported yet in go-memory variant")
	}
	addressIndex := indexmem.NewIndex[common.Address, uint32](common.AddressSerializer{})
	slotIndex := indexmem.NewIndex[common.SlotIdx[uint32], uint32](common.SlotIdxSerializer32{})
	keyIndex := indexmem.NewIndex[common.Key, uint32](common.KeySerializer{})

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
	valuesStore, err := storemem.NewStore[uint32, common.Value](common.ValueSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
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

	addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

	state := &GoState{
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
		nil, nil, nil, nil, nil, nil, nil}
	return state, nil
}

// NewGoFileState creates File based Index and Store implementations
func NewGoFileState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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
	slotIndex, err := file.NewIndex[common.SlotIdx[uint32], uint32](slotsIndexPath, common.SlotIdxSerializer32{}, common.Identifier32Serializer{}, common.SlotIdxHasher{}, common.Identifier32Comparator{})
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
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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
	addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(archivePath + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
	}

	state := &GoState{
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
		nil, nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoCachedFileState creates File based Index and Store implementations
func NewGoCachedFileState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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
	slotIndex, err := file.NewIndex[common.SlotIdx[uint32], uint32](slotsIndexPath, common.SlotIdxSerializer32{}, common.Identifier32Serializer{}, common.SlotIdxHasher{}, common.Identifier32Comparator{})
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
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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
	addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(archivePath + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		nil, nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoCachedFileLdbArchiveState creates File based Index and Store implementations with LevelDB based archive (if enabled)
func NewGoCachedFileLdbArchiveState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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
	slotIndex, err := file.NewIndex[common.SlotIdx[uint32], uint32](slotsIndexPath, common.SlotIdxSerializer32{}, common.Identifier32Serializer{}, common.SlotIdxHasher{}, common.Identifier32Comparator{})
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
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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
	addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

	var arch archive.Archive
	var cleanup []func() = nil
	if params.WithArchive {
		db, err := common.OpenLevelDb(archivePath, nil)
		if err != nil {
			return nil, err
		}
		cleanup = cleanUpByClosing(db)

		arch, err = archldb.NewArchive(db)
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		cleanup, nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoLeveLIndexFileStoreState creates LevelDB Index and File Store implementations
func NewGoLeveLIndexFileStoreState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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
	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(archivePath + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
	}

	state := &GoState{
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
		cleanUpByClosing(db), nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoCachedLeveLIndexFileStoreState creates Cached LevelDB Index and File Store implementations
func NewGoCachedLeveLIndexFileStoreState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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

	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(archivePath + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		cleanUpByClosing(db), nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoCachedTransactLeveLIndexFileStoreState creates Cached and Transactional LevelDB Index and File Store implementations
func NewGoCachedTransactLeveLIndexFileStoreState(params Parameters) (State, error) {
	indexPath, storePath, archivePath, err := createSubDirs(params.Directory)
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

	valuesStorePath := storePath + string(filepath.Separator) + "values"
	if err = os.MkdirAll(valuesStorePath, 0700); err != nil {
		return nil, err
	}
	valuesStore, err := pagedfile.NewStore[uint32, common.Value](valuesStorePath, common.ValueSerializer{}, common.PageSize, htfile.CreateHashTreeFactory(valuesStorePath, HashTreeFactor), PoolSize)
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

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](tx, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(archivePath + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		cleanup, nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func NewGoLeveLIndexAndStoreState(params Parameters) (State, error) {
	db, err := common.OpenLevelDb(params.Directory, nil)
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
	valueHashTreeFactory := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, common.PageSize)
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

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = archldb.NewArchive(db)
		if err != nil {
			return nil, err
		}
	}

	state := &GoState{
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
		cleanUpByClosing(db), nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoCachedLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func NewGoCachedLeveLIndexAndStoreState(params Parameters) (State, error) {
	db, err := common.OpenLevelDb(params.Directory, nil)
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
	valueHashTreeFactory := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, common.PageSize)
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

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = archldb.NewArchive(db)
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		cleanUpByClosing(db), nil, arch, nil, nil, nil, nil}

	return state, nil
}

// NewGoTransactCachedLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func NewGoTransactCachedLeveLIndexAndStoreState(params Parameters) (State, error) {
	opts := opt.Options{WriteBuffer: TransactBufferMB}
	db, err := common.OpenLevelDb(params.Directory, &opts)
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
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](tx, common.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](tx, common.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](tx, common.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	valueHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.ValueStoreKey, HashTreeFactor)
	valuesStore, err := ldbstore.NewStore[uint32, common.Value](tx, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, valueHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}

	depotHashTreeFactory := htldb.CreateHashTreeFactory(tx, common.DepotCodeKey, HashTreeFactor)
	codesDepot, err := ldbDepot.NewDepot[uint32](tx, common.DepotCodeKey, common.Identifier32Serializer{}, depotHashTreeFactory, CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](tx, common.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), common.PageSize)
	if err != nil {
		return nil, err
	}

	addressToSlots := mapldb.NewMultiMap[uint32, uint32](tx, common.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

	var arch archive.Archive
	if params.WithArchive {
		arch, err = sqlite.NewArchive(params.Directory + string(filepath.Separator) + "archive.sqlite")
		if err != nil {
			return nil, err
		}
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
		addressToSlots,
		cleanup, nil, arch, nil, nil, nil, nil}

	return state, nil
}

// createSubDirs creates two subdirectories of the given for the Store and the Index
func createSubDirs(rootPath string) (indexPath, storePath, archivePath string, err error) {
	indexPath = rootPath + string(filepath.Separator) + "index"
	if err = os.MkdirAll(indexPath, 0700); err != nil {
		return
	}

	storePath = rootPath + string(filepath.Separator) + "store"
	if err = os.MkdirAll(storePath, 0700); err != nil {
		return
	}

	archivePath = rootPath + string(filepath.Separator) + "archive"
	err = os.MkdirAll(archivePath, 0700)

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
