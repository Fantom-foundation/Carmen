package state

import (
	archldb "github.com/Fantom-foundation/Carmen/go/backend/archive/ldb"
	cachedDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/cache"
	fileDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/file"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	cachedIndex "github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	cachedStore "github.com/Fantom-foundation/Carmen/go/backend/store/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"path/filepath"
)

// CacheCapacity is the size of the cache expressed as the number of cached keys
const CacheCapacity = 1 << 20 // 2 ^ 20 keys -> 32MB for 32-bytes keys

// PoolSize is the maximum amount of data pages loaded in memory for the paged file store
const PoolSize = 100000

// CodeHashGroupSize represents the number of codes grouped together in depots to form one leaf node of the hash tree.
const CodeHashGroupSize = 4

// NewScheme3GoFileLdbArchiveState creates File based Index and Store implementations,
// using the Scheme 3 and LDB Archive.
func NewScheme3GoFileLdbArchiveState(path string) (State, error) {
	indexPath := path + string(filepath.Separator) + "index"
	if err := os.MkdirAll(indexPath, 0700); err != nil {
		return nil, err
	}
	storePath := path + string(filepath.Separator) + "store"
	if err := os.MkdirAll(storePath, 0700); err != nil {
		return nil, err
	}
	addressIndexPath := indexPath + string(filepath.Separator) + "addresses"
	if err := os.MkdirAll(addressIndexPath, 0700); err != nil {
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
	slotIndex, err := file.NewIndex[common.SlotIdxKey[uint32], uint32](slotsIndexPath, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{}, common.SlotIdx32KeyHasher{}, common.SlotIdx32KeyComparator{})
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

	reincStorePath := storePath + string(filepath.Separator) + "reincarnation"
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

	schema := &GoSchema3{
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

	archivePath := path + string(filepath.Separator) + "archive"
	if err := os.MkdirAll(storePath, 0700); err != nil {
		return nil, err
	}

	db, err := common.OpenLevelDb(archivePath, nil)
	if err != nil {
		return nil, err
	}
	archiveCleanup := func() { _ = db.Close() }
	arch, err := archldb.NewArchive(db)
	if err != nil {
		return nil, err
	}

	state := NewGoState(schema, arch, []func(){archiveCleanup})
	return state, nil
}
