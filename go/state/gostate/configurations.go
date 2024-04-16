//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package gostate

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"

	"io"
	"os"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/sqlite"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/syndtr/goleveldb/leveldb/opt"

	archldb "github.com/Fantom-foundation/Carmen/go/backend/archive/ldb"
	cachedDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/cache"
	fileDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/file"
	ldbDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/ldb"
	cachedIndex "github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	indexmem "github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	mapbtree "github.com/Fantom-foundation/Carmen/go/backend/multimap/btreemem"
	mapldb "github.com/Fantom-foundation/Carmen/go/backend/multimap/ldb"
	mapmem "github.com/Fantom-foundation/Carmen/go/backend/multimap/memory"
	cachedStore "github.com/Fantom-foundation/Carmen/go/backend/store/cache"
	ldbstore "github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	storemem "github.com/Fantom-foundation/Carmen/go/backend/store/memory"
)

const HashTreeFactor = 32

// CacheCapacity is the size of the cache expressed as the number of cached keys
const CacheCapacity = 1 << 20 // 2 ^ 20 keys -> 32MB for 32-bytes keys

// TransactBufferMB is the size of buffer before the transaction is flushed expressed in MBs
const TransactBufferMB = 128 * opt.MiB

// PoolSize is the maximum amount of data pages loaded in memory for the paged file store
const PoolSize = 100000

// CodeHashGroupSize represents the number of codes grouped together in depots to form one leaf node of the hash tree.
const CodeHashGroupSize = 4

const defaultSchema = state.Schema(1)

const (
	VariantGoMemory         state.Variant = "go-memory"
	VariantGoFile           state.Variant = "go-file"
	VariantGoFileNoCache    state.Variant = "go-file-nocache"
	VariantGoLevelDb        state.Variant = "go-ldb"
	VariantGoLevelDbNoCache state.Variant = "go-ldb-nocache"
)

func init() {
	generallySupportedArchives := []state.ArchiveType{
		state.NoArchive,
		state.LevelDbArchive,
		state.SqliteArchive,
	}

	// Register all configuration options supported by the Go implementation.
	// TODO [cleanup]: break this up on a per schema basis
	for schema := state.Schema(1); schema <= state.Schema(5); schema++ {
		for _, archive := range generallySupportedArchives {
			state.RegisterStateFactory(state.Configuration{
				Variant: VariantGoMemory,
				Schema:  schema,
				Archive: archive,
			}, newGoMemoryState)
			state.RegisterStateFactory(state.Configuration{
				Variant: VariantGoFile,
				Schema:  schema,
				Archive: archive,
			}, newGoCachedFileState)

			if schema < state.Schema(3) {
				state.RegisterStateFactory(state.Configuration{
					Variant: VariantGoFileNoCache,
					Schema:  schema,
					Archive: archive,
				}, newGoFileState)
				state.RegisterStateFactory(state.Configuration{
					Variant: VariantGoLevelDb,
					Schema:  schema,
					Archive: archive,
				}, newGoCachedLeveLIndexAndStoreState)
				state.RegisterStateFactory(state.Configuration{
					Variant: VariantGoLevelDbNoCache,
					Schema:  schema,
					Archive: archive,
				}, newGoLeveLIndexAndStoreState)
			}
		}
	}

	mptSetups := []struct {
		schema  state.Schema
		archive state.ArchiveType
	}{
		{4, state.S4Archive},
		{5, state.S5Archive},
	}

	for _, setup := range mptSetups {
		state.RegisterStateFactory(state.Configuration{
			Variant: VariantGoMemory,
			Schema:  setup.schema,
			Archive: setup.archive,
		}, newGoMemoryState)

		state.RegisterStateFactory(state.Configuration{
			Variant: VariantGoFile,
			Schema:  setup.schema,
			Archive: setup.archive,
		}, newGoFileState)
	}

}

// newGoMemoryState creates in memory implementation
// (path parameter for compatibility with other state factories, can be left empty)
func newGoMemoryState(params state.Parameters) (state.State, error) {
	_, err := getLiveDbPath(params)
	if err != nil {
		return nil, err
	}
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	if params.Schema == 4 {
		return newGoMemoryS4State(params)
	}
	if params.Schema == 5 {
		return newGoMemoryS5State(params)
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

	var live state.LiveDB
	switch params.Schema {
	case 1:
		slotIndex := indexmem.NewIndex[common.SlotIdx[uint32], uint32](common.SlotIdx32Serializer{})
		keyIndex := indexmem.NewIndex[common.Key, uint32](common.KeySerializer{})
		valuesStore, err := storemem.NewStore[uint32, common.Value](common.ValueSerializer{}, common.PageSize, htmemory.CreateHashTreeFactory(HashTreeFactor))
		if err != nil {
			return nil, err
		}
		addressToSlots := mapmem.NewMultiMap[uint32, uint32]()

		live = &GoSchema1{
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

		live = &GoSchema2{
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

		live = &GoSchema3{
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
		return nil, fmt.Errorf("%w: the go implementation only supports schemas 1-5, got %d", state.UnsupportedConfiguration, params.Schema)
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := newGoState(live, arch, []func(){archiveCleanup})
	return state, nil
}

// newGoFileState creates File based Index and Store implementations
func newGoFileState(params state.Parameters) (state.State, error) {
	path, err := getLiveDbPath(params)
	if err != nil {
		return nil, err
	}
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	if params.Schema == 4 {
		return newGoFileS4State(params)
	}
	if params.Schema == 5 {
		return newGoFileS5State(params)
	}

	indexPath, storePath, err := createSubDirs(path)
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

	var live state.LiveDB
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

		live = &GoSchema1{
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

		live = &GoSchema2{
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

		live = &GoSchema3{
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
		return nil, fmt.Errorf("%w: the go implementation only supports schemas 1-5, got %d", state.UnsupportedConfiguration, params.Schema)
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := newGoState(live, arch, []func(){archiveCleanup})
	return state, nil
}

// newGoCachedFileState creates File based Index and Store implementations
func newGoCachedFileState(params state.Parameters) (state.State, error) {
	path, err := getLiveDbPath(params)
	if err != nil {
		return nil, err
	}
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	if params.Schema == 4 {
		return newGoFileS4State(params)
	}
	if params.Schema == 5 {
		return newGoFileS5State(params)
	}

	indexPath, storePath, err := createSubDirs(path)
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

	var live state.LiveDB
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

		live = &GoSchema1{
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

		live = &GoSchema2{
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

		live = &GoSchema3{
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
		return nil, fmt.Errorf("%w: the go implementation only supports schemas 1-5, got %d", state.UnsupportedConfiguration, params.Schema)
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := newGoState(live, arch, []func(){archiveCleanup})
	return state, nil
}

// newGoLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func newGoLeveLIndexAndStoreState(params state.Parameters) (state.State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	db, err := backend.OpenLevelDb(params.Directory+string(filepath.Separator)+"live", nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, backend.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, backend.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, backend.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, backend.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	codesDepot, err := ldbDepot.NewDepot[uint32](db, backend.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, backend.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), common.PageSize)
	if err != nil {
		return nil, err
	}

	var live state.LiveDB
	switch params.Schema {
	case 1:
		slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32Serializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		keyIndex, err := ldb.NewIndex[common.Key, uint32](db, backend.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, backend.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, backend.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		live = &GoSchema1{
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
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, backend.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, backend.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		live = &GoSchema2{
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
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		reincarnationsStore, err := ldbstore.NewStore[uint32, common.Reincarnation](db, backend.ReincarnationStoreKey, common.ReincarnationSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ReincarnationStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.SlotReincValue](db, backend.ValueStoreKey, common.SlotReincValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}

		live = &GoSchema3{
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
		return nil, fmt.Errorf("%w: the go implementation only supports schemas 1,2,3, got %d", state.UnsupportedConfiguration, params.Schema)
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := newGoState(live, arch, []func(){archiveCleanup, cleanUpByClosing(db)})
	return state, nil
}

// newGoCachedLeveLIndexAndStoreState creates Index and Store both backed up by the leveldb
func newGoCachedLeveLIndexAndStoreState(params state.Parameters) (state.State, error) {
	if params.Schema == 0 {
		params.Schema = defaultSchema
	}
	db, err := backend.OpenLevelDb(params.Directory+string(filepath.Separator)+"live", nil)
	if err != nil {
		return nil, err
	}
	addressIndex, err := ldb.NewIndex[common.Address, uint32](db, backend.AddressIndexKey, common.AddressSerializer{}, common.Identifier32Serializer{})
	if err != nil {
		return nil, err
	}
	accountHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.AccountStoreKey, HashTreeFactor)
	accountsStore, err := ldbstore.NewStore[uint32, common.AccountState](db, backend.AccountStoreKey, common.AccountStateSerializer{}, common.Identifier32Serializer{}, accountHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	nonceHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.NonceStoreKey, HashTreeFactor)
	noncesStore, err := ldbstore.NewStore[uint32, common.Nonce](db, backend.NonceStoreKey, common.NonceSerializer{}, common.Identifier32Serializer{}, nonceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	balanceHashTreeFactory := htldb.CreateHashTreeFactory(db, backend.BalanceStoreKey, HashTreeFactor)
	balancesStore, err := ldbstore.NewStore[uint32, common.Balance](db, backend.BalanceStoreKey, common.BalanceSerializer{}, common.Identifier32Serializer{}, balanceHashTreeFactory, common.PageSize)
	if err != nil {
		return nil, err
	}
	codesDepot, err := ldbDepot.NewDepot[uint32](db, backend.DepotCodeKey, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.DepotCodeKey, HashTreeFactor), CodeHashGroupSize)
	if err != nil {
		return nil, err
	}
	codeHashesStore, err := ldbstore.NewStore[uint32, common.Hash](db, backend.CodeHashStoreKey, common.HashSerializer{}, common.Identifier32Serializer{}, hashtree.GetNoHashFactory(), common.PageSize)
	if err != nil {
		return nil, err
	}

	var live state.LiveDB
	switch params.Schema {
	case 1:
		slotIndex, err := ldb.NewIndex[common.SlotIdx[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32Serializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		keyIndex, err := ldb.NewIndex[common.Key, uint32](db, backend.KeyIndexKey, common.KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, backend.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, backend.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		live = &GoSchema1{
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
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.Value](db, backend.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		addressToSlots := mapldb.NewMultiMap[uint32, uint32](db, backend.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier32Serializer{})

		live = &GoSchema2{
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
		slotIndex, err := ldb.NewIndex[common.SlotIdxKey[uint32], uint32](db, backend.SlotLocIndexKey, common.SlotIdx32KeySerializer{}, common.Identifier32Serializer{})
		if err != nil {
			return nil, err
		}
		reincarnationsStore, err := ldbstore.NewStore[uint32, common.Reincarnation](db, backend.ReincarnationStoreKey, common.ReincarnationSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ReincarnationStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}
		valuesStore, err := ldbstore.NewStore[uint32, common.SlotReincValue](db, backend.ValueStoreKey, common.SlotReincValueSerializer{}, common.Identifier32Serializer{}, htldb.CreateHashTreeFactory(db, backend.ValueStoreKey, HashTreeFactor), common.PageSize)
		if err != nil {
			return nil, err
		}

		live = &GoSchema3{
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
		return nil, fmt.Errorf("%w: the go implementation only supports schemas 1,2,3, got %d", state.UnsupportedConfiguration, params.Schema)
	}

	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}

	state := newGoState(live, arch, []func(){archiveCleanup, cleanUpByClosing(db)})
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

func getLiveDbPath(params state.Parameters) (string, error) {
	path := filepath.Join(params.Directory, "live")
	return path, os.MkdirAll(path, 0700)
}

func getArchivePath(params state.Parameters) (string, error) {
	path := filepath.Join(params.Directory, "archive")
	return path, os.MkdirAll(path, 0700)
}

func openArchive(params state.Parameters) (archive archive.Archive, cleanup func(), err error) {
	switch params.Archive {

	case state.ArchiveType(""), state.NoArchive:
		return nil, nil, nil

	case state.LevelDbArchive:
		path, err := getArchivePath(params)
		if err != nil {
			return nil, nil, err
		}
		db, err := backend.OpenLevelDb(path, nil)
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() { _ = db.Close() }
		arch, err := archldb.NewArchive(db)
		return arch, cleanup, err

	case state.SqliteArchive:
		path, err := getArchivePath(params)
		if err != nil {
			return nil, nil, err
		}
		arch, err := sqlite.NewArchive(filepath.Join(path, "archive.sqlite"))
		return arch, nil, err

	case state.S4Archive:
		path, err := getArchivePath(params)
		if err != nil {
			return nil, nil, err
		}
		arch, err := mpt.OpenArchiveTrie(path, mpt.S4ArchiveConfig, mpt.DefaultMptStateCapacity)
		return arch, nil, err

	case state.S5Archive:
		path, err := getArchivePath(params)
		if err != nil {
			return nil, nil, err
		}
		arch, err := mpt.OpenArchiveTrie(path, mpt.S5ArchiveConfig, mptStateCapacity(params.ArchiveCache))
		return arch, nil, err
	}
	return nil, nil, fmt.Errorf("unknown archive type: %v", params.Archive)
}
