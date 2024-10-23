// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package ldb

import (
	"crypto/sha256"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/witness"

	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Archive struct {
	db                       *backend.LevelDbMemoryFootprintWrapper
	reincarnationNumberCache map[common.Address]int
	accountHashCache         *common.LruCache[common.Address, common.Hash]
	batch                    leveldb.Batch
	lastBlockCache           blockCache
	addMutex                 sync.Mutex
}

func NewArchive(db *backend.LevelDbMemoryFootprintWrapper) (*Archive, error) {
	return &Archive{
		db:                       db,
		reincarnationNumberCache: map[common.Address]int{},
		accountHashCache:         common.NewLruCache[common.Address, common.Hash](100_000),
	}, nil
}

func (a *Archive) Flush() error {
	// nothing to do
	return nil
}

func (a *Archive) Close() error {
	a.accountHashCache.Clear()
	return nil
}

// Add a new update as a new block into the archive. Should be called from a single thread only.
func (a *Archive) Add(block uint64, update common.Update, _ any) error {
	a.addMutex.Lock()
	defer a.addMutex.Unlock()

	lastBlock, isEmpty, lastHash, err := a.getLastBlock()
	if err != nil {
		return fmt.Errorf("failed to get preceding block hash; %s", err)
	}
	if !isEmpty && block <= lastBlock {
		return fmt.Errorf("unable to add block %d, is higher or equal to already present block %d", block, lastBlock)
	}

	a.batch.Reset()
	var blockHash common.Hash
	if update.IsEmpty() {
		blockHash = lastHash
	} else {
		err := a.addUpdateIntoBatch(block, update)
		if err != nil {
			return err
		}

		blockHasher := sha256.New()
		blockHasher.Write(lastHash[:])

		reusedHasher := sha256.New()
		updatedAccounts, accountUpdates := archive.AccountUpdatesFrom(&update)
		for _, account := range updatedAccounts {
			accountUpdate := accountUpdates[account]

			lastAccountHash, err := a.getLastAccountHash(account)
			if err != nil {
				return fmt.Errorf("failed to get previous account hash; %s", err)
			}
			accountUpdateHash := accountUpdate.GetHash(reusedHasher)

			reusedHasher.Reset()
			reusedHasher.Write(lastAccountHash[:])
			reusedHasher.Write(accountUpdateHash[:])
			hash := reusedHasher.Sum(nil)
			newAccountHash := *(*common.Hash)(hash)
			blockHasher.Write(newAccountHash[:])

			var accountK accountBlockKey
			accountK.set(backend.AccountHashArchiveKey, account, block)
			a.batch.Put(accountK[:], newAccountHash[:])
			a.accountHashCache.Set(account, newAccountHash)
		}

		hash := blockHasher.Sum(nil)
		copy(blockHash[:], hash)
	}

	var blockK blockKey
	blockK.set(block)
	a.batch.Put(blockK[:], blockHash[:])

	err = a.db.Write(&a.batch, nil)
	if err != nil {
		return err
	}

	a.lastBlockCache.set(block, blockHash)
	return nil
}

func (a *Archive) addUpdateIntoBatch(block uint64, update common.Update) error {
	// helper function for obtaining current reincarnation number of an account
	getReincarnationNumber := func(account common.Address) (int, error) {
		if res, exists := a.reincarnationNumberCache[account]; exists {
			return res, nil
		}
		_, reincarnation, err := a.getStatus(block, account)
		if err != nil {
			return 0, err
		}
		a.reincarnationNumberCache[account] = reincarnation
		return reincarnation, err
	}

	for _, account := range update.DeletedAccounts {
		reincarnation, err := getReincarnationNumber(account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var accountK accountBlockKey
		accountK.set(backend.AccountArchiveKey, account, block)
		var accountStatusV accountStatusValue
		accountStatusV.set(false, reincarnation+1)
		a.batch.Put(accountK[:], accountStatusV[:])
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	for _, account := range update.CreatedAccounts {
		reincarnation, err := getReincarnationNumber(account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var accountK accountBlockKey
		accountK.set(backend.AccountArchiveKey, account, block)
		var accountStatusV accountStatusValue
		accountStatusV.set(true, reincarnation+1)
		a.batch.Put(accountK[:], accountStatusV[:])
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	for _, balanceUpdate := range update.Balances {
		var accountK accountBlockKey
		accountK.set(backend.BalanceArchiveKey, balanceUpdate.Account, block)
		b := balanceUpdate.Balance.Bytes32()
		a.batch.Put(accountK[:], b[:])
	}

	for _, codeUpdate := range update.Codes {
		var accountK accountBlockKey
		accountK.set(backend.CodeArchiveKey, codeUpdate.Account, block)
		a.batch.Put(accountK[:], codeUpdate.Code[:])
	}

	for _, nonceUpdate := range update.Nonces {
		var accountK accountBlockKey
		accountK.set(backend.NonceArchiveKey, nonceUpdate.Account, block)
		a.batch.Put(accountK[:], nonceUpdate.Nonce[:])
	}

	for _, slotUpdate := range update.Slots {
		reincarnation, err := getReincarnationNumber(slotUpdate.Account) // use changes from status updates above
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var slotK accountKeyBlockKey
		slotK.set(backend.StorageArchiveKey, slotUpdate.Account, reincarnation, slotUpdate.Key, block)
		a.batch.Put(slotK[:], slotUpdate.Value[:])
	}

	return nil
}

// getLastBlock provides info about the last completely written block
func (a *Archive) getLastBlock() (number uint64, empty bool, hash common.Hash, err error) {
	number, hash = a.lastBlockCache.get()
	if number != 0 {
		return number, false, hash, nil
	}
	number, empty, hash, err = a.getLastBlockSlow()
	return number, empty, hash, err
}

// getLastBlockSlow represents the slow path of getLastBlock() method (extracted to allow inlining the fast path)
func (a *Archive) getLastBlockSlow() (number uint64, empty bool, hash common.Hash, err error) {
	keyRange := getBlockKeyRangeFromHighest()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		var blockK blockKey
		copy(blockK[:], it.Key())
		copy(hash[:], it.Value())
		return blockK.get(), false, hash, nil
	}
	return 0, true, common.Hash{}, it.Error()
}

func (a *Archive) GetBlockHeight() (block uint64, empty bool, err error) {
	block, empty, _, err = a.getLastBlock()
	return block, empty, err
}

func (a *Archive) getStatus(block uint64, account common.Address) (exists bool, reincarnation int, err error) {
	var key accountBlockKey
	key.set(backend.AccountArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, &opt.ReadOptions{})
	defer it.Release()

	if it.Next() {
		var accountStatusV accountStatusValue
		copy(accountStatusV[:], it.Value())
		exists, reincarnation = accountStatusV.get()
		return exists, reincarnation, nil
	}
	return false, 0, it.Error()
}

func (a *Archive) Exists(block uint64, account common.Address) (exists bool, err error) {
	exists, _, err = a.getStatus(block, account)
	return exists, err
}

func (a *Archive) GetBalance(block uint64, account common.Address) (balance amount.Amount, err error) {
	var key accountBlockKey
	key.set(backend.BalanceArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		balance = amount.NewFromBytes(it.Value()[:]...)
		return balance, nil
	}
	return amount.New(), it.Error()
}

func (a *Archive) GetCode(block uint64, account common.Address) (code []byte, err error) {
	var key accountBlockKey
	key.set(backend.CodeArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		code = make([]byte, len(it.Value()))
		copy(code, it.Value())
		return code, nil
	}
	return nil, it.Error()
}

func (a *Archive) GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error) {
	var key accountBlockKey
	key.set(backend.NonceArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(nonce[:], it.Value())
		return nonce, nil
	}
	return common.Nonce{}, it.Error()
}

func (a *Archive) GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error) {
	accountExists, reincarnation, err := a.getStatus(block, account)
	if !accountExists || err != nil {
		return common.Value{}, err
	}

	var key accountKeyBlockKey
	key.set(backend.StorageArchiveKey, account, reincarnation, slot, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(value[:], it.Value())
		return value, nil
	}
	return common.Value{}, it.Error()
}

func (a *Archive) GetHash(block uint64) (hash common.Hash, err error) {
	var blockK blockKey
	blockK.set(block)
	hashBytes, err := a.db.Get(blockK[:], nil)
	copy(hash[:], hashBytes)
	return hash, err
}

func (a *Archive) getLastAccountHash(account common.Address) (hash common.Hash, err error) {
	hash, exists := a.accountHashCache.Get(account)
	if !exists {
		hash, err = a.GetAccountHash(maxBlock, account)
		if err == nil {
			a.accountHashCache.Set(account, hash)
		}
	}
	return hash, err
}

func (a *Archive) GetAccountHash(block uint64, account common.Address) (hash common.Hash, err error) {
	var key accountBlockKey
	key.set(backend.AccountHashArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(hash[:], it.Value())
		return hash, nil
	}
	return common.Hash{}, it.Error()
}

func (a *Archive) CreateWitnessProof(_ uint64, _ common.Address, _ ...common.Key) (witness.Proof, error) {
	return nil, archive.ErrWitnessProofNotSupported
}

func (a *Archive) HasEmptyStorage(_ uint64, _ common.Address) (bool, error) {
	return false, archive.ErrEmptyStorageNotSupported
}

// GetMemoryFootprint provides the size of the archive in memory in bytes
func (a *Archive) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	var address common.Address
	var reincarnation int
	mf.AddChild("reincarnationNumberCache", common.NewMemoryFootprint(uintptr(len(a.reincarnationNumberCache))*(unsafe.Sizeof(address)+unsafe.Sizeof(reincarnation))))
	mf.AddChild("accountHashCache", a.accountHashCache.GetMemoryFootprint(0))
	return mf
}

// blockCache caches info about the last block in the archive
type blockCache struct {
	mu            sync.Mutex
	lastBlockNum  uint64
	lastBlockHash common.Hash
}

func (c *blockCache) set(number uint64, hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastBlockNum = number
	c.lastBlockHash = hash
}

func (c *blockCache) get() (number uint64, hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastBlockNum, c.lastBlockHash
}
