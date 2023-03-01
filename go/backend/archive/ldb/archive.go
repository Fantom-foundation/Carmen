package ldb

import (
	"crypto/sha256"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sync"
	"unsafe"
)

type Archive struct {
	db                       *common.LevelDbMemoryFootprintWrapper
	reincarnationNumberCache map[common.Address]int
	batch                    leveldb.Batch
	lastBlockCache           blockCache
	addMutex                 sync.Mutex
}

func NewArchive(db *common.LevelDbMemoryFootprintWrapper) (*Archive, error) {
	return &Archive{
		db:                       db,
		reincarnationNumberCache: map[common.Address]int{},
	}, nil
}

func (a *Archive) Close() error {
	// no-op
	return nil
}

// Add a new update as a new block into the archive. Should be called from a single thread only.
func (a *Archive) Add(block uint64, update common.Update) error {
	a.addMutex.Lock()
	defer a.addMutex.Unlock()

	lastBlock, lastHash, err := a.getLastBlock()
	if err != nil && err != leveldb.ErrNotFound {
		return fmt.Errorf("failed to get preceding block hash; %s", err)
	}
	if block <= lastBlock && err != leveldb.ErrNotFound {
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

			lastAccountHash, err := a.GetAccountHash(block, account)
			if err != nil {
				return fmt.Errorf("failed to get previous account hash; %s", err)
			}
			accountUpdateHash := accountUpdate.GetHash(reusedHasher)

			reusedHasher.Reset()
			reusedHasher.Write(lastAccountHash[:])
			reusedHasher.Write(accountUpdateHash[:])
			newAccountHash := reusedHasher.Sum(nil)
			blockHasher.Write(newAccountHash)

			var accountK accountBlockKey
			accountK.set(common.AccountHashArchiveKey, account, block)
			a.batch.Put(accountK[:], newAccountHash)
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
		accountK.set(common.AccountArchiveKey, account, block)
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
		accountK.set(common.AccountArchiveKey, account, block)
		var accountStatusV accountStatusValue
		accountStatusV.set(true, reincarnation+1)
		a.batch.Put(accountK[:], accountStatusV[:])
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	for _, balanceUpdate := range update.Balances {
		var accountK accountBlockKey
		accountK.set(common.BalanceArchiveKey, balanceUpdate.Account, block)
		a.batch.Put(accountK[:], balanceUpdate.Balance[:])
	}

	for _, codeUpdate := range update.Codes {
		var accountK accountBlockKey
		accountK.set(common.CodeArchiveKey, codeUpdate.Account, block)
		a.batch.Put(accountK[:], codeUpdate.Code[:])
	}

	for _, nonceUpdate := range update.Nonces {
		var accountK accountBlockKey
		accountK.set(common.NonceArchiveKey, nonceUpdate.Account, block)
		a.batch.Put(accountK[:], nonceUpdate.Nonce[:])
	}

	for _, slotUpdate := range update.Slots {
		reincarnation, err := getReincarnationNumber(slotUpdate.Account) // use changes from status updates above
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var slotK accountKeyBlockKey
		slotK.set(common.StorageArchiveKey, slotUpdate.Account, reincarnation, slotUpdate.Key, block)
		a.batch.Put(slotK[:], slotUpdate.Value[:])
	}

	return nil
}

// getLastBlock provides info about the last completely written block
func (a *Archive) getLastBlock() (number uint64, hash common.Hash, err error) {
	number, hash = a.lastBlockCache.get()
	if number != 0 {
		return number, hash, nil
	}
	number, hash, err = a.getLastBlockSlow()
	return number, hash, err
}

// getLastBlockSlow represents the slow path of getLastBlock() method (extracted to allow inlining the fast path)
func (a *Archive) getLastBlockSlow() (number uint64, hash common.Hash, err error) {
	keyRange := getBlockKeyRangeFromHighest()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		var blockK blockKey
		copy(blockK[:], it.Key())
		copy(hash[:], it.Value())
		return blockK.get(), hash, nil
	}
	err = it.Error()
	if err == nil {
		err = leveldb.ErrNotFound
	}
	return 0, common.Hash{}, err
}

func (a *Archive) GetLastBlockHeight() (block uint64, err error) {
	block, _, err = a.getLastBlock()
	return block, err
}

func (a *Archive) getStatus(block uint64, account common.Address) (exists bool, reincarnation int, err error) {
	var key accountBlockKey
	key.set(common.AccountArchiveKey, account, block)
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

func (a *Archive) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	var key accountBlockKey
	key.set(common.BalanceArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(balance[:], it.Value())
		return balance, nil
	}
	return common.Balance{}, it.Error()
}

func (a *Archive) GetCode(block uint64, account common.Address) (code []byte, err error) {
	var key accountBlockKey
	key.set(common.CodeArchiveKey, account, block)
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
	key.set(common.NonceArchiveKey, account, block)
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
	key.set(common.StorageArchiveKey, account, reincarnation, slot, block)
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

func (a *Archive) GetAccountHash(block uint64, account common.Address) (hash common.Hash, err error) {
	var key accountBlockKey
	key.set(common.AccountHashArchiveKey, account, block)
	keyRange := key.getRange()
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(hash[:], it.Value())
		return hash, nil
	}
	return common.Hash{}, it.Error()
}

// GetMemoryFootprint provides the size of the archive in memory in bytes
func (a *Archive) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	var address common.Address
	var reincarnation int
	mf.AddChild("reincarnationNumberCache", common.NewMemoryFootprint(uintptr(len(a.reincarnationNumberCache))*(unsafe.Sizeof(address)+unsafe.Sizeof(reincarnation))))
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
