package ldb

import (
	"crypto/sha256"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"unsafe"
)

type Archive struct {
	db                       *common.LevelDbMemoryFootprintWrapper
	reincarnationNumberCache map[common.Address]int
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

func (a *Archive) Add(block uint64, update common.Update) error {
	batch := leveldb.Batch{}

	getReincarnationNumber := func(account common.Address) (int, error) {
		if res, exists := a.reincarnationNumberCache[account]; exists {
			return res, nil
		}
		_, reincarnation, err := a.getStatus(block, account)
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
		batch.Put(accountK[:], accountStatusV[:])
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
		batch.Put(accountK[:], accountStatusV[:])
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	for _, balanceUpdate := range update.Balances {
		var accountK accountBlockKey
		accountK.set(common.BalanceArchiveKey, balanceUpdate.Account, block)
		batch.Put(accountK[:], balanceUpdate.Balance[:])
	}

	for _, codeUpdate := range update.Codes {
		var accountK accountBlockKey
		accountK.set(common.CodeArchiveKey, codeUpdate.Account, block)
		batch.Put(accountK[:], codeUpdate.Code[:])
	}

	for _, nonceUpdate := range update.Nonces {
		var accountK accountBlockKey
		accountK.set(common.NonceArchiveKey, nonceUpdate.Account, block)
		batch.Put(accountK[:], nonceUpdate.Nonce[:])
	}

	for _, slotUpdate := range update.Slots {
		reincarnation, err := getReincarnationNumber(slotUpdate.Account) // use changes from status updates above
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var slotK accountKeyBlockKey
		slotK.set(common.StorageArchiveKey, slotUpdate.Account, reincarnation, slotUpdate.Key, block)
		batch.Put(slotK[:], slotUpdate.Value[:])
	}

	blockHasher := sha256.New()
	lastBlockHash, err := a.GetHash(block)
	if err != nil {
		return fmt.Errorf("failed to get previous block hash; %s", err)
	}
	blockHasher.Write(lastBlockHash[:])

	reusedHasher := sha256.New()
	accountUpdates := archive.AccountUpdatesFrom(&update)
	for account, accountUpdate := range accountUpdates {
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
		batch.Put(accountK[:], newAccountHash)
	}

	blockHash := blockHasher.Sum(nil)
	var blockK blockKey
	blockK.set(block)
	batch.Put(blockK[:], blockHash[:])

	return a.db.Write(&batch, nil)
}

func (a *Archive) GetLastBlockHeight() (block uint64, err error) {
	keyRange := getBlockKeyRangeFromHighest()
	it := a.db.NewIterator(&keyRange, &opt.ReadOptions{})
	defer it.Release()

	if it.Next() {
		var key blockKey
		copy(key[:], it.Key())
		block = key.get()
		return block, nil
	}
	return 0, it.Error()
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
	keyRange := getBlockKeyRangeFrom(block)
	it := a.db.NewIterator(&keyRange, nil)
	defer it.Release()

	if it.Next() {
		copy(hash[:], it.Value())
		return hash, nil
	}
	return common.Hash{}, it.Error()
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
