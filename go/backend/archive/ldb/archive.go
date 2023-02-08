package ldb

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Archive struct {
	db *common.LevelDbMemoryFootprintWrapper
}

func NewArchive(db *common.LevelDbMemoryFootprintWrapper) (*Archive, error) {
	return &Archive{
		db: db,
	}, nil
}

func (a *Archive) Close() error {
	// no-op
	return nil
}

func (a *Archive) Add(block uint64, update common.Update) error {
	tx, err := a.db.OpenTransaction()
	if err != nil {
		return err
	}
	var succeed bool
	defer func() {
		if !succeed {
			tx.Discard()
		}
	}()

	hash := update.GetHash()
	var blockK blockKey
	blockK.set(block)
	err = tx.Put(blockK[:], hash[:], nil)
	if err != nil {
		return fmt.Errorf("failed to add block; %s", err)
	}

	for _, account := range update.DeletedAccounts {
		_, reincarnation, err := a.getStatus(tx, block, account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var accountK accountBlockKey
		accountK.set(common.AccountArchiveKey, account, block)
		var accountStatusV accountStatusValue
		accountStatusV.set(false, reincarnation+1)
		err = tx.Put(accountK[:], accountStatusV[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
	}

	for _, account := range update.CreatedAccounts {
		_, reincarnation, err := a.getStatus(tx, block, account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var accountK accountBlockKey
		accountK.set(common.AccountArchiveKey, account, block)
		var accountStatusV accountStatusValue
		accountStatusV.set(true, reincarnation+1)
		err = tx.Put(accountK[:], accountStatusV[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
	}

	for _, balanceUpdate := range update.Balances {
		var accountK accountBlockKey
		accountK.set(common.BalanceArchiveKey, balanceUpdate.Account, block)
		err = tx.Put(accountK[:], balanceUpdate.Balance[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add balance; %s", err)
		}
	}

	for _, codeUpdate := range update.Codes {
		var accountK accountBlockKey
		accountK.set(common.CodeArchiveKey, codeUpdate.Account, block)
		err = tx.Put(accountK[:], codeUpdate.Code[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add code; %s", err)
		}
	}

	for _, nonceUpdate := range update.Nonces {
		var accountK accountBlockKey
		accountK.set(common.NonceArchiveKey, nonceUpdate.Account, block)
		err = tx.Put(accountK[:], nonceUpdate.Nonce[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add nonce; %s", err)
		}
	}

	for _, slotUpdate := range update.Slots {
		_, reincarnation, err := a.getStatus(tx, block, slotUpdate.Account) // use changes from status updates above
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		var slotK accountKeyBlockKey
		slotK.set(common.StorageArchiveKey, slotUpdate.Account, reincarnation, slotUpdate.Key, block)
		err = tx.Put(slotK[:], slotUpdate.Value[:], nil)
		if err != nil {
			return fmt.Errorf("failed to add storage value; %s", err)
		}
	}

	succeed = true
	return tx.Commit()
}

func (a *Archive) getStatus(tx common.LevelDB, block uint64, account common.Address) (exists bool, reincarnation int, err error) {
	var key accountBlockKey
	key.set(common.AccountArchiveKey, account, block)
	keyRange := key.getRange()
	it := tx.NewIterator(&keyRange, &opt.ReadOptions{})
	defer it.Release()

	if it.Next() {
		var accountStatusV accountStatusValue
		copy(accountStatusV[:], it.Value())
		exists, reincarnation = accountStatusV.get()
		return exists, reincarnation, nil
	}
	return false, 0, it.Error()
}

func (a *Archive) GetLastBlockHeight() (block uint64, err error) {
	keyRange := getLastBlockRange()
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

func (a *Archive) Exists(block uint64, account common.Address) (exists bool, err error) {
	exists, _, err = a.getStatus(a.db, block, account)
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
	accountExists, reincarnation, err := a.getStatus(a.db, block, account)
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
