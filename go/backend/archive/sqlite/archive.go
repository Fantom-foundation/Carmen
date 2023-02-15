package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
	_ "github.com/mattn/go-sqlite3"
)

var (
	// See https://www.sqlite.org/pragma.html
	kConfigureConnection = []string{
		"PRAGMA journal_mode = OFF",
		"PRAGMA synchronous = OFF",
		"PRAGMA cache_size = -1048576", // abs(N*1024) = 1GB
		"PRAGMA locking_mode = EXCLUSIVE",
	}
)

const (
	kCreateBlockTable   = "CREATE TABLE IF NOT EXISTS block (number INT PRIMARY KEY, hash BLOB)"
	kAddBlockStmt       = "INSERT INTO block(number, hash) VALUES (?,?)"
	kGetBlockHeightStmt = "SELECT number FROM block ORDER BY number DESC LIMIT 1"
	kGetBlockHashStmt   = "SELECT hash FROM block WHERE number <= ? ORDER BY number DESC LIMIT 1"

	kCreateStatusTable = "CREATE TABLE IF NOT EXISTS status (account BLOB, block INT, exist INT, reincarnation INT, PRIMARY KEY (account,block))"
	kAddStatusStmt     = "INSERT INTO status(account,block,exist,reincarnation) VALUES (?,?,?,?)"
	kGetStatusStmt     = "SELECT exist, reincarnation FROM status WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateBalanceTable = "CREATE TABLE IF NOT EXISTS balance (account BLOB, block INT, value BLOB, PRIMARY KEY (account,block))"
	kAddBalanceStmt     = "INSERT INTO balance(account,block,value) VALUES (?,?,?)"
	kGetBalanceStmt     = "SELECT value FROM balance WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateCodeTable = "CREATE TABLE IF NOT EXISTS code (account BLOB, block INT, code BLOB, PRIMARY KEY (account,block))"
	kAddCodeStmt     = "INSERT INTO code(account,block,code) VALUES (?,?,?)"
	kGetCodeStmt     = "SELECT code FROM code WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateNonceTable = "CREATE TABLE IF NOT EXISTS nonce (account BLOB, block INT, value BLOB, PRIMARY KEY (account,block))"
	kAddNonceStmt     = "INSERT INTO nonce(account,block,value) VALUES (?,?,?)"
	kGetNonceStmt     = "SELECT value FROM nonce WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateValueTable = "CREATE TABLE IF NOT EXISTS storage (account BLOB, reincarnation INT, slot BLOB, block INT, value BLOB, PRIMARY KEY (account,reincarnation,slot,block))"
	kAddValueStmt     = "INSERT INTO storage(account,reincarnation,slot,block,value) VALUES (?,?,?,?,?)"
	kGetValueStmt     = "SELECT value FROM storage WHERE account = ? AND reincarnation = ? AND slot = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateAccountHashTable = "CREATE TABLE IF NOT EXISTS account_hash (account BLOB, block INT, hash BLOB, PRIMARY KEY(account,block))"
	kAddAccountHashStmt     = "INSERT INTO account_hash(account, block, hash) VALUES (?,?,?)"
	kGetAccountHashStmt     = "SELECT hash FROM account_hash WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"
)

type Archive struct {
	db                 *sql.DB
	addBlockStmt       *sql.Stmt
	getBlockHeightStmt *sql.Stmt
	getBlockHashStmt   *sql.Stmt
	addStatusStmt      *sql.Stmt
	getStatusStmt      *sql.Stmt
	addBalanceStmt     *sql.Stmt
	getBalanceStmt     *sql.Stmt
	addCodeStmt        *sql.Stmt
	getCodeStmt        *sql.Stmt
	addNonceStmt       *sql.Stmt
	getNonceStmt       *sql.Stmt
	addValueStmt       *sql.Stmt
	getValueStmt       *sql.Stmt
	addAccountHashStmt *sql.Stmt
	getAccountHashStmt *sql.Stmt

	reincarnationNumberCache map[common.Address]int
}

func NewArchive(file string) (*Archive, error) {
	db, err := sql.Open("sqlite3", "file:"+file)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite; %s", err)
	}
	for _, cmd := range kConfigureConnection {
		_, err = db.Exec(cmd)
		if err != nil {
			return nil, fmt.Errorf("failed to configure connection with %s; %s", cmd, err)
		}
	}
	_, err = db.Exec(kCreateBlockTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create block table; %s", err)
	}
	_, err = db.Exec(kCreateStatusTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create status table; %s", err)
	}
	_, err = db.Exec(kCreateBalanceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create balance table; %s", err)
	}
	_, err = db.Exec(kCreateCodeTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create code table; %s", err)
	}
	_, err = db.Exec(kCreateNonceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create nonce table; %s", err)
	}
	_, err = db.Exec(kCreateValueTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create value table; %s", err)
	}
	_, err = db.Exec(kCreateAccountHashTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create account hash table; %s", err)
	}

	addBlock, err := db.Prepare(kAddBlockStmt)
	if err != nil {
		return nil, err
	}
	getBlockHeight, err := db.Prepare(kGetBlockHeightStmt)
	if err != nil {
		return nil, err
	}
	getBlockHash, err := db.Prepare(kGetBlockHashStmt)
	if err != nil {
		return nil, err
	}
	addStatus, err := db.Prepare(kAddStatusStmt)
	if err != nil {
		return nil, err
	}
	getStatus, err := db.Prepare(kGetStatusStmt)
	if err != nil {
		return nil, err
	}
	addBalance, err := db.Prepare(kAddBalanceStmt)
	if err != nil {
		return nil, err
	}
	getBalance, err := db.Prepare(kGetBalanceStmt)
	if err != nil {
		return nil, err
	}
	addCode, err := db.Prepare(kAddCodeStmt)
	if err != nil {
		return nil, err
	}
	getCode, err := db.Prepare(kGetCodeStmt)
	if err != nil {
		return nil, err
	}
	addNonce, err := db.Prepare(kAddNonceStmt)
	if err != nil {
		return nil, err
	}
	getNonce, err := db.Prepare(kGetNonceStmt)
	if err != nil {
		return nil, err
	}
	addValue, err := db.Prepare(kAddValueStmt)
	if err != nil {
		return nil, err
	}
	getValue, err := db.Prepare(kGetValueStmt)
	if err != nil {
		return nil, err
	}
	addAccountHash, err := db.Prepare(kAddAccountHashStmt)
	if err != nil {
		return nil, err
	}
	getAccountHash, err := db.Prepare(kGetAccountHashStmt)
	if err != nil {
		return nil, err
	}

	return &Archive{
		db:                       db,
		addBlockStmt:             addBlock,
		getBlockHeightStmt:       getBlockHeight,
		getBlockHashStmt:         getBlockHash,
		addStatusStmt:            addStatus,
		getStatusStmt:            getStatus,
		addBalanceStmt:           addBalance,
		getBalanceStmt:           getBalance,
		addCodeStmt:              addCode,
		getCodeStmt:              getCode,
		addNonceStmt:             addNonce,
		getNonceStmt:             getNonce,
		addValueStmt:             addValue,
		getValueStmt:             getValue,
		addAccountHashStmt:       addAccountHash,
		getAccountHashStmt:       getAccountHash,
		reincarnationNumberCache: map[common.Address]int{},
	}, nil
}

func (a *Archive) Close() error {
	return a.db.Close()
}

func (a *Archive) Add(block uint64, update common.Update) error {
	// Empty updates can be skipped. Blocks are implicitly empty,
	// and being tolerante here makes client code easier.
	if update.IsEmpty() {
		return nil
	}
	tx, err := a.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	var succeed bool
	defer func() {
		if !succeed {
			if err := tx.Rollback(); err != nil {
				panic(fmt.Errorf("failed to rollback; %s", err))
			}
		}
	}()

	// helper function for obtaining current reincarnation number of an account
	getReincarnationNumber := func(account common.Address) (int, error) {
		if res, exists := a.reincarnationNumberCache[account]; exists {
			return res, nil
		}
		_, res, err := a.getStatus(tx, block, account)
		if err != nil {
			return 0, err
		}
		a.reincarnationNumberCache[account] = res
		return res, nil
	}

	stmt := tx.Stmt(a.addStatusStmt)
	for _, account := range update.DeletedAccounts {
		reincarnation, err := getReincarnationNumber(account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = stmt.Exec(account[:], block, false, reincarnation+1)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	for _, account := range update.CreatedAccounts {
		reincarnation, err := getReincarnationNumber(account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = stmt.Exec(account[:], block, true, reincarnation+1)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
		a.reincarnationNumberCache[account] = reincarnation + 1
	}

	stmt = tx.Stmt(a.addBalanceStmt)
	for _, balanceUpdate := range update.Balances {
		_, err = stmt.Exec(balanceUpdate.Account[:], block, balanceUpdate.Balance[:])
		if err != nil {
			return fmt.Errorf("failed to add balance; %s", err)
		}
	}

	stmt = tx.Stmt(a.addCodeStmt)
	for _, codeUpdate := range update.Codes {
		_, err = stmt.Exec(codeUpdate.Account[:], block, codeUpdate.Code)
		if err != nil {
			return fmt.Errorf("failed to add code; %s", err)
		}
	}

	stmt = tx.Stmt(a.addNonceStmt)
	for _, nonceUpdate := range update.Nonces {
		_, err = stmt.Exec(nonceUpdate.Account[:], block, nonceUpdate.Nonce[:])
		if err != nil {
			return fmt.Errorf("failed to add nonce; %s", err)
		}
	}

	stmt = tx.Stmt(a.addValueStmt)
	for _, slotUpdate := range update.Slots {
		reincarnation, err := getReincarnationNumber(slotUpdate.Account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = stmt.Exec(slotUpdate.Account[:], reincarnation, slotUpdate.Key[:], block, slotUpdate.Value[:])
		if err != nil {
			return fmt.Errorf("failed to add storage value; %s", err)
		}
	}

	blockHasher := sha256.New()
	lastBlockHash, err := a.getBlockHash(tx, block) // needs to be in tx, otherwise database is locked
	if err != nil {
		return fmt.Errorf("failed to get previous block hash; %s", err)
	}
	blockHasher.Write(lastBlockHash[:])

	// calculate changed accounts hashes
	reusedHasher := sha256.New()
	stmt = tx.Stmt(a.addAccountHashStmt)
	accountUpdates := archive.AccountUpdatesFrom(&update)
	for account, accountUpdate := range accountUpdates {
		lastAccountHash, err := a.getAccountHash(tx, block, account) // needs to be in tx, otherwise database is locked
		if err != nil {
			return fmt.Errorf("failed to get previous account hash; %s", err)
		}
		accountUpdateHash := accountUpdate.GetHash(reusedHasher)

		reusedHasher.Reset()
		reusedHasher.Write(lastAccountHash[:])
		reusedHasher.Write(accountUpdateHash[:])
		newAccountHash := reusedHasher.Sum(nil)
		blockHasher.Write(newAccountHash)

		_, err = stmt.Exec(account[:], block, newAccountHash[:])
		if err != nil {
			return fmt.Errorf("failed to add account hash; %s", err)
		}
	}

	blockHash := blockHasher.Sum(nil)
	_, err = tx.Stmt(a.addBlockStmt).Exec(block, blockHash)
	if err != nil {
		return fmt.Errorf("failed to add block %d; %s", block, err)
	}

	succeed = true
	return tx.Commit()
}

func (a *Archive) getStatus(tx *sql.Tx, block uint64, account common.Address) (exists bool, reincarnation int, err error) {
	stmt := a.getStatusStmt
	if tx != nil {
		stmt = tx.Stmt(stmt)
	}
	rows, err := stmt.Query(account[:], block)
	if err != nil {
		return false, 0, err
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&exists, &reincarnation)
		return exists, reincarnation, err
	}
	return false, 0, rows.Err()
}

func (a *Archive) GetLastBlockHeight() (block uint64, err error) {
	rows, err := a.getBlockHeightStmt.Query()
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&block)
		return block, err
	}
	return 0, rows.Err()
}

func (a *Archive) Exists(block uint64, account common.Address) (exists bool, err error) {
	exists, _, err = a.getStatus(nil, block, account)
	return exists, err
}

func (a *Archive) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	rows, err := a.getBalanceStmt.Query(account[:], block)
	if err != nil {
		return common.Balance{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var bytes sql.RawBytes
		err = rows.Scan(&bytes)
		copy(balance[:], bytes)
		return balance, err
	}
	return common.Balance{}, rows.Err()
}

func (a *Archive) GetCode(block uint64, account common.Address) (code []byte, err error) {
	rows, err := a.getCodeStmt.Query(account[:], block)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&code)
		return code, err
	}
	return nil, rows.Err()
}

func (a *Archive) GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error) {
	rows, err := a.getNonceStmt.Query(account[:], block)
	if err != nil {
		return common.Nonce{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var bytes sql.RawBytes
		err = rows.Scan(&bytes)
		copy(nonce[:], bytes)
		return nonce, err
	}
	return common.Nonce{}, rows.Err()
}

func (a *Archive) GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error) {
	accountExists, reincarnation, err := a.getStatus(nil, block, account)
	if !accountExists || err != nil {
		return common.Value{}, err
	}

	rows, err := a.getValueStmt.Query(account[:], reincarnation, slot[:], block)
	if err != nil {
		return common.Value{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var bytes sql.RawBytes
		err = rows.Scan(&bytes)
		copy(value[:], bytes)
		return value, err
	}
	return common.Value{}, rows.Err()
}

func (a *Archive) getBlockHash(tx *sql.Tx, block uint64) (hash common.Hash, err error) {
	stmt := a.getBlockHashStmt
	if tx != nil {
		stmt = tx.Stmt(stmt)
	}
	rows, err := stmt.Query(block)
	if err != nil {
		return common.Hash{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var bytes sql.RawBytes
		err = rows.Scan(&bytes)
		copy(hash[:], bytes)
		return hash, err
	}
	return common.Hash{}, rows.Err()
}

func (a *Archive) GetHash(block uint64) (hash common.Hash, err error) {
	return a.getBlockHash(nil, block)
}

func (a *Archive) getAccountHash(tx *sql.Tx, block uint64, account common.Address) (hash common.Hash, err error) {
	stmt := a.getAccountHashStmt
	if tx != nil {
		stmt = tx.Stmt(stmt)
	}
	rows, err := stmt.Query(account[:], block)
	if err != nil {
		return common.Hash{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var bytes sql.RawBytes
		err = rows.Scan(&bytes)
		copy(hash[:], bytes)
		return hash, err
	}
	return common.Hash{}, rows.Err()
}

func (a *Archive) GetAccountHash(block uint64, account common.Address) (hash common.Hash, err error) {
	return a.getAccountHash(nil, block, account)
}

// GetMemoryFootprint provides the size of the archive in memory in bytes
func (a *Archive) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	var address common.Address
	var reincarnation int
	mf.AddChild("reincarnationNumberCache", common.NewMemoryFootprint(uintptr(len(a.reincarnationNumberCache))*(unsafe.Sizeof(address)+unsafe.Sizeof(reincarnation))))
	return mf
}
