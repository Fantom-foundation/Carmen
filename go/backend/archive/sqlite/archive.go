package sqlite

import (
	"context"
	"database/sql"
	"fmt"

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
	kCreateBlockTable   = "CREATE TABLE block (number INT PRIMARY KEY, hash BLOB)"
	kAddBlockStmt       = "INSERT INTO block(number, hash) VALUES (?,?)"
	kGetBlockHeightStmt = "SELECT number FROM block ORDER BY number DESC LIMIT 1"

	kCreateStatusTable = "CREATE TABLE status (account BLOB, block INT, exist INT, reincarnation INT, PRIMARY KEY (account,block))"
	kAddStatusStmt     = "INSERT INTO status(account,block,exist,reincarnation) VALUES (?,?,?,?)"
	kGetStatusStmt     = "SELECT exist, reincarnation FROM status WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateBalanceTable = "CREATE TABLE balance (account BLOB, block INT, value BLOB, PRIMARY KEY (account,block))"
	kAddBalanceStmt     = "INSERT INTO balance(account,block,value) VALUES (?,?,?)"
	kGetBalanceStmt     = "SELECT value FROM balance WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateCodeTable = "CREATE TABLE code (account BLOB, block INT, code BLOB, PRIMARY KEY (account,block))"
	kAddCodeStmt     = "INSERT INTO code(account,block,code) VALUES (?,?,?)"
	kGetCodeStmt     = "SELECT code FROM code WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateNonceTable = "CREATE TABLE nonce (account BLOB, block INT, value BLOB, PRIMARY KEY (account,block))"
	kAddNonceStmt     = "INSERT INTO nonce(account,block,value) VALUES (?,?,?)"
	kGetNonceStmt     = "SELECT value FROM nonce WHERE account = ? AND block <= ? ORDER BY block DESC LIMIT 1"

	kCreateValueTable = "CREATE TABLE storage (account BLOB, reincarnation INT, slot BLOB, block INT, value BLOB, PRIMARY KEY (account,reincarnation,slot,block))"
	kAddValueStmt     = "INSERT INTO storage(account,reincarnation,slot,block,value) VALUES (?,?,?,?,?)"
	kGetValueStmt     = "SELECT value FROM storage WHERE account = ? AND reincarnation = ? AND slot = ? AND block <= ? ORDER BY block DESC LIMIT 1"
)

type Archive struct {
	db                 *sql.DB
	addBlockStmt       *sql.Stmt
	getBlockHeightStmt *sql.Stmt
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

	addBlock, err := db.Prepare(kAddBlockStmt)
	if err != nil {
		return nil, err
	}
	getBlockHeight, err := db.Prepare(kGetBlockHeightStmt)
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

	return &Archive{
		db:                 db,
		addBlockStmt:       addBlock,
		getBlockHeightStmt: getBlockHeight,
		addStatusStmt:      addStatus,
		getStatusStmt:      getStatus,
		addBalanceStmt:     addBalance,
		getBalanceStmt:     getBalance,
		addCodeStmt:        addCode,
		getCodeStmt:        getCode,
		addNonceStmt:       addNonce,
		getNonceStmt:       getNonce,
		addValueStmt:       addValue,
		getValueStmt:       getValue,
	}, nil
}

func (a *Archive) Close() error {
	return a.db.Close()
}

func (a *Archive) Add(block uint64, update common.Update) error {
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

	hash := update.GetHash()
	_, err = tx.Stmt(a.addBlockStmt).Exec(block, hash[:])
	if err != nil {
		return fmt.Errorf("failed to add block; %s", err)
	}

	for _, account := range update.DeletedAccounts {
		_, reincarnation, err := a.getStatus(tx, block, account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = tx.Stmt(a.addStatusStmt).Exec(account[:], block, false, reincarnation+1)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
	}

	for _, account := range update.CreatedAccounts {
		_, reincarnation, err := a.getStatus(tx, block, account)
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = tx.Stmt(a.addStatusStmt).Exec(account[:], block, true, reincarnation+1)
		if err != nil {
			return fmt.Errorf("failed to add status; %s", err)
		}
	}

	for _, balanceUpdate := range update.Balances {
		_, err = tx.Stmt(a.addBalanceStmt).Exec(balanceUpdate.Account[:], block, balanceUpdate.Balance[:])
		if err != nil {
			return fmt.Errorf("failed to add balance; %s", err)
		}
	}

	for _, codeUpdate := range update.Codes {
		_, err = tx.Stmt(a.addCodeStmt).Exec(codeUpdate.Account[:], block, codeUpdate.Code)
		if err != nil {
			return fmt.Errorf("failed to add code; %s", err)
		}
	}

	for _, nonceUpdate := range update.Nonces {
		_, err = tx.Stmt(a.addNonceStmt).Exec(nonceUpdate.Account[:], block, nonceUpdate.Nonce[:])
		if err != nil {
			return fmt.Errorf("failed to add nonce; %s", err)
		}
	}

	for _, slotUpdate := range update.Slots {
		_, reincarnation, err := a.getStatus(tx, block, slotUpdate.Account) // use changes from status updates above
		if err != nil {
			return fmt.Errorf("failed to get status; %s", err)
		}
		_, err = tx.Stmt(a.addValueStmt).Exec(slotUpdate.Account[:], reincarnation, slotUpdate.Key[:], block, slotUpdate.Value[:])
		if err != nil {
			return fmt.Errorf("failed to add storage value; %s", err)
		}
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
