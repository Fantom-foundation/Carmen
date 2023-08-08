package s4

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"

	gethcommon "github.com/ethereum/go-ethereum/common"
)

func TestEthereumCompatibleHash_EmptyTrie(t *testing.T) {
	trie := newEthereumStateDB()
	expected := trie.IntermediateRoot(true)

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}
	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_SingleAccount(t *testing.T) {
	trie := newEthereumStateDB()
	trie.SetNonce(gethcommon.Address{1}, 10)
	trie.SetBalance(gethcommon.Address{1}, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{1}, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_SingleAccountWithSingleValue(t *testing.T) {

	gethAddr := gethcommon.Address{1}
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr, 10)
	trie.SetBalance(gethAddr, big.NewInt(12))
	trie.SetState(gethAddr, gethcommon.Hash{1}, gethcommon.Hash{2})
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{1}, balance)
	state.SetStorage(common.Address{1}, common.Key{1}, common.Value{2})
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func _TestEthereumCompatibleHash_TwoAccounts(t *testing.T) {

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{2}, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}
	state.trie.Dump()

	gethAddr1 := gethcommon.Address{1}
	gethAddr2 := gethcommon.Address{2}
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr1, 10)
	trie.SetBalance(gethAddr2, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)
	trie.DumpToConsole()

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

// --- The following is copied from Aida ----
// DO NOT SUBMIT -- DO NOT SUBMIT -- DO NOT SUBMIT -- DO NOT SUBMIT

// offTheChainDB is state.cachingDB clone without disk caches
type offTheChainDB struct {
	db *trie.Database
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *offTheChainDB) OpenTrie(root gethcommon.Hash) (state.Trie, error) {
	tr, err := trie.NewSecure(root, db.db)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// OpenStorageTrie opens the storage trie of an account.
func (db *offTheChainDB) OpenStorageTrie(addrHash, root gethcommon.Hash) (state.Trie, error) {
	tr, err := trie.NewSecure(root, db.db)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// CopyTrie returns an independent copy of the given trie.
func (db *offTheChainDB) CopyTrie(t state.Trie) state.Trie {
	switch t := t.(type) {
	case *trie.SecureTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ContractCode retrieves a particular contract's code.
func (db *offTheChainDB) ContractCode(addrHash, codeHash gethcommon.Hash) ([]byte, error) {
	code := rawdb.ReadCode(db.db.DiskDB(), codeHash)
	if len(code) > 0 {
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeWithPrefix retrieves a particular contract's code. If the
// code can't be found in the cache, then check the existence with **new**
// db scheme.
func (db *offTheChainDB) ContractCodeWithPrefix(addrHash, codeHash gethcommon.Hash) ([]byte, error) {
	code := rawdb.ReadCodeWithPrefix(db.db.DiskDB(), codeHash)
	if len(code) > 0 {
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *offTheChainDB) ContractCodeSize(addrHash, codeHash gethcommon.Hash) (int, error) {
	code, err := db.ContractCode(addrHash, codeHash)
	return len(code), err
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *offTheChainDB) TrieDB() *trie.Database {
	return db.db
}

func newEthereumStateDB() *state.StateDB {
	// backend in-memory key-value database
	kvdb := rawdb.NewMemoryDatabase()

	// zeroed trie.Config to disable Cache, Journal, Preimages, ...
	zerodConfig := &trie.Config{}
	tdb := trie.NewDatabaseWithConfig(kvdb, zerodConfig)

	sdb := &offTheChainDB{
		db: tdb,
	}

	statedb, err := state.New(gethcommon.Hash{}, sdb, nil)
	if err != nil {
		panic(fmt.Errorf("error calling state.New() in NewOffTheChainDB(): %v", err))
	}
	return statedb
}

// DO NOT SUBMIT -- DO NOT SUBMIT -- DO NOT SUBMIT -- DO NOT SUBMIT
