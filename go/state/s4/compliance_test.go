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

func TestEthereumCompatibleHash_TwoAccounts(t *testing.T) {

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

	gethAddr1 := gethcommon.Address{1}
	gethAddr2 := gethcommon.Address{2}
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr1, 10)
	trie.SetBalance(gethAddr2, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_TwoAccountsWithValues(t *testing.T) {

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.trie.SetValue(common.Address{1}, common.Key{1}, common.Value{0, 0, 1})
	state.trie.SetValue(common.Address{1}, common.Key{2}, common.Value{2})

	state.SetBalance(common.Address{2}, balance)
	state.trie.SetValue(common.Address{2}, common.Key{1}, common.Value{0, 0, 1})
	state.trie.SetValue(common.Address{2}, common.Key{2}, common.Value{2})
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	gethAddr1 := gethcommon.Address{1}
	gethAddr2 := gethcommon.Address{2}
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr1, 10)
	trie.SetBalance(gethAddr2, big.NewInt(12))
	trie.SetState(gethAddr1, gethcommon.Hash{1}, gethcommon.Hash{0, 0, 1})
	trie.SetState(gethAddr1, gethcommon.Hash{2}, gethcommon.Hash{2})
	trie.SetState(gethAddr2, gethcommon.Hash{1}, gethcommon.Hash{0, 0, 1})
	trie.SetState(gethAddr2, gethcommon.Hash{2}, gethcommon.Hash{2})
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_TwoAccountsWithExtensionNodeWithEvenLength(t *testing.T) {
	// These two addresses have the same first byte when hashed:
	addr1 := common.Address{0x04}
	addr2 := common.Address{0x2F}
	if a, b := keccak256(addr1[:])[0], keccak256(addr2[:])[0]; a != b {
		t.Fatalf("invalid setup, addresses do not have common prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(addr1, common.ToNonce(10))
	state.SetBalance(addr2, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	gethAddr1 := gethcommon.Address(addr1)
	gethAddr2 := gethcommon.Address(addr2)
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr1, 10)
	trie.SetBalance(gethAddr2, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_TwoAccountsWithExtensionNodeWithOddLength(t *testing.T) {
	// These two addresses have the same first nibble when hashed:
	addr1 := common.Address{0x02}
	addr2 := common.Address{0x07}
	if a, b := keccak256(addr1[:])[0], keccak256(addr2[:])[0]; (a>>4) != (b>>4) && a == b {
		t.Fatalf("invalid setup, addresses do not have single prefix bit prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(addr1, common.ToNonce(10))
	state.SetBalance(addr2, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	gethAddr1 := gethcommon.Address(addr1)
	gethAddr2 := gethcommon.Address(addr2)
	trie := newEthereumStateDB()
	trie.SetNonce(gethAddr1, 10)
	trie.SetBalance(gethAddr2, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestEthereumCompatibleHash_InsertLotsOfData(t *testing.T) {
	const N = 100

	reference := newEthereumStateDB()

	trie, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	address := getTestAddresses(N)
	keys := getTestKeys(N)

	// Fill the tree.
	for i, addr := range address {
		refAddr := gethcommon.Address(addr)
		trgAddr := common.Address(addr)

		reference.SetNonce(refAddr, uint64(i)+1)
		if err := trie.SetNonce(trgAddr, common.ToNonce(uint64(i)+1)); err != nil {
			t.Fatalf("failed to insert account: %v", err)
		}

		for i, key := range keys {
			reference.SetState(refAddr, gethcommon.Hash(key), gethcommon.Hash{byte(i)})
			if err := trie.SetStorage(trgAddr, key, common.Value{byte(i)}); err != nil {
				t.Fatalf("failed to insert value: %v", err)
			}

			want := common.Hash(reference.IntermediateRoot(true))
			got, err := trie.GetHash()
			if err != nil {
				t.Fatalf("failed to compute hash: %v", err)
			}

			if got != want {
				//fmt.Printf("Have:\n")
				//trie.trie.Dump()
				//fmt.Printf("Should:\n")
				//reference.DumpToConsole()

				t.Fatalf("invalid hash, expected %x, got %x", got, want)
			}
		}
	}

	// Delete all accounts.
	for _, addr := range address {
		refAddr := gethcommon.Address(addr)
		trgAddr := common.Address(addr)

		reference.SetNonce(refAddr, 0)
		if err := trie.DeleteAccount(trgAddr); err != nil {
			t.Fatalf("failed to delete account: %v", err)
		}

		want := common.Hash(reference.IntermediateRoot(true))
		got, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		if got != want {
			//fmt.Printf("Have:\n")
			//trie.trie.Dump()
			//fmt.Printf("Should:\n")
			//reference.DumpToConsole()

			t.Fatalf("invalid hash, expected %x, got %x", got, want)
		}
	}
}

func TestEthereumCompatibleHash_InsertLotsOfValues(t *testing.T) {
	const N = 1000

	reference := newEthereumStateDB()

	trie, err := OpenGoMemoryState(t.TempDir(), S5Config)
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	refAddr := gethcommon.Address{}
	trgAddr := common.Address{}
	keys := getTestKeys(N)

	// Create a single account.
	reference.SetNonce(refAddr, 1)
	if err := trie.SetNonce(trgAddr, common.ToNonce(1)); err != nil {
		t.Fatalf("failed to insert account: %v", err)
	}

	// Insert keys one-by-one.
	for i, key := range keys {
		reference.SetState(refAddr, gethcommon.Hash(key), gethcommon.Hash{byte(i)})
		if err := trie.SetStorage(trgAddr, key, common.Value{byte(i)}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}

		want := common.Hash(reference.IntermediateRoot(true))
		got, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		if got != want {
			t.Fatalf("invalid hash, expected %v, got %v", got, want)
		}
	}

	// Delete all values.
	for _, key := range keys {
		reference.SetState(refAddr, gethcommon.Hash(key), gethcommon.Hash{})
		if err := trie.SetStorage(trgAddr, key, common.Value{}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}

		want := common.Hash(reference.IntermediateRoot(true))
		got, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		if got != want {
			t.Fatalf("invalid hash, expected %v, got %v", got, want)
		}
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
