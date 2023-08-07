package s4

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4/rlp"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
	gomock "github.com/golang/mock/gomock"

	gethcommon "github.com/ethereum/go-ethereum/common"
)

var emptyNodeHash = keccak256(rlp.Encode(rlp.String{}))

func TestMptHasher_EmptyNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(EmptyNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	if got, want := hash, emptyNodeHash; got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}

func TestMptHasher_EmptyTrie(t *testing.T) {
	trie := newEthereumStateDB()
	expected := trie.IntermediateRoot(true)

	hasher := MptHasher{}
	hash, err := hasher.GetHash(EmptyNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestMptHasher_SingleAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	nodeSource := NewMockNodeSource(ctrl)
	hashSource := NewMockHashSource(ctrl)

	hashSource.EXPECT().GetHashFor(EmptyId()).Return(emptyNodeHash, nil)

	nonce := common.ToNonce(10)
	balance, _ := common.ToBalance(big.NewInt(12))
	node := &AccountNode{
		address: common.Address{1},
		info: AccountInfo{
			Nonce:   nonce,
			Balance: balance,
		},
	}

	hasher := MptHasher{}
	hash, err := hasher.GetHash(node, nodeSource, hashSource)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	trie := newEthereumStateDB()
	trie.SetNonce(gethcommon.Address{1}, 10)
	trie.SetBalance(gethcommon.Address{1}, big.NewInt(12))
	trie.Commit(true)
	expected := trie.IntermediateRoot(true)

	if got := gethcommon.Hash(hash); got != expected {
		t.Errorf("invalid hash\nexpected %v\n     got %v", expected, got)
	}
}

func TestMptHasher_ValueNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(&ValueNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	empty := rlp.Encode(
		rlp.List{[]rlp.Item{
			rlp.String{}, // TODO: encode path here
			rlp.String{make([]byte, common.ValueSize)},
		}})
	if got, want := hash, keccak256(empty); got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
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
