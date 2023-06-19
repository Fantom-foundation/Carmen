package s4

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestStateTrie_EmptyTrieIsConsistent(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}
}

func TestStateTrie_NonExistingAccountsHaveEmptyInfo(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr1 := common.Address{1}
	if info, err := trie.GetAccountInfo(addr1); err != nil || info != (AccountInfo{}) {
		t.Errorf("failed to get default account infor from empty state, got %v, err: %v", info, err)
	}
}

func TestStateTrie_SetAndGetSingleAccountInformationWorks(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr := common.Address{1}
	info := AccountInfo{
		Nonce:    common.Nonce{1},
		Balance:  common.Balance{2},
		CodeHash: common.Hash{3},
	}

	if err := trie.SetAccountInfo(addr, info); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}

	if err := trie.Check(); err != nil {
		trie.dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if recovered, err := trie.GetAccountInfo(addr); err != nil || recovered != info {
		t.Errorf("failed to recover account information, wanted %v, got %v, err %v", info, recovered, err)
	}

	if err := trie.Check(); err != nil {
		trie.dump()
		t.Errorf("trie corrupted after read: %v", err)
	}
}

func TestStateTrie_SetAndGetMultipleAccountInformationWorks(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	addr3 := common.Address{0, 0, 0, 0, 0, 0, 3}

	if err := trie.SetAccountInfo(addr1, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if err := trie.SetAccountInfo(addr2, AccountInfo{Nonce: common.Nonce{2}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if err := trie.SetAccountInfo(addr3, AccountInfo{Nonce: common.Nonce{3}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}
}

func TestStateTrie_NonExistingValueHasZeroValue(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{1}
	key := common.Key{1}

	// If the account does not exist, the result should be empty.
	if value, err := trie.GetValue(addr, key); value != (common.Value{}) || err != nil {
		t.Errorf("expected value of non-existing account to be empty, got %v, err: %v", value, err)
	}

	// Also, if the account exists, the result should be empty.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to create an account")
	}
	if value, err := trie.GetValue(addr, key); value != (common.Value{}) || err != nil {
		t.Errorf("expected value of uninitialized slot to be empty, got %v, err: %v", value, err)
	}
}
