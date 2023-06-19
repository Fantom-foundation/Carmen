package s4

import (
	"math/rand"
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

func TestStateTrie_ValuesCanBeSetAndRetrieved(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{1}
	key := common.Key{1}
	value := common.Value{1}

	// If the account does not exist, the write has no effect.
	if err := trie.SetValue(addr, key, value); err != nil {
		t.Errorf("writing to non-existing account failed: %v", err)
	}
	if got, err := trie.GetValue(addr, key); got != (common.Value{}) || err != nil {
		t.Errorf("wanted %v, got %v", common.Value{}, got)
	}

	// Create the account.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to create account for test: %v", err)
	}
	if err := trie.SetValue(addr, key, value); err != nil {
		t.Errorf("writing to existing account failed: %v", err)
	}
	if got, err := trie.GetValue(addr, key); value != got || err != nil {
		t.Errorf("wanted %v, got %v", value, got)
	}
}

func TestStateTrie_InsertLotsOfData(t *testing.T) {
	t.Parallel()
	const N = 100

	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	address := getTestAddresses(N)
	keys := getTestKeys(N)

	// Fill the tree.
	for i, addr := range address {
		if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.ToNonce(uint64(i) + 1)}); err != nil {
			t.Fatalf("failed to insert account: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.dump()
			t.Fatalf("trie inconsistent after account insert:\n%v", err)
		}

		for i, key := range keys {
			if err := trie.SetValue(addr, key, common.Value{byte(i)}); err != nil {
				t.Fatalf("failed to insert value: %v", err)
			}
			if err := trie.Check(); err != nil {
				trie.dump()
				t.Fatalf("trie inconsistent after value insert:\n%v", err)
			}
		}
	}

	// Check its content.
	for i, addr := range address {
		if info, err := trie.GetAccountInfo(addr); int(info.Nonce.ToUint64()) != i+1 || err != nil {
			t.Fatalf("wrong value, wanted %v, got %v, err %v", i+1, int(info.Nonce.ToUint64()), err)
		}
		for i, key := range keys {
			if value, err := trie.GetValue(addr, key); value[0] != byte(i) || err != nil {
				t.Fatalf("wrong value, wanted %v, got %v, err %v", byte(i), value[0], err)
			}
		}
	}

	// Delete all accounts.
	for _, addr := range address {
		if err := trie.SetAccountInfo(addr, AccountInfo{}); err != nil {
			t.Fatalf("failed to delete account: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.dump()
			t.Fatalf("trie inconsistent after account deletion:\n%v\nDeleted account: %v", err, addr)
		}
	}
}

func TestStateTrie_InsertLotsOfValues(t *testing.T) {
	t.Parallel()
	const N = 10000

	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{}
	keys := getTestKeys(N)

	// Fill a single account.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.ToNonce(1)}); err != nil {
		t.Fatalf("failed to insert account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.dump()
		t.Fatalf("trie inconsistent after account insert:\n%v", err)
	}

	for i, key := range keys {
		if err := trie.SetValue(addr, key, common.Value{byte(i)}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.dump()
			t.Fatalf("trie inconsistent after value insert:\n%v", err)
		}
	}

	// Check its content.
	for i, key := range keys {
		if value, err := trie.GetValue(addr, key); value[0] != byte(i) || err != nil {
			t.Fatalf("wrong value, wanted %v, got %v, err %v", byte(i), value[0], err)
		}
	}

	// Delete all values.
	for _, key := range keys {
		if err := trie.SetValue(addr, key, common.Value{}); err != nil {
			t.Fatalf("failed to delete value: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.dump()
			t.Fatalf("trie inconsistent after value deletion:\n%v\nDeleted value: %v", err, key)
		}
	}
}

func getTestAddresses(number int) []common.Address {
	res := make([]common.Address, number)
	for i := range res {
		j := i * i
		res[i][0] = byte(j)
		res[i][1] = byte(j >> 8)
		res[i][2] = byte(j >> 16)
		res[i][3] = byte(j >> 24)
	}
	rand.Seed(0)
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}

func getTestKeys(number int) []common.Key {
	res := make([]common.Key, number)
	for i := range res {
		j := i * i
		res[i][0] = byte(j)
		res[i][1] = byte(j >> 8)
		res[i][2] = byte(j >> 16)
		res[i][3] = byte(j >> 24)
	}
	rand.Seed(0)
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}
