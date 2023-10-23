package mpt

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestNodeStatistics_CollectTrieStatisticsWorks(t *testing.T) {
	trie, err := OpenInMemoryLiveTrie(t.TempDir(), S4LiveConfig)
	if err != nil {
		t.Fatalf("failed to create empty trie: %v", err)
	}
	defer trie.Close()

	stats, err := GetTrieNodeStatistics(trie)
	if err != nil {
		t.Fatalf("Failed to collect stats from empty trie: %v", err)
	}
	if stats.numAccounts != 0 || stats.numBranches != 0 || stats.numExtensions != 0 || stats.numValues != 0 {
		t.Errorf("invalid stats for empty trie: %v", stats)
	}

	trie.SetAccountInfo(common.Address{}, AccountInfo{Nonce: common.ToNonce(12)})
	trie.SetValue(common.Address{}, common.Key{1}, common.Value{1})
	trie.SetValue(common.Address{}, common.Key{2}, common.Value{2})

	stats, err = GetTrieNodeStatistics(trie)
	if err != nil {
		t.Fatalf("Failed to collect stats from trie: %v", err)
	}
	if stats.numAccounts != 1 || stats.numBranches != 1 || stats.numExtensions != 1 || stats.numValues != 2 {
		t.Errorf("invalid stats for trie: %v", &stats)
	}
}

func TestNodeStatistics_CollectForestStatisticsWorks(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig)
	if err != nil {
		t.Fatalf("failed to create empty trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close empty archive: %v", err)
	}

	stats, err := GetForestNodeStatistics(dir, S5ArchiveConfig)
	if err != nil {
		t.Fatalf("Failed to collect stats from empty archive: %v", err)
	}
	if stats.numAccounts != 0 || stats.numBranches != 0 || stats.numExtensions != 0 || stats.numValues != 0 {
		t.Errorf("invalid stats for empty archive: %v", stats)
	}

	archive, err = OpenArchiveTrie(dir, S5ArchiveConfig)
	if err != nil {
		t.Fatalf("failed to re-open empty archive: %v", err)
	}

	archive.Add(2, common.Update{
		CreatedAccounts: []common.Address{{1}},
		Nonces: []common.NonceUpdate{
			{Account: common.Address{1}, Nonce: common.ToNonce(12)},
		},
		Slots: []common.SlotUpdate{
			{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{1}},
			{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
		},
	}, nil)

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	stats, err = GetForestNodeStatistics(dir, S5ArchiveConfig)
	if err != nil {
		t.Fatalf("Failed to collect stats from archive: %v", err)
	}
	if stats.numAccounts != 1 || stats.numBranches != 1 || stats.numExtensions != 0 || stats.numValues != 2 {
		t.Errorf("invalid stats for archive: %v", &stats)
	}
}
