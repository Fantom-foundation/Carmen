package s4

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// Note: most properties of the ArchiveTrie are tested through the common
// test infrastructure in /backend/archive.
//
// TODO: generalize common archive tests in /backend/archive such that they
// can be executed as part of this package's test suite

func TestArchiveTrie_OpenAndClose(t *testing.T) {
	archive, err := OpenArchiveTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open empty archive: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close empty archive: %v", err)
	}
}

func TestArchiveTrie_CanHandleMultipleBlocks(t *testing.T) {
	archive, err := OpenArchiveTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open empty archive: %v", err)
	}
	defer archive.Close()

	addr1 := common.Address{1}
	blc0 := common.Balance{0}
	blc1 := common.Balance{1}
	blc2 := common.Balance{2}

	archive.Add(1, common.Update{
		CreatedAccounts: []common.Address{addr1},
		Balances: []common.BalanceUpdate{
			{Account: addr1, Balance: blc1},
		},
	})

	archive.Add(3, common.Update{
		Balances: []common.BalanceUpdate{
			{Account: addr1, Balance: blc2},
		},
	})

	want := []common.Balance{blc0, blc1, blc1, blc2}
	for i, want := range want {
		got, err := archive.GetBalance(uint64(i), addr1)
		if err != nil || got != want {
			t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, want, err)
		}
	}
}
