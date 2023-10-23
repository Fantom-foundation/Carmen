package mpt

import (
	"bufio"
	"bytes"
	"os"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Note: most properties of the ArchiveTrie are tested through the common
// test infrastructure in /backend/archive.
//
// TODO: generalize common archive tests in /backend/archive such that they
// can be executed as part of this package's test suite

func TestArchiveTrie_OpenAndClose(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close empty archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_CanHandleMultipleBlocks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config)
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
			}, nil)

			archive.Add(3, common.Update{
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc2},
				},
			}, nil)

			want := []common.Balance{blc0, blc1, blc1, blc2}
			for i, want := range want {
				got, err := archive.GetBalance(uint64(i), addr1)
				if err != nil || got != want {
					t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, want, err)
				}
			}
		})
	}
}

func TestArchiveTrie_VerificationOfEmptyDirectoryPasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := VerifyArchive(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("an empty directory should be fine, got: %v", err)
			}
		})
	}
}

func TestArchiveTrie_VerificationOfFreshArchivePasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config)
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			err = archive.Add(4, common.Update{
				CreatedAccounts: []common.Address{{3}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{3}, Nonce: common.ToNonce(3)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{3}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{3}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			if err := VerifyArchive(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("a freshly closed archive should be fine, got: %v", err)
			}
		})
	}
}

func TestArchiveTrie_VerificationOfArchiveWithMissingFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config)
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			if err := os.Remove(dir + "/branches/freelist.dat"); err != nil {
				t.Fatalf("failed to delete file")
			}

			if err := VerifyArchive(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("missing file should be detected")
			}
		})
	}
}

func TestArchiveTrie_VerificationOfArchiveWithCorruptedFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config)
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			// manipulate one of the files
			filename := dir + "/branches/values.dat"
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatalf("failed to load data from file: %v", err)
			}
			data[0]++
			if err := os.WriteFile(filename, data, 0600); err != nil {
				t.Fatalf("failed to modify file")
			}

			if err := VerifyArchive(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("corrupted file should have been detected")
			}
		})
	}
}

func TestArchiveTrie_CanLoadRootsFromJunkySource(t *testing.T) {

	roots := []Root{
		{ValueId(12), common.Hash{12}},
		{ValueId(14), common.Hash{14}},
	}

	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	storeRootsTo(writer, roots)
	writer.Flush()

	for _, size := range []int{1, 2, 4, 1024} {
		reader := utils.NewChunkReader(b.Bytes(), size)
		res, err := loadRootsFrom(reader)
		if err != nil {
			t.Fatalf("error loading roots: %v", err)
		}
		if !reflect.DeepEqual(roots, res) {
			t.Errorf("failed to restore roots, wanted %v, got %v", roots, res)
		}
	}
}
