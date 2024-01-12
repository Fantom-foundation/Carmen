package mpt

import (
	"bufio"
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
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
			archive, err := OpenArchiveTrie(t.TempDir(), config, 1024)
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
			archive, err := OpenArchiveTrie(t.TempDir(), config, 1024)
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

func TestArchiveTrie_CanHandleEmptyBlocks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, 1024)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			addr := common.Address{1}
			balance := common.Balance{0}

			// Block 1 adds an actual change.
			err = archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr},
				Balances: []common.BalanceUpdate{
					{Account: addr, Balance: balance},
				},
			}, nil)
			if err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			// Block 2 is skipped.

			// Block 3 is empty, without hints.
			if err := archive.Add(3, common.Update{}, nil); err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			// Block 4 is empty, with hints.
			if err := archive.Add(4, common.Update{}, nil); err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			for i := 0; i < 5; i++ {
				got, err := archive.GetBalance(uint64(i), addr)
				if err != nil || got != balance {
					t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, balance, err)
				}
			}
		})
	}
}

func TestArchiveTrie_CanProcessPrecomputedHashes(t *testing.T) {
	for _, config := range allMptConfigs {
		if config.HashStorageLocation != HashStoredWithNode {
			continue
		}
		t.Run(config.Name, func(t *testing.T) {
			live, err := OpenGoMemoryState(t.TempDir(), config, 1024)
			if err != nil {
				t.Fatalf("failed to open live trie: %v", err)
			}

			archiveDir := t.TempDir()
			archive, err := OpenArchiveTrie(archiveDir, config, 1024)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			blc1 := common.Balance{1}
			blc2 := common.Balance{2}

			// Block 1
			update := common.Update{
				CreatedAccounts: []common.Address{addr1, addr2},
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc1},
					{Account: addr2, Balance: blc2},
				},
			}
			hints, err := live.Apply(1, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}
			err = archive.Add(1, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			// Block 2
			update = common.Update{
				Balances: []common.BalanceUpdate{{Account: addr1, Balance: blc2}},
			}
			hints, err = live.Apply(2, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}

			err = archive.Add(2, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			// Block 4 -- larger range of data
			update = common.Update{}
			for i := 0; i < 100; i++ {
				addr := common.Address{byte(i + 10)}
				err = errors.Join(
					live.CreateAccount(addr),
					live.SetBalance(addr, blc1),
				)
				if err != nil {
					t.Fatalf("failed to update live db: %v", err)
				}
				update.CreatedAccounts = append(update.CreatedAccounts, addr)
				update.Balances = append(update.Balances, common.BalanceUpdate{Account: addr, Balance: blc1})
			}
			hints, err = live.Apply(4, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}
			err = archive.Add(4, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := errors.Join(live.Close(), archive.Close()); err != nil {
				t.Fatalf("failed to close resources: %v", err)
			}

			if err := VerifyArchive(archiveDir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("failed to verify archive: %v", err)
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
			archive, err := OpenArchiveTrie(dir, config, 1024)
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
			archive, err := OpenArchiveTrie(dir, config, 1024)
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
			archive, err := OpenArchiveTrie(dir, config, 1024)
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
		{NewNodeReference(ValueId(12)), common.Hash{12}},
		{NewNodeReference(ValueId(14)), common.Hash{14}},
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

func TestArchiveTrie_StoreLoadRoots(t *testing.T) {
	roots := []Root{}
	for i := 0; i < 48; i++ {
		id := NodeId(uint64(1) << i)
		roots = append(roots, Root{NodeRef: NewNodeReference(id)})
		id = NodeId((uint64(1) << (i + 1)) - 1)
		roots = append(roots, Root{NodeRef: NewNodeReference(id)})
	}

	dir := t.TempDir()
	file := dir + string(filepath.Separator) + "roots.dat"
	if err := StoreRoots(file, roots); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	restored, err := loadRoots(file)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if len(roots) != len(restored) {
		t.Fatalf("invalid number of restored roots, wanted %d, got %d", len(roots), len(restored))
	}

	for i := 0; i < len(roots); i++ {
		want := roots[i].NodeRef.Id()
		got := restored[i].NodeRef.Id()
		if want != got {
			t.Errorf("invalid restored root at position %d, wanted %v, got %v", i, want, got)
		}
	}
}

func TestArchiveTrie_RecreateAccount_ClearStorage(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, 1024)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}

			addr := common.Address{0xA}
			key := common.Key{0xB}
			val := common.Value{0xC}

			// create an account with a non-empty slot
			update := common.Update{}
			update.AppendCreateAccount(addr)
			update.AppendSlotUpdate(addr, key, val)
			if err := archive.Add(0, update, nil); err != nil {
				t.Errorf("cannot add update: %s", err)
			}

			// re-create an account in the next block
			update = common.Update{}
			update.AppendCreateAccount(addr)
			if err := archive.Add(1, update, nil); err != nil {
				t.Errorf("cannot add update: %s", err)
			}

			// verify that the account is re-created with an empty slot
			exists, err := archive.Exists(1, addr)
			if err != nil {
				t.Errorf("cannot check account existence: %s", err)
			}
			if !exists {
				t.Errorf("account does not exist")
			}
			storage, err := archive.GetStorage(1, addr, key)
			if err != nil {
				t.Errorf("cannot get slot value: %s", err)
			}

			var empty common.Value
			if storage != empty {
				t.Errorf("value is not empty, but it is: %v", storage)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close empty archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_QueryLoadTest(t *testing.T) {
	// Goal: stress-test an archive with a limited node cache.
	archive, err := OpenArchiveTrie(t.TempDir(), S5ArchiveConfig, 30_000)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}

	// We fill the archive with N blocks, each with N accounts and N slots.
	const N = 100
	for b := 0; b < N; b++ {
		update := common.Update{}
		for a := 0; a < N; a++ {
			addr := common.Address{byte(a)}
			if b == 0 {
				update.AppendCreateAccount(addr)
			}
			for k := 0; k < N; k++ {
				update.AppendSlotUpdate(addr, common.Key{byte(k)}, common.Value{byte(b), byte(a), byte(k)})
			}
		}
		if err := archive.Add(uint64(b), update, nil); err != nil {
			t.Errorf("failed to add update to archive: %v", err)
		}
	}

	// In a second step, random queries are send concurrently into the archive.
	const Q = 10_000
	P := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(P)
	for i := 0; i < P; i++ {
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			for i := 0; i < Q; i++ {
				block := uint64(r.Intn(N))
				addr := common.Address{byte(r.Intn(N))}
				key := common.Key{byte(r.Intn(N))}

				live, err := archive.getView(block)
				if err != nil {
					t.Errorf("failed to get view to block %d: %v", block, err)
					continue
				}
				value, err := live.GetValue(addr, key)
				if err != nil {
					t.Errorf("failed to get value from archive: %d/%v/%v: %v", block, addr, key, err)
				}
				if want, got := (common.Value{byte(block), addr[0], key[0]}), value; want != got {
					t.Errorf("wrong result for lookup %d/%v/%v: wanted %v, got %v", block, addr, key, want, got)
				}
			}
		}(i)
	}
	wg.Wait()

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
}
