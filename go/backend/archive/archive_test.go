package archive_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/sqlite"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

type archiveFactory struct {
	label      string
	getArchive func(tempDir string) archive.Archive
	customHash bool
}

func getArchiveFactories(tb testing.TB) []archiveFactory {
	return []archiveFactory{
		{
			label: "SQLite",
			getArchive: func(tempDir string) archive.Archive {
				archive, err := sqlite.NewArchive(tempDir + "/archive.sqlite")
				if err != nil {
					tb.Fatalf("failed to create archive; %s", err)
				}
				return archive
			},
		},
		{
			label: "LevelDB",
			getArchive: func(tempDir string) archive.Archive {
				db, err := common.OpenLevelDb(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to open LevelDB; %s", err)
				}
				archive, err := ldb.NewArchive(db)
				if err != nil {
					tb.Fatalf("failed to create archive; %s", err)
				}
				return &ldbArchiveWrapper{archive, db}
			},
		},
		{
			label: "S4",
			getArchive: func(tempDir string) archive.Archive {
				archive, err := mpt.OpenArchiveTrie(tempDir, mpt.S4ArchiveConfig)
				if err != nil {
					tb.Fatalf("failed to open S4 archive: %v", err)
				}
				return archive
			},
			customHash: true,
		},
		{
			label: "S5",
			getArchive: func(tempDir string) archive.Archive {
				archive, err := mpt.OpenArchiveTrie(tempDir, mpt.S5ArchiveConfig)
				if err != nil {
					tb.Fatalf("failed to open S5 archive: %v", err)
				}
				return archive
			},
			customHash: true,
		},
	}
}

// ldbArchiveWrapper wraps the ldb.Archive to close the LevelDB on the archive Close
type ldbArchiveWrapper struct {
	archive.Archive
	db io.Closer
}

func (w *ldbArchiveWrapper) Close() error {
	err := w.Archive.Close()
	if err != nil {
		return err
	}
	return w.db.Close()
}

var (
	addr1 = common.Address{0x01}
)

func TestAddGet(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x12}},
				},
				Codes:  nil,
				Nonces: nil,
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x05}, common.Value{0x47}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(5, common.Update{
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x34}},
				},
				Codes: []common.CodeUpdate{
					{addr1, []byte{0x12, 0x23}},
				},
				Nonces: []common.NonceUpdate{
					{addr1, common.Nonce{0x54}},
				},
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x05}, common.Value{0x89}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 5; %v", err)
			}
			if err := a.Add(7, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add block 7; %v", err)
			}

			if balance, err := a.GetBalance(1, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 1: %x; %v", balance, err)
			}
			if balance, err := a.GetBalance(3, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 3: %x; %v", balance, err)
			}
			if balance, err := a.GetBalance(5, addr1); err != nil || balance != (common.Balance{0x34}) {
				t.Errorf("unexpected balance at block 5: %x; %v", balance, err)
			}

			if code, err := a.GetCode(3, addr1); err != nil || code != nil {
				t.Errorf("unexpected code at block 0: %x; %v", code, err)
			}
			if code, err := a.GetCode(5, addr1); err != nil || !bytes.Equal(code, []byte{0x12, 0x23}) {
				t.Errorf("unexpected code at block 5: %x; %v", code, err)
			}

			if nonce, err := a.GetNonce(4, addr1); err != nil || nonce != (common.Nonce{}) {
				t.Errorf("unexpected nonce at block 0: %x; %v", nonce, err)
			}
			if nonce, err := a.GetNonce(5, addr1); err != nil || nonce != (common.Nonce{0x54}) {
				t.Errorf("unexpected nonce at block 5: %x; %v", nonce, err)
			}

			if value, err := a.GetStorage(0, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 0: %x; %v", value, err)
			}
			if value, err := a.GetStorage(2, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
				t.Errorf("unexpected value at block 2: %x; %v", value, err)
			}
			if value, err := a.GetStorage(6, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x89}) {
				t.Errorf("unexpected value at block 6: %x; %v", value, err)
			}

			if lastBlock, empty, err := a.GetBlockHeight(); err != nil || empty || lastBlock != 7 {
				t.Errorf("unexpected last block height: %d; %t, %v", lastBlock, empty, err)
			}

			if !factory.customHash {
				if hash, err := a.GetHash(1); err != nil || fmt.Sprintf("%x", hash) != "9834327080d1ead8544edff892ae26c6fe0640dc13ded9c15338721081490b04" {
					t.Errorf("unexpected hash of block 1: %x; %v", hash, err)
				}
				if hash, err := a.GetHash(5); err != nil || fmt.Sprintf("%x", hash) != "6c616faab374f93e9322e02452da86dbbbf10e84c826afb5b13db0e2ef6e71e8" {
					t.Errorf("unexpected hash of block 5: %x; %v", hash, err)
				}
			}
		})
	}
}

func TestAccountDeleteCreate(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x12}},
				},
				Codes: []common.CodeUpdate{
					{addr1, []byte{0x12, 0x23}},
				},
				Nonces: []common.NonceUpdate{
					{addr1, common.Nonce{0x14}},
				},
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x05}, common.Value{0x47}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(5, common.Update{
				DeletedAccounts: []common.Address{addr1},
			}, nil); err != nil {
				t.Fatalf("failed to add block 5; %s", err)
			}

			if err := a.Add(9, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}, nil); err != nil {
				t.Fatalf("failed to add block 9; %s", err)
			}

			if exists, err := a.Exists(1, addr1); err != nil || exists != true {
				t.Errorf("unexpected existence status at block 1: %t; %v", exists, err)
			}
			if exists, err := a.Exists(5, addr1); err != nil || exists != false {
				t.Errorf("unexpected existence status at block 1: %t; %v", exists, err)
			}
			if exists, err := a.Exists(9, addr1); err != nil || exists != true {
				t.Errorf("unexpected existence status at block 1: %t; %v", exists, err)
			}

			if value, err := a.GetStorage(1, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
				t.Errorf("unexpected value at block 1: %x; %v", value, err)
			}
			if value, err := a.GetStorage(5, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 5: %x; %v", value, err)
			}
			if value, err := a.GetStorage(9, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 9: %x; %v", value, err)
			}

			if !factory.customHash {
				if hash, err := a.GetHash(1); err != nil || fmt.Sprintf("%x", hash) != "5631163004034afb5bd05e1a8ddfd4b00e26f3ab80da10be86542267a05b1103" {
					t.Errorf("unexpected hash of block 1: %x; %v", hash, err)
				}
				if hash, err := a.GetHash(5); err != nil || fmt.Sprintf("%x", hash) != "3d97c83a6e0da0fb5ada88bb93d161cd62834e25336b131d51d1a8bd74f7e617" {
					t.Errorf("unexpected hash of block 5: %x; %v", hash, err)
				}
				if hash, err := a.GetHash(9); err != nil || fmt.Sprintf("%x", hash) != "2be813b99a1b766bbcb9619ed4daa69b6c33397d916f55af6ea11742cec8b391" {
					t.Errorf("unexpected hash of block 9: %x; %v", hash, err)
				}
			}
		})
	}
}

func TestAccountStatusOnly(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}
			if err := a.Add(2, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add block 2; %s", err)
			}

			if exists, err := a.Exists(1, addr1); err != nil || !exists {
				t.Errorf("unexpected account status at block 1: %t; %s", exists, err)
			}
			if exists, err := a.Exists(2, addr1); err != nil || !exists {
				t.Errorf("unexpected account status at block 2: %t; %s", exists, err)
			}
		})
	}
}

func TestBalanceOnly(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x12}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(200, common.Update{
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x34}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 200; %s", err)
			}

			if err := a.Add(400, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add block 400; %s", err)
			}

			if balance, err := a.GetBalance(1, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 1: %x; %s", balance, err)
			}

			if balance, err := a.GetBalance(300, addr1); err != nil || balance != (common.Balance{0x34}) {
				t.Errorf("unexpected balance at block 3: %x; %s", balance, err)
			}
		})
	}
}

func TestStorageOnly(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x37}, common.Value{0x12}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(2, common.Update{
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x37}, common.Value{0x34}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 2; %s", err)
			}

			if value, err := a.GetStorage(1, addr1, common.Key{0x37}); err != nil || value != (common.Value{0x12}) {
				t.Errorf("unexpected value at block 1: %x; %s", value, err)
			}

			if value, err := a.GetStorage(2, addr1, common.Key{0x37}); err != nil || value != (common.Value{0x34}) {
				t.Errorf("unexpected value at block 2: %x; %s", value, err)
			}
		})
	}
}

func TestPreventingBlockOverrides(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(1, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x37}, common.Value{0x12}},
				},
			}, nil); err == nil {
				t.Errorf("allowed overriding already written block 1")
			}

			if value, err := a.GetStorage(1, addr1, common.Key{0x37}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 1: %x; %s", value, err)
			}

		})
	}
}

func TestPreventingBlockOutOfOrder(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(2, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}, nil); err != nil {
				t.Fatalf("failed to add block 2; %s", err)
			}

			if err := a.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Slots: []common.SlotUpdate{
					{addr1, common.Key{0x37}, common.Value{0x12}},
				},
			}, nil); err == nil {
				t.Errorf("allowed inserting block 1 while block 2 already exists")
			}

			if value, err := a.GetStorage(1, addr1, common.Key{0x37}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 1: %x; %s", value, err)
			}
		})
	}
}

func TestEmptyBlockHash(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(0, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 0; %v", err)
			}

			if err := a.Add(1, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 1; %v", err)
			}

			if err := a.Add(2, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}, nil); err != nil {
				t.Fatalf("failed to add block 2; %v", err)
			}

			if err := a.Add(3, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 3; %v", err)
			}

			if err := a.Add(4, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 4; %v", err)
			}

			if hash, err := a.GetHash(1); err != nil || (factory.label != "S5" && hash != (common.Hash{})) {
				t.Errorf("unexpected hash of block 1: %x; %v", hash, err)
			}
			hash2, err := a.GetHash(2)
			if err != nil || hash2 == (common.Hash{}) {
				t.Errorf("unexpected hash of block 1: %x; %v", hash2, err)
			}
			hash3, err := a.GetHash(3)
			if err != nil || hash2 != hash3 {
				t.Errorf("unexpected hash of block 3: %x != %x; %v", hash2, hash3, err)
			}
			hash4, err := a.GetHash(4)
			if err != nil || hash2 != hash4 {
				t.Errorf("unexpected hash of block 4: %x != %x; %v", hash2, hash4, err)
			}
		})
	}
}

func TestZeroBlock(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(0, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x11}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 0; %s", err)
			}

			if err := a.Add(1, common.Update{
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x12}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if exists, err := a.Exists(0, addr1); err != nil || !exists {
				t.Errorf("unexpected account status at block 0: %t; %s", exists, err)
			}
			if exists, err := a.Exists(1, addr1); err != nil || !exists {
				t.Errorf("unexpected account status at block 1: %t; %s", exists, err)
			}
			if balance, err := a.GetBalance(0, addr1); err != nil || balance != (common.Balance{0x11}) {
				t.Errorf("unexpected balance at block 0: %x; %s", balance, err)
			}
			if balance, err := a.GetBalance(1, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 1: %x; %s", balance, err)
			}
		})
	}
}

func TestTwinProtection(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			if err := a.Add(0, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 0; %s", err)
			}

			if err := a.Add(0, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}, nil); err == nil {
				t.Errorf("second adding of block 0 should have failed but it succeed")
			}

			if err := a.Add(1, common.Update{
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x12}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(1, common.Update{
				Balances: []common.BalanceUpdate{
					{addr1, common.Balance{0x34}},
				},
			}, nil); err == nil {
				t.Errorf("second adding of block 1 should have failed but it succeed")
			}
		})
	}
}

func TestBlockHeight(t *testing.T) {
	for _, factory := range getArchiveFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			a := factory.getArchive(t.TempDir())
			defer a.Close()

			// Initially, the block height should be indicated as empty.
			if _, empty, err := a.GetBlockHeight(); !empty || err != nil {
				t.Fatalf("failed to report proper block height for empty archive, got %t, err %v", empty, err)
			}

			// Adding block 0 should turn the archive non-empty.
			if err := a.Add(0, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add empty block 0; %s", err)
			}

			if height, empty, err := a.GetBlockHeight(); height != 0 || empty || err != nil {
				t.Fatalf("failed to report proper block height for archive with block height 0: %d, %t, %v", height, empty, err)
			}

			// Adding block 5 should raise the block height accordingly.
			if err := a.Add(5, common.Update{CreatedAccounts: []common.Address{addr1}}, nil); err != nil {
				t.Fatalf("failed to add block 5; %s", err)
			}

			if height, empty, err := a.GetBlockHeight(); height != 5 || empty || err != nil {
				t.Fatalf("failed to report proper block height for archive with block height 5: %d, %t, %v", height, empty, err)
			}
		})
	}
}
