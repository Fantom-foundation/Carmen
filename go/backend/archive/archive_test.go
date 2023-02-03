package archive

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/archive/sqlite"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

type archiveFactory struct {
	label      string
	getArchive func(tempDir string) Archive
}

func getArchiveFactories(tb testing.TB) []archiveFactory {
	return []archiveFactory{
		{
			label: "SQLite",
			getArchive: func(tempDir string) Archive {
				archive, err := sqlite.NewArchive(tempDir + "/archive.sqlite")
				if err != nil {
					tb.Fatalf("failed to create archive; %s", err)
				}
				return archive
			},
		},
	}
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
			}); err != nil {
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
			}); err != nil {
				t.Fatalf("failed to add block 5; %s", err)
			}

			if balance, err := a.GetBalance(1, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 1: %x; %s", balance, err)
			}
			if balance, err := a.GetBalance(3, addr1); err != nil || balance != (common.Balance{0x12}) {
				t.Errorf("unexpected balance at block 3: %x; %s", balance, err)
			}
			if balance, err := a.GetBalance(5, addr1); err != nil || balance != (common.Balance{0x34}) {
				t.Errorf("unexpected balance at block 5: %x; %s", balance, err)
			}

			if code, err := a.GetCode(3, addr1); err != nil || code != nil {
				t.Errorf("unexpected code at block 0: %x; %s", code, err)
			}
			if code, err := a.GetCode(5, addr1); err != nil || !bytes.Equal(code, []byte{0x12, 0x23}) {
				t.Errorf("unexpected code at block 5: %x; %s", code, err)
			}

			if nonce, err := a.GetNonce(4, addr1); err != nil || nonce != (common.Nonce{}) {
				t.Errorf("unexpected nonce at block 0: %x; %s", nonce, err)
			}
			if nonce, err := a.GetNonce(5, addr1); err != nil || nonce != (common.Nonce{0x54}) {
				t.Errorf("unexpected nonce at block 5: %x; %s", nonce, err)
			}

			if value, err := a.GetStorage(0, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 0: %x; %s", value, err)
			}
			if value, err := a.GetStorage(2, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
				t.Errorf("unexpected value at block 2: %x; %s", value, err)
			}
			if value, err := a.GetStorage(6, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x89}) {
				t.Errorf("unexpected value at block 6: %x; %s", value, err)
			}

			if lastBlock, err := a.GetLastBlockHeight(); err != nil || lastBlock != 5 {
				t.Errorf("unexpected last block height: %d; %s", lastBlock, err)
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
			}); err != nil {
				t.Fatalf("failed to add block 1; %s", err)
			}

			if err := a.Add(5, common.Update{
				DeletedAccounts: []common.Address{addr1},
			}); err != nil {
				t.Fatalf("failed to add block 5; %s", err)
			}

			if err := a.Add(9, common.Update{
				CreatedAccounts: []common.Address{addr1},
			}); err != nil {
				t.Fatalf("failed to add block 9; %s", err)
			}

			if exists, err := a.Exist(1, addr1); err != nil || exists != true {
				t.Errorf("unexpected existence status at block 1: %t; %s", exists, err)
			}
			if exists, err := a.Exist(5, addr1); err != nil || exists != false {
				t.Errorf("unexpected existence status at block 1: %t; %s", exists, err)
			}
			if exists, err := a.Exist(9, addr1); err != nil || exists != true {
				t.Errorf("unexpected existence status at block 1: %t; %s", exists, err)
			}

			if value, err := a.GetStorage(1, addr1, common.Key{0x05}); err != nil || value != (common.Value{0x47}) {
				t.Errorf("unexpected value at block 1: %x; %s", value, err)
			}
			if value, err := a.GetStorage(5, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 5: %x; %s", value, err)
			}
			if value, err := a.GetStorage(9, addr1, common.Key{0x05}); err != nil || value != (common.Value{}) {
				t.Errorf("unexpected value at block 9: %x; %s", value, err)
			}

		})
	}
}
