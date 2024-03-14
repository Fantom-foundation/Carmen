package io

import (
	"bytes"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

func TestIO_Archive_ExportAndImport(t *testing.T) {

	// Create a small Archive to be exported.
	sourceDir := t.TempDir()
	source, err := mpt.OpenArchiveTrie(sourceDir, mpt.S5ArchiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	blockHeight := fillTestBlocksIntoArchive(t, source)

	hashes := []common.Hash{}
	for i := 0; i <= blockHeight; i++ {
		hash, err := source.GetHash(uint64(i))
		if err != nil {
			t.Fatalf("failed to fetch hash for block %d: %v", i, err)
		}
		hashes = append(hashes, hash)
	}

	if err := source.Close(); err != nil {
		t.Fatalf("failed to close source archive: %v", err)
	}

	// Export the archive into a buffer.
	buffer := new(bytes.Buffer)
	if err := ExportArchive(sourceDir, buffer); err != nil {
		t.Fatalf("failed to export Archive: %v", err)
	}
	genesis := buffer.Bytes()

	// Import the archive into a new directory.
	targetDir := t.TempDir()
	buffer = bytes.NewBuffer(genesis)
	if err := ImportArchive(targetDir, buffer); err != nil {
		t.Fatalf("failed to import Archive: %v", err)
	}

	if err := mpt.VerifyArchive(targetDir, mpt.S5ArchiveConfig, nil); err != nil {
		t.Fatalf("verification of imported Archive failed: %v", err)
	}

	target, err := mpt.OpenArchiveTrie(targetDir, mpt.S5ArchiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to open recovered Archive: %v", err)
	}
	defer target.Close()

	height, _, err := target.GetBlockHeight()
	if err != nil {
		t.Fatalf("failed to get block height from recovered archive: %v", err)
	}
	if height != uint64(blockHeight) {
		t.Fatalf("unexpected block height in recovered Archive, wanted %d, got %d", 3, height)
	}

	for i, want := range hashes {
		got, err := target.GetHash(uint64(i))
		if err != nil {
			t.Fatalf("failed to fetch hash for block %d: %v", i, err)
		}
		if want != got {
			t.Errorf("wrong hash for block %d, wanted %v, got %v", i, want, got)
		}
	}
}

func fillTestBlocksIntoArchive(t *testing.T, archive *mpt.ArchiveTrie) (blockHeight int) {

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	balance1 := common.Balance{1}
	balance2 := common.Balance{2}
	nonce1 := common.Nonce{1}
	nonce2 := common.Nonce{2}
	code1 := []byte{1, 2, 3}

	err := archive.Add(0, common.Update{
		CreatedAccounts: []common.Address{addr1},
		Balances:        []common.BalanceUpdate{{Account: addr1, Balance: balance1}},
		Nonces:          []common.NonceUpdate{{Account: addr1, Nonce: nonce1}},
		Codes:           []common.CodeUpdate{{Account: addr1, Code: code1}},
		Slots: []common.SlotUpdate{
			{Account: addr1, Key: common.Key{1}, Value: common.Value{1}},
			{Account: addr1, Key: common.Key{2}, Value: common.Value{2}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create block in archive: %v", err)
	}

	err = archive.Add(3, common.Update{
		CreatedAccounts: []common.Address{addr2},
		Balances:        []common.BalanceUpdate{{Account: addr2, Balance: balance2}},
		Nonces:          []common.NonceUpdate{{Account: addr2, Nonce: nonce2}},
		Slots: []common.SlotUpdate{
			{Account: addr1, Key: common.Key{1}, Value: common.Value{0}},
			{Account: addr1, Key: common.Key{2}, Value: common.Value{3}},
			{Account: addr2, Key: common.Key{1}, Value: common.Value{2}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create block in archive: %v", err)
	}

	err = archive.Add(7, common.Update{
		DeletedAccounts: []common.Address{addr1},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create block in archive: %v", err)
	}

	return 7
}