package io

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

func TestIO_ExportAndImportAsLiveDb(t *testing.T) {
	genesis, hash := exportExampleState(t)

	buffer := bytes.NewBuffer(genesis)
	targetDir := t.TempDir()
	if err := ImportLiveDb(targetDir, buffer); err != nil {
		t.Fatalf("failed to import DB: %v", err)
	}

	if err := mpt.VerifyFileLiveTrie(targetDir, mpt.S5LiveConfig, nil); err != nil {
		t.Fatalf("verification of imported DB failed: %v", err)
	}

	db, err := mpt.OpenGoFileState(targetDir, mpt.S5LiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to open recovered DB: %v", err)
	}
	defer db.Close()

	if exists, err := db.Exists(common.Address{1}); err != nil || !exists {
		t.Fatalf("restored DB does not contain account 1")
	}
	if exists, err := db.Exists(common.Address{2}); err != nil || !exists {
		t.Fatalf("restored DB does not contain account 2")
	}

	if got, err := db.GetHash(); err != nil || got != hash {
		t.Fatalf("restored DB failed to reproduce same hash\nwanted %x\n   got %x\n   err %v", hash, got, err)
	}
}

func TestIO_ExportAndImportAsArchive(t *testing.T) {
	genesis, hash := exportExampleState(t)

	buffer := bytes.NewBuffer(genesis)
	targetDir := t.TempDir()
	genesisBlock := uint64(12)
	if err := InitializeArchive(targetDir, buffer, genesisBlock); err != nil {
		t.Fatalf("failed to import DB: %v", err)
	}

	if err := mpt.VerifyArchive(targetDir, mpt.S5ArchiveConfig, nil); err != nil {
		t.Fatalf("verification of imported DB failed: %v", err)
	}

	db, err := mpt.OpenArchiveTrie(targetDir, mpt.S5ArchiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to open recovered DB: %v", err)
	}
	defer db.Close()

	height, empty, err := db.GetBlockHeight()
	if err != nil || empty || height != genesisBlock {
		t.Fatalf("invalid block height, wanted %d, got %d, empty %t, err %v", genesisBlock, height, empty, err)
	}

	if exists, err := db.Exists(genesisBlock, common.Address{1}); err != nil || !exists {
		t.Fatalf("restored DB does not contain account 1")
	}
	if exists, err := db.Exists(genesisBlock, common.Address{2}); err != nil || !exists {
		t.Fatalf("restored DB does not contain account 2")
	}

	if got, err := db.GetHash(genesisBlock); err != nil || got != hash {
		t.Fatalf("restored DB failed to reproduce same hash\nwanted %x\n   got %x\n   err %v", hash, got, err)
	}

	for i := uint64(0); i < genesisBlock; i++ {
		if got, err := db.GetHash(i); err != nil || got != mpt.EmptyNodeEthereumHash {
			t.Fatalf("invalid hash for pre-genesis block %d\nwanted %x\n   got %x\n   err %v", i, mpt.EmptyNodeEthereumHash, got, err)
		}
	}
}

func exportExampleState(t *testing.T) ([]byte, common.Hash) {
	t.Helper()
	sourceDir := t.TempDir()

	// Create a small LiveDB.
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5LiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	key1 := common.Key{1}
	key2 := common.Key{2}
	value1 := common.Value{1}
	value2 := common.Value{2}
	err = errors.Join(
		// First account, with code.
		db.SetNonce(addr1, common.ToNonce(1)),
		db.SetBalance(addr1, common.Balance{12}),
		db.SetStorage(addr1, key1, value1),
		db.SetCode(addr1, []byte("some_code")),
		// Second account, without code.
		db.SetNonce(addr2, common.ToNonce(2)),
		db.SetBalance(addr2, common.Balance{14}),
		db.SetStorage(addr2, key1, value1),
		db.SetStorage(addr2, key2, value2),
	)

	if err != nil {
		t.Fatalf("failed to seed test DB: %v", err)
	}

	hash, err := db.GetHash()
	if err != nil {
		t.Fatalf("failed to fetch hash from DB: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}

	// Export database to buffer.
	var buffer bytes.Buffer
	if err := Export(sourceDir, &buffer); err != nil {
		t.Fatalf("failed to export DB: %v", err)
	}

	return buffer.Bytes(), hash
}
