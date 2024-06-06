// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
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

func createExampleLiveDB(t *testing.T, sourceDir string) common.Hash {
	t.Helper()
	// Create a small LiveDB.
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5LiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
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

	return hash
}

func exportExampleState(t *testing.T) ([]byte, common.Hash) {
	t.Helper()
	sourceDir := t.TempDir()

	hash := createExampleLiveDB(t, sourceDir)

	// Export database to buffer.
	var buffer bytes.Buffer
	if err := Export(sourceDir, &buffer); err != nil {
		t.Fatalf("failed to export DB: %v", err)
	}

	return buffer.Bytes(), hash
}

func TestImport_ImportIntoNonEmptyTargetDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+string(os.PathSeparator)+"test.txt", nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := ImportLiveDb(dir, nil); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitializeArchive_ImportIntoNonEmptyTargetDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+string(os.PathSeparator)+"test.txt", nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := InitializeArchive(dir, nil, 0); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckEmptyDirectory_PassesIfEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := checkEmptyDirectory(dir); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckEmptyDirectory_FailsIfDirectoryDoesNotExist(t *testing.T) {
	dir := t.TempDir()
	if err := checkEmptyDirectory(dir + string(os.PathSeparator) + "sub"); err == nil {
		t.Errorf("test expected to produce an error")
	}
}

func TestCheckEmptyDirectory_FailsIfDirectoryIsAFile(t *testing.T) {
	dir := t.TempDir()
	file := dir + string(os.PathSeparator) + "test.txt"
	if err := os.WriteFile(file, nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := checkEmptyDirectory(file); err == nil {
		t.Errorf("test expected to produce an error")
	}
}

func TestCheckEmptyDirectory_FailsIfDirectoryContainsAFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+string(os.PathSeparator)+"test.txt", nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := checkEmptyDirectory(dir); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckEmptyDirectory_FailsIfDirectoryContainsADirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(dir+string(os.PathSeparator)+"sub", 0700); err != nil {
		t.Fatalf("failed to create sub-directory: %v", err)
	}
	if err := checkEmptyDirectory(dir); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("unexpected error: %v", err)
	}
}
