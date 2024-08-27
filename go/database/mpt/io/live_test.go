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
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

func TestIO_ExportAndImportAsLiveDb(t *testing.T) {
	genesis, hash := exportExampleState(t)

	buffer := bytes.NewBuffer(genesis)
	targetDir := t.TempDir()
	if err := ImportLiveDb(NewLog(), targetDir, buffer); err != nil {
		t.Fatalf("failed to import DB: %v", err)
	}

	if err := mpt.VerifyFileLiveTrie(targetDir, mpt.S5LiveConfig, nil); err != nil {
		t.Fatalf("verification of imported DB failed: %v", err)
	}

	db, err := mpt.OpenGoFileState(targetDir, mpt.S5LiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
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
	if err := InitializeArchive(NewLog(), targetDir, buffer, genesisBlock); err != nil {
		t.Fatalf("failed to import DB: %v", err)
	}

	if err := mpt.VerifyArchiveTrie(targetDir, mpt.S5ArchiveConfig, nil); err != nil {
		t.Fatalf("verification of imported DB failed: %v", err)
	}

	db, err := mpt.OpenArchiveTrie(targetDir, mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
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

func TestIO_ExportedDataIsDeterministic(t *testing.T) {
	reference, _ := exportExampleState(t)
	for i := 0; i < 10; i++ {
		data, _ := exportExampleState(t)
		if !bytes.Equal(data, reference) {
			t.Fatalf("exported data is not deterministic")
		}
	}
}

func TestIO_ExportedDataDoesNotContainExtraCodes(t *testing.T) {
	reference, referenceHash := exportExampleState(t)

	// Modify the state by adding and removing code from an account.
	// This temporary code should not be included in the resulting exported data.
	modified, modifiedHash := exportExampleStateWithModification(t, func(s *mpt.MptState) {
		codesBefore := s.GetCodes()

		addr1 := common.Address{1}
		code, err := s.GetCode(addr1)
		if err != nil {
			t.Fatalf("failed to fetch code: %v", err)
		}
		modified := append(code, []byte("extra_code")...)
		s.SetCode(addr1, modified)
		s.SetCode(addr1, code)
		codesAfter := s.GetCodes()
		if before, after := len(codesBefore), len(codesAfter); before+1 != after {
			t.Fatalf("modification did not had expected code-altering effect: %d -> %d", before, after)
		}
	})

	// Check that the test indeed did not modify the state content.
	if referenceHash != modifiedHash {
		t.Fatalf("modified state has different hash than reference state: got: %x, want %x", modifiedHash, referenceHash)
	}

	// The extra code that was only temporary in the state should no be included
	// in the exported data.
	if !bytes.Equal(reference, modified) {
		t.Fatalf("exported data contains extra codes")
	}
}

func exportExampleState(t *testing.T) ([]byte, common.Hash) {
	t.Helper()
	return exportExampleStateWithModification(t, nil)
}

func createExampleLiveDB(t *testing.T, sourceDir string) *mpt.MptState {
	// Create a small LiveDB.
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5LiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	addr3 := common.Address{3}
	addr4 := common.Address{4}
	key1 := common.Key{1}
	key2 := common.Key{2}
	value1 := common.Value{1}
	value2 := common.Value{2}
	err = errors.Join(
		// First account, with code.
		db.SetNonce(addr1, common.ToNonce(1)),
		db.SetBalance(addr1, amount.New(12)),
		db.SetStorage(addr1, key1, value1),
		db.SetCode(addr1, []byte("some_code")),
		// Second account, without code.
		db.SetNonce(addr2, common.ToNonce(2)),
		db.SetBalance(addr2, amount.New(14)),
		db.SetStorage(addr2, key1, value1),
		db.SetStorage(addr2, key2, value2),
		// Third account, with different code as first account.
		db.SetNonce(addr3, common.ToNonce(3)),
		db.SetBalance(addr3, amount.New(16)),
		db.SetCode(addr3, []byte("some_other_code")),
		// Fourth account, with same code as first account.
		db.SetNonce(addr4, common.ToNonce(4)),
		db.SetBalance(addr4, amount.New(18)),
		db.SetCode(addr4, []byte("some_code")),
	)

	if err != nil {
		t.Fatalf("failed to seed test DB: %v", err)

	}
	return db
}

func exportExampleStateWithModification(t *testing.T, modify func(s *mpt.MptState)) ([]byte, common.Hash) {
	t.Helper()
	sourceDir := t.TempDir()

	db := createExampleLiveDB(t, sourceDir)

	if modify != nil {
		modify(db)
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
	if err := Export(context.Background(), NewLog(), sourceDir, &buffer); err != nil {
		t.Fatalf("failed to export DB: %v", err)
	}

	return buffer.Bytes(), hash
}

func TestImport_ImportIntoNonEmptyTargetDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+string(os.PathSeparator)+"test.txt", nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := ImportLiveDb(NewLog(), dir, nil); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitializeArchive_ImportIntoNonEmptyTargetDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+string(os.PathSeparator)+"test.txt", nil, 0700); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := InitializeArchive(NewLog(), dir, nil, 0); err == nil || !strings.Contains(err.Error(), "is not empty") {
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

func TestIO_ExportBlockFromArchive(t *testing.T) {
	// Create a small Archive from which we export LiveDB genesis.
	sourceDir := t.TempDir()
	archive, err := mpt.OpenArchiveTrie(sourceDir, mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	const (
		Blocks   = 10
		Accounts = 3
	)

	var expectedHashes []common.Hash

	for i := 0; i < Blocks; i++ {
		code := []byte{1, 2, 3, byte(i)}
		u := uint64(i)
		update := common.Update{}
		for j := 0; j < Accounts; j++ {
			newAddr := common.AddressFromNumber(j)

			update.CreatedAccounts = append(update.CreatedAccounts, newAddr)
			update.Balances = append(update.Balances, common.BalanceUpdate{Account: newAddr, Balance: amount.New(u + 1)})
			update.Nonces = append(update.Nonces, common.NonceUpdate{Account: newAddr, Nonce: common.ToNonce(u + 1)})
			update.Codes = append(update.Codes, common.CodeUpdate{Account: newAddr, Code: code})
			update.Slots = append(update.Slots, common.SlotUpdate{Account: newAddr, Key: common.Key{byte(j)}, Value: common.Value{byte(i)}})
		}
		err = archive.Add(u, update, nil)
		if err != nil {
			t.Fatalf("failed to create block in archive: %v", err)
		}

		hash, err := archive.GetHash(u)
		if err != nil {
			t.Fatalf("cannot get hash: %v", err)
		}

		expectedHashes = append(expectedHashes, hash)
	}

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive archive: %v", err)
	}

	for i := 0; i < Blocks; i++ {
		// Export live database from archive.
		buffer := new(bytes.Buffer)
		if err := ExportBlockFromArchive(context.Background(), NewLog(), sourceDir, buffer, uint64(i)); err != nil {
			t.Fatalf("failed to export Archive: %v", err)
		}

		// Import live database.
		targetDir := t.TempDir()
		if err := ImportLiveDb(NewLog(), targetDir, buffer); err != nil {
			t.Fatalf("failed to import DB: %v", err)
		}

		if err := mpt.VerifyFileLiveTrie(targetDir, mpt.S5LiveConfig, nil); err != nil {
			t.Fatalf("verification of imported DB failed: %v", err)
		}

		db, err := mpt.OpenGoFileState(targetDir, mpt.S5LiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
		if err != nil {
			t.Fatalf("failed to open recovered DB: %v", err)
		}

		got, err := db.GetHash()
		if err != nil {
			t.Fatalf("cannot get hash: %v", err)
		}
		want := expectedHashes[i]
		if got != want {
			t.Errorf("restored DB failed to reproduce same hash\nwanted %x\n got %x", want, got)
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("cannot close database: %v", err)
		}
	}

}

func TestIO_ExportBlockFromOnlineArchive(t *testing.T) {
	archive, err := mpt.OpenArchiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	defer func() {
		if err := archive.Close(); err != nil {
			t.Fatalf("failed to close archive archive: %v", err)
		}
	}()

	const (
		Blocks   = 10
		Accounts = 3
	)

	for i := 0; i < Blocks; i++ {
		code := []byte{1, 2, 3, byte(i)}
		u := uint64(i)
		update := common.Update{}
		for j := 0; j < Accounts; j++ {
			newAddr := common.AddressFromNumber(j)

			update.CreatedAccounts = append(update.CreatedAccounts, newAddr)
			update.Balances = append(update.Balances, common.BalanceUpdate{Account: newAddr, Balance: amount.New(u + 1)})
			update.Nonces = append(update.Nonces, common.NonceUpdate{Account: newAddr, Nonce: common.ToNonce(u + 1)})
			update.Codes = append(update.Codes, common.CodeUpdate{Account: newAddr, Code: code})
			update.Slots = append(update.Slots, common.SlotUpdate{Account: newAddr, Key: common.Key{byte(j)}, Value: common.Value{byte(i)}})
		}
		err = archive.Add(u, update, nil)
		if err != nil {
			t.Fatalf("failed to create block in archive: %v", err)
		}
		hashArchive, err := archive.GetHash(u)
		if err != nil {
			t.Fatalf("cannot get hash: %v", err)
		}

		// while the archive has not been explicitly flushed or closed, the data should be available when exporting
		buffer := new(bytes.Buffer)
		if err := ExportBlockFromOnlineArchive(context.Background(), NewLog(), archive, buffer, uint64(i)); err != nil {
			t.Fatalf("failed to export Archive: %v", err)
		}

		// Import live database.
		targetDir := t.TempDir()
		if err := ImportLiveDb(nil, targetDir, buffer); err != nil {
			t.Fatalf("failed to import DB: %v", err)
		}

		if err := mpt.VerifyFileLiveTrie(targetDir, mpt.S5LiveConfig, nil); err != nil {
			t.Fatalf("verification of imported DB failed: %v", err)
		}

		db, err := mpt.OpenGoFileState(targetDir, mpt.S5LiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
		if err != nil {
			t.Fatalf("failed to open recovered DB: %v", err)
		}

		hashLive, err := db.GetHash()
		if err != nil {
			t.Fatalf("cannot get hash: %v", err)
		}

		if got, want := hashLive, hashArchive; got != want {
			t.Errorf("restored DB failed to reproduce same hash\nwanted %x\n got %x", want, got)
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("cannot close database: %v", err)
		}
	}
}

func TestIO_Live_Import_IncorrectMagicNumberIsNoticed(t *testing.T) {
	b := bytes.NewBuffer(archiveMagicNumber)
	// fill the buffer with zero data to match the size
	_, err := b.Write(make([]byte, len(stateMagicNumber)))
	if err != nil {
		t.Fatalf("cannot write magic number: %v", err)
	}
	_, _, err = runImport(NewLog(), t.TempDir(), b, mpt.MptConfig{})
	if err == nil {
		t.Fatal("import must fail")
	}

	got := err.Error()
	want := "incorrect input data format use the `import-archive` sub-command  with this type of data"
	if !strings.EqualFold(got, want) {
		t.Errorf("unexpected error message\ngot: %v\nwant:%v", got, want)
	}
}
