package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

func TestIO_ExportAndImport(t *testing.T) {
	sourceDir := t.TempDir()

	// Create a small LiveDB.
	db, err := mpt.OpenGoFileState(sourceDir, mpt.S5LiveConfig)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{1}
	key1 := common.Key{1}
	key2 := common.Key{2}
	value1 := common.Value{1}
	value2 := common.Value{2}
	err = errors.Join(
		db.SetNonce(addr1, common.ToNonce(1)),
		db.SetStorage(addr1, key1, value1),
		db.SetCode(addr1, []byte("some_code")),
		db.SetNonce(addr2, common.ToNonce(1)),
		db.SetStorage(addr2, key1, value1),
		db.SetStorage(addr2, key2, value2),
		db.SetCode(addr2, []byte("some_other_code")),
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

	targetDir := t.TempDir()
	if err := Import(targetDir, &buffer); err != nil {
		t.Fatalf("failed to import DB: %v", err)
	}

	db, err = mpt.OpenGoFileState(targetDir, mpt.S5LiveConfig)
	if err != nil {
		t.Fatalf("failed to open recovered DB: %v", err)
	}
	defer db.Close()

	if got, err := db.GetHash(); err != nil || got != hash {
		t.Fatalf("restored DB failed to reproduce same hash\nwanted %x\n   got %x\n   err %v", hash, got, err)
	}
}
