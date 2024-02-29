package carmen

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDatabase_OpenWorksForFreshDirectory(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_OpenFailsForInvalidDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "some_file.dat")
	if err := os.WriteFile(path, []byte("hello"), 0600); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	_, err := OpenDatabase(path, testConfig, nil)
	if err == nil {
		t.Fatalf("expected an error, got nothing")
	}
}

func TestDatabase_OpenFailsForInvalidProperty(t *testing.T) {
	tests := map[string]struct {
		property Property
		value    string
	}{
		"liveCache-not-an-int": {
			property: LiveDBCache,
			value:    "hello",
		},
		"archiveCache-not-an-int": {
			property: ArchiveCache,
			value:    "hello",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			properties := Properties{}
			properties[test.property] = test.value
			_, err := OpenDatabase(t.TempDir(), testConfig, properties)
			if err == nil {
				t.Errorf("expected an error, got nothing")
			}
		})
	}
}

func TestHeadBlockContext_CanCreateSequenceOfBlocks(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	for i := 0; i < 10; i++ {
		block, err := db.BeginBlock(uint64(i))
		if err != nil {
			t.Fatalf("failed to create block %d: %v", i, err)
		}
		if err := block.Abort(); err != nil {
			t.Fatalf("failed to abort block %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_CannotStartMultipleBlocksAtOnce(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	block, err := db.BeginBlock(12)
	if err != nil {
		t.Fatalf("failed to start block: %v", err)
	}

	_, err = db.BeginBlock(14)
	if err == nil {
		t.Fatalf("opening two head blocks at the same time should fail")
	}

	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort head block: %v", err)
	}

	block, err = db.BeginBlock(12)
	if err != nil {
		t.Fatalf("failed to start block: %v", err)
	}

	if err := block.Abort(); err != nil {
		t.Fatalf("failed to abort head block: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestDatabase_BulkLoadProducesBlocks(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), testConfig, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	load, err := db.StartBulkLoad(12)
	if err != nil {
		t.Fatalf("failed to start bulk-load: %v", err)
	}

	load.CreateAccount(Address{1})
	load.SetNonce(Address{1}, 12)
	load.CreateAccount(Address{2})
	load.SetNonce(Address{2}, 14)

	if err := load.Finalize(); err != nil {
		t.Fatalf("failed to finalize bulk load: %v", err)
	}

	err = errors.Join(
		db.QueryBlock(11, func(bc HistoricBlockContext) error {
			return errors.Join(
				bc.RunTransaction(1, func(tc TransactionContext) error {
					if tc.Exist(Address{1}) {
						t.Errorf("account 1 should not exist")
					}
					if tc.Exist(Address{2}) {
						t.Errorf("account 2 should not exist")
					}
					return nil
				}),
			)
		}),
		db.QueryBlock(12, func(bc HistoricBlockContext) error {
			return errors.Join(
				bc.RunTransaction(1, func(tc TransactionContext) error {
					if !tc.Exist(Address{1}) {
						t.Errorf("account 1 should exist")
					}
					if want, got := uint64(12), tc.GetNonce(Address{1}); want != got {
						t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
					}
					if !tc.Exist(Address{2}) {
						t.Errorf("account 2 should exist")
					}
					if want, got := uint64(14), tc.GetNonce(Address{2}); want != got {
						t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
					}
					return nil
				}),
			)
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error during query evaluation: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}
