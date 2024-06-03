// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	cio "github.com/Fantom-foundation/Carmen/go/database/mpt/io"
)

func TestExport_CanBeInterrupted(t *testing.T) {
	type testFuncs struct {
		export   func(context.Context, string, io.Writer) error
		createDB func(t *testing.T, dir string)
	}
	tests := map[string]testFuncs{
		"archive": {
			export:   cio.ExportArchive,
			createDB: createArchiveTrie,
		},
		"live": {
			export:   cio.Export,
			createDB: createLiveTrie,
		},
	}

	for name, tf := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), name)
			tf.createDB(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			catchInterrupt(ctx, cancel, time.Now())

			success := make(chan any, 1)
			go func() {
				time.Sleep(time.Second)
				err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
				if err != nil {
					t.Error("failed to create a SIGINT signal")
					return
				}
				select {
				case <-success:
					return
				case <-time.After(10 * time.Second):
					t.Error("export was not interrupted")
				}
			}()

			if err := tf.export(ctx, dir, newMockWriter(t)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			close(success)
		})
	}

}

type mockWriter struct {
	nested io.Writer
}

func (m mockWriter) Write(p []byte) (n int, err error) {
	// slow down writting to be able to interrupt the export
	time.Sleep(time.Second)
	return m.nested.Write(p)
}

func newMockWriter(t *testing.T) io.Writer {
	t.Helper()
	return mockWriter{
		nested: bytes.NewBuffer([]byte{}),
	}
}

func createLiveTrie(t *testing.T, dir string) {
	t.Helper()
	// Create a small LiveDB.
	db, err := mpt.OpenGoFileState(dir, mpt.S5LiveConfig, 1024)
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

	_, err = db.GetHash()
	if err != nil {
		t.Fatalf("failed to fetch hash from DB: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}
}

func createArchiveTrie(t *testing.T, dir string) {
	t.Helper()
	archive, err := mpt.OpenArchiveTrie(dir, mpt.S5ArchiveConfig, 1024)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	balance1 := common.Balance{1}
	balance2 := common.Balance{2}
	nonce1 := common.Nonce{1}
	nonce2 := common.Nonce{2}
	code1 := []byte{1, 2, 3}

	err = archive.Add(0, common.Update{
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
	if err = archive.Close(); err != nil {
		t.Fatalf("failed to close the archive: %v", err)
	}
}
