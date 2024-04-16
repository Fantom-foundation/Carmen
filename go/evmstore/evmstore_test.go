//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package evmstore

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func getEvmStore(t *testing.T, dir string) EvmStore {
	evmStore, err := NewEvmStore(Parameters{
		Directory: dir,
	})
	if err != nil {
		t.Fatalf("failed to create evmstore; %v", err)
	}
	return evmStore
}

func TestTxPosition(t *testing.T) {
	dir := t.TempDir()
	store := getEvmStore(t, dir)
	defer store.Close()

	txHash := common.Hash{0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33}
	want := TxPosition{
		Block:       12345689,
		Event:       common.Hash{0x58, 0x98, 0x33},
		EventOffset: 58374,
		BlockOffset: 129874,
	}

	got, err := store.GetTxPosition(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if got != (TxPosition{}) {
		t.Errorf("loaded position is not empty")
	}

	err = store.SetTxPosition(txHash, want)
	if err != nil {
		t.Fatalf("failed to set tx position; %v", err)
	}

	got, err = store.GetTxPosition(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if got != want {
		t.Errorf("loaded position does not match (got %v want %v)", got, want)
	}

	_ = store.Close()
	store = getEvmStore(t, dir)

	got, err = store.GetTxPosition(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if got != want {
		t.Errorf("loaded position does not match after the store reopening (got %v want %v)", got, want)
	}
}

func TestReceipts(t *testing.T) {
	dir := t.TempDir()
	store := getEvmStore(t, dir)
	defer store.Close()

	blockNum := uint64(87564)
	want := []byte{0x87, 0xAC, 0x34}

	got, err := store.GetRawReceipts(blockNum)
	if err != nil {
		t.Fatalf("failed to get receipts; %v", err)
	}
	if got != nil {
		t.Errorf("loaded receipts are not nil")
	}

	err = store.SetRawReceipts(blockNum, want)
	if err != nil {
		t.Fatalf("failed to set receipts; %v", err)
	}

	got, err = store.GetRawReceipts(blockNum)
	if err != nil {
		t.Fatalf("failed to get receipts; %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("loaded receipts does not match (got %v want %v)", got, want)
	}

	_ = store.Close()
	store = getEvmStore(t, dir)

	got, err = store.GetRawReceipts(blockNum)
	if err != nil {
		t.Fatalf("failed to get receipts; %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("loaded receipts does not match after the store reopening (got %v want %v)", got, want)
	}
}

func TestTx(t *testing.T) {
	dir := t.TempDir()
	store := getEvmStore(t, dir)
	defer store.Close()

	txHash := common.Hash{0xAB, 0xCD}
	want := []byte{0x87, 0xAC, 0x34}

	got, err := store.GetTx(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if got != nil {
		t.Errorf("loaded tx is not nil")
	}

	err = store.SetTx(txHash, want)
	if err != nil {
		t.Fatalf("failed to set tx; %v", err)
	}

	got, err = store.GetTx(txHash)
	if err != nil {
		t.Fatalf("failed to get tx; %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("loaded tx does not match (got %v want %v)", got, want)
	}

	_ = store.Close()
	store = getEvmStore(t, dir)

	got, err = store.GetTx(txHash)
	if err != nil {
		t.Fatalf("failed to get tx; %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("loaded tx does not match after the store reopening (got %v want %v)", got, want)
	}
}
