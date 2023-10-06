package evmstore

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func getEvmStore(t *testing.T) EvmStore {
	evmStore, err := NewEvmStore(Parameters{
		Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("failed to create evmstore; %v", err)
	}
	return evmStore
}

func TestTxPosition(t *testing.T) {
	store := getEvmStore(t)
	defer store.Close()

	txHash := common.Hash{0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33, 0x11, 0x22, 0x33}
	position := TxPosition{
		Block:       12345689,
		Event:       common.Hash{0x58, 0x98, 0x33},
		EventOffset: 58374,
		BlockOffset: 129874,
	}

	storedPosition, err := store.GetTxPosition(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if storedPosition != (TxPosition{}) {
		t.Errorf("loaded position is not empty")
	}

	err = store.SetTxPosition(txHash, position)
	if err != nil {
		t.Fatalf("failed to set tx position; %v", err)
	}

	storedPosition, err = store.GetTxPosition(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if storedPosition != position {
		t.Errorf("loaded position does not match")
	}
}

func TestReceipts(t *testing.T) {
	store := getEvmStore(t)
	defer store.Close()

	blockNum := uint64(87564)
	receipts := []byte{0x87, 0xAC, 0x34}

	storedReceipts, err := store.GetRawReceipts(blockNum)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if storedReceipts != nil {
		t.Errorf("loaded receipts are not nil")
	}

	err = store.SetRawReceipts(blockNum, receipts)
	if err != nil {
		t.Fatalf("failed to set receipts; %v", err)
	}

	storedReceipts, err = store.GetRawReceipts(blockNum)
	if err != nil {
		t.Fatalf("failed to get receipts; %v", err)
	}
	if !bytes.Equal(storedReceipts, receipts) {
		t.Errorf("loaded receipts does not match")
	}
}

func TestTx(t *testing.T) {
	store := getEvmStore(t)
	defer store.Close()

	txHash := common.Hash{0xAB, 0xCD}
	tx := []byte{0x87, 0xAC, 0x34}

	storedTx, err := store.GetTx(txHash)
	if err != nil {
		t.Fatalf("failed to get tx position; %v", err)
	}
	if storedTx != nil {
		t.Errorf("loaded tx is not nil")
	}

	err = store.SetTx(txHash, tx)
	if err != nil {
		t.Fatalf("failed to set tx; %v", err)
	}

	storedTx, err = store.GetTx(txHash)
	if err != nil {
		t.Fatalf("failed to get tx; %v", err)
	}
	if !bytes.Equal(storedTx, tx) {
		t.Errorf("loaded tx does not match")
	}
}
