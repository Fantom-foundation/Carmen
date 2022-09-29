package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	indexmem "github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	storemem "github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

const (
	HashTreeFactor = 3
	PageSize       = 32
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}

	key1 = common.Key{0x01}
	key2 = common.Key{0x02}
	key3 = common.Key{0x03}

	val1 = common.Value{0x01}
	val2 = common.Value{0x02}
	val3 = common.Value{0x03}
)

func TestInMemoryComposition(t *testing.T) {
	NewInMemoryComposition()
}

func TestMissingKeys(t *testing.T) {
	state := NewInMemoryComposition()

	balance, err := state.GetBalance(address1)
	if (err != nil || balance != common.Balance{}) {
		t.Errorf("Balance must be empty. It is: %s, err: %s", balance, err)
	}
	nonce, err := state.GetNonce(address1)
	if (err != nil || nonce != common.Nonce{}) {
		t.Errorf("Nonce must be empty. It is: %s, err: %s", nonce, err)
	}
	value, err := state.GetStorage(address1, key1)
	if (err != nil || value != common.Value{}) {
		t.Errorf("Value must be empty. It is: %s, err: %s", value, err)
	}
}

func TestBasicOperations(t *testing.T) {
	state := NewInMemoryComposition()

	// fill-in values
	if err := state.SetNonce(address1, common.Nonce{123}); err != nil {
		t.Errorf("Error: %s", err)
	}
	if err := state.SetBalance(address2, common.Balance{45}); err != nil {
		t.Errorf("Error: %s", err)
	}
	if err := state.SetStorage(address3, key1, common.Value{67}); err != nil {
		t.Errorf("Error: %s", err)
	}

	// fetch values
	if val, err := state.GetNonce(address1); (err != nil || val != common.Nonce{123}) {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
	if val, err := state.GetBalance(address2); (err != nil || val != common.Balance{45}) {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
	if val, err := state.GetStorage(address3, key1); (err != nil || val != common.Value{67}) {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}

	// fetch wrong combinations
	if val, err := state.GetStorage(address1, key1); (err != nil || val != common.Value{}) {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
}

func TestMoreInserts(t *testing.T) {
	state := NewInMemoryComposition()

	// insert more combinations, so we do not have only zero-indexes everywhere
	_ = state.SetStorage(address1, key1, val1)
	_ = state.SetStorage(address1, key2, val2)
	_ = state.SetStorage(address1, key3, val3)

	_ = state.SetStorage(address2, key1, val1)
	_ = state.SetStorage(address2, key2, val2)
	_ = state.SetStorage(address2, key3, val3)

	_ = state.SetStorage(address3, key1, val1)
	_ = state.SetStorage(address3, key2, val2)
	_ = state.SetStorage(address3, key3, val3)

	if val, err := state.GetStorage(address1, key3); err != nil || val != val3 {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
	if val, err := state.GetStorage(address2, key1); err != nil || val != val1 {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
	if val, err := state.GetStorage(address3, key2); err != nil || val != val2 {
		t.Errorf("Invalid value or error returned: Val: %s, Err: %s", val, err)
	}
}

func NewInMemoryComposition() State {
	var addressIndex index.Index[common.Address, uint32] = indexmem.NewMemory[common.Address](common.AddressSerializer{})
	var keyIndex index.Index[common.Key, uint32] = indexmem.NewMemory[common.Key](common.KeySerializer{})
	var slotIndex index.Index[common.SlotIdx[uint32], uint32] = indexmem.NewMemory[common.SlotIdx[uint32]](common.SlotIdxSerializer32{})
	var noncesStore store.Store[uint32, common.Nonce] = storemem.NewMemory[common.Nonce](common.NonceSerializer{}, common.Nonce{}, PageSize, HashTreeFactor)
	var balancesStore store.Store[uint32, common.Balance] = storemem.NewMemory[common.Balance](common.BalanceSerializer{}, common.Balance{}, PageSize, HashTreeFactor)
	var valuesStore store.Store[uint32, common.Value] = storemem.NewMemory[common.Value](common.ValueSerializer{}, common.Value{}, PageSize, HashTreeFactor)

	return New(addressIndex, keyIndex, slotIndex, noncesStore, balancesStore, valuesStore)
}
