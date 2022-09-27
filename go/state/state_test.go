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

func TestInMemoryComposition(t *testing.T) {
	var addressIndex index.Index[common.Address, uint32] = indexmem.NewMemory[common.Address](common.AddressSerializer{})
	var keyIndex index.Index[common.Key, uint32] = indexmem.NewMemory[common.Key](common.KeySerializer{})
	var slotIndex index.Index[common.SlotIdx[uint32], uint32] = indexmem.NewMemory[common.SlotIdx[uint32]](common.SlotIdxSerializer32{})
	var noncesStore store.Store[uint32, common.Nonce] = storemem.NewMemory[common.Nonce](common.NonceSerializer{}, common.Nonce{}, PageSize, HashTreeFactor)
	var balancesStore store.Store[uint32, common.Balance] = storemem.NewMemory[common.Balance](common.BalanceSerializer{}, common.Balance{}, PageSize, HashTreeFactor)
	var valuesStore store.Store[uint32, common.Value] = storemem.NewMemory[common.Value](common.ValueSerializer{}, common.Value{}, PageSize, HashTreeFactor)

	New(addressIndex, keyIndex, slotIndex, noncesStore, balancesStore, valuesStore)
}
