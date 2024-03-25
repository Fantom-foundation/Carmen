package carmen

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"math/big"
)

type transactionContext struct {
	blockContext *commonContext
	state        state.VmStateDB
}

func (t *transactionContext) CreateAccount(address Address) {
	if t.state != nil {
		t.state.CreateAccount(common.Address(address))
	}
}

func (t *transactionContext) Exist(address Address) bool {
	if t.state != nil {
		return t.state.Exist(common.Address(address))
	}
	return false
}

func (t *transactionContext) Empty(address Address) bool {
	if t.state != nil {
		return t.state.Empty(common.Address(address))
	}
	return false
}

func (t *transactionContext) SelfDestruct(address Address) bool {
	if t.state != nil {
		return t.state.Suicide(common.Address(address))
	}
	return false
}

func (t *transactionContext) HasSelfDestructed(address Address) bool {
	if t.state != nil {
		return t.state.HasSuicided(common.Address(address))
	}
	return false
}

func (t *transactionContext) GetBalance(address Address) *big.Int {
	if t.state != nil {
		return t.state.GetBalance(common.Address(address))
	}
	return nil
}

func (t *transactionContext) AddBalance(address Address, value *big.Int) {
	if t.state != nil {
		t.state.AddBalance(common.Address(address), value)
	}
}

func (t *transactionContext) SubBalance(address Address, value *big.Int) {
	if t.state != nil {
		t.state.SubBalance(common.Address(address), value)
	}
}

func (t *transactionContext) GetNonce(address Address) uint64 {
	if t.state != nil {
		return t.state.GetNonce(common.Address(address))
	}
	return 0
}

func (t *transactionContext) SetNonce(address Address, nonce uint64) {
	if t.state != nil {
		t.state.SetNonce(common.Address(address), nonce)
	}
}

func (t *transactionContext) GetCommittedState(address Address, key Key) Value {
	if t.state != nil {
		return Value(t.state.GetCommittedState(common.Address(address), common.Key(key)))
	}
	return Value{}
}

func (t *transactionContext) GetState(address Address, key Key) Value {
	if t.state != nil {
		return Value(t.state.GetState(common.Address(address), common.Key(key)))
	}
	return Value{}
}

func (t *transactionContext) SetState(address Address, key Key, value Value) {
	if t.state != nil {
		t.state.SetState(common.Address(address), common.Key(key), common.Value(value))
	}
}

func (t *transactionContext) GetCode(address Address) []byte {
	if t.state != nil {
		return t.state.GetCode(common.Address(address))
	}
	return []byte{}
}

func (t *transactionContext) SetCode(address Address, code []byte) {
	if t.state != nil {
		t.state.SetCode(common.Address(address), code)
	}
}

func (t *transactionContext) GetCodeHash(address Address) Hash {
	if t.state != nil {
		return Hash(t.state.GetCodeHash(common.Address(address)))
	}
	return Hash{}
}

func (t *transactionContext) GetCodeSize(address Address) int {
	if t.state != nil {
		return t.state.GetCodeSize(common.Address(address))
	}
	return 0
}

func (t *transactionContext) AddRefund(value uint64) {
	if t.state != nil {
		t.state.AddRefund(value)
	}
}

func (t *transactionContext) SubRefund(value uint64) {
	if t.state != nil {
		t.state.SubRefund(value)
	}
}

func (t *transactionContext) GetRefund() uint64 {
	if t.state != nil {
		return t.state.GetRefund()
	}
	return 0
}

func (t *transactionContext) AddLog(log *Log) {
	if t.state != nil && log != nil {
		topics := make([]common.Hash, 0, len(log.Topics))
		for _, topic := range log.Topics {
			topics = append(topics, common.Hash(topic))
		}
		t.state.AddLog(&common.Log{
			Address: common.Address(log.Address),
			Topics:  topics,
			Data:    log.Data,
			Index:   log.Index,
		})
	}
}

func (t *transactionContext) GetLogs() []*Log {
	if t.state != nil {
		logs := t.state.GetLogs()
		res := make([]*Log, 0, len(logs))
		for _, log := range logs {
			topics := make([]Hash, 0, len(log.Topics))
			for _, topic := range log.Topics {
				topics = append(topics, Hash(topic))
			}

			res = append(res, &Log{
				Address: Address(log.Address),
				Topics:  topics,
				Data:    log.Data,
				Index:   log.Index,
			})
		}
		return res
	}
	return []*Log{}
}

func (t *transactionContext) ClearAccessList() {
	if t.state != nil {
		t.state.ClearAccessList()
	}
}

func (t *transactionContext) AddAddressToAccessList(address Address) {
	if t.state != nil {
		t.state.AddAddressToAccessList(common.Address(address))
	}
}

func (t *transactionContext) AddSlotToAccessList(address Address, key Key) {
	if t.state != nil {
		t.state.AddSlotToAccessList(common.Address(address), common.Key(key))
	}
}

func (t *transactionContext) IsAddressInAccessList(address Address) bool {
	if t.state != nil {
		return t.state.IsAddressInAccessList(common.Address(address))
	}
	return false
}

func (t *transactionContext) IsSlotInAccessList(address Address, key Key) (addressPresent bool, slotPresent bool) {
	if t.state != nil {
		return t.state.IsSlotInAccessList(common.Address(address), common.Key(key))
	}
	return false, false
}

func (t *transactionContext) Snapshot() int {
	if t.state != nil {
		return t.state.Snapshot()
	}
	return -1
}

func (t *transactionContext) RevertToSnapshot(snapshot int) {
	if t.state != nil {
		t.state.RevertToSnapshot(snapshot)
	}
}

func (t *transactionContext) Commit() error {
	if t.state == nil {
		return fmt.Errorf("transaction context is invalid")
	}
	t.state.EndTransaction() // < commits changes
	return t.end()           // < releases resources
}

func (t *transactionContext) Abort() error {
	if t.state == nil {
		return nil
	}
	t.state.AbortTransaction()
	return t.end()
}

func (t *transactionContext) end() error {
	// inform block context of ended transaction
	t.blockContext.releaseTxsContext()
	t.blockContext = nil
	err := t.state.Check()
	t.state = nil
	return err
}
