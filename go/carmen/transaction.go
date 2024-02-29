package carmen

import (
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

type transactionContext struct {
	blockContext *blockContext
	state        state.VmStateDB
}

func (t *transactionContext) CreateAccount(address Address) {
	t.state.CreateAccount(common.Address(address))
}

func (t *transactionContext) Exist(address Address) bool {
	return t.state.Exist(common.Address(address))
}

func (t *transactionContext) Empty(address Address) bool {
	return t.state.Empty(common.Address(address))
}

func (t *transactionContext) SelfDestruct(address Address) bool {
	return t.state.Suicide(common.Address(address))
}

func (t *transactionContext) HasSelfDestructed(address Address) bool {
	return t.state.HasSuicided(common.Address(address))
}

func (t *transactionContext) GetBalance(address Address) *big.Int {
	return t.state.GetBalance(common.Address(address))
}

func (t *transactionContext) AddBalance(address Address, value *big.Int) {
	t.state.AddBalance(common.Address(address), value)
}

func (t *transactionContext) SubBalance(address Address, value *big.Int) {
	t.state.SubBalance(common.Address(address), value)
}

func (t *transactionContext) GetNonce(address Address) uint64 {
	return t.state.GetNonce(common.Address(address))
}

func (t *transactionContext) SetNonce(address Address, nonce uint64) {
	t.state.SetNonce(common.Address(address), nonce)
}

func (t *transactionContext) GetCommittedState(address Address, key Key) Value {
	return Value(t.state.GetCommittedState(common.Address(address), common.Key(key)))
}

func (t *transactionContext) GetState(address Address, key Key) Value {
	return Value(t.state.GetState(common.Address(address), common.Key(key)))
}

func (t *transactionContext) SetState(address Address, key Key, value Value) {
	t.state.SetState(common.Address(address), common.Key(key), common.Value(value))
}

func (t *transactionContext) GetCode(address Address) []byte {
	return t.state.GetCode(common.Address(address))
}

func (t *transactionContext) SetCode(address Address, code []byte) {
	t.state.SetCode(common.Address(address), code)
}

func (t *transactionContext) GetCodeHash(address Address) Hash {
	return Hash(t.state.GetCodeHash(common.Address(address)))
}

func (t *transactionContext) GetCodeSize(address Address) int {
	return t.state.GetCodeSize(common.Address(address))
}

func (t *transactionContext) AddRefund(value uint64) {
	t.state.AddRefund(value)
}

func (t *transactionContext) SubRefund(value uint64) {
	t.state.SubRefund(value)
}

func (t *transactionContext) GetRefund() uint64 {
	return t.state.GetRefund()
}

func (t *transactionContext) AddLog(log *Log) {
	t.state.AddLog((*common.Log)(log))
}

func (t *transactionContext) GetLogs() []*Log {
	logs := t.state.GetLogs()
	res := make([]*Log, len(logs))
	for i := 0; i < len(logs); i++ {
		res[i] = (*Log)(logs[i])
	}
	return res
}

func (t *transactionContext) ClearAccessList() {
	t.state.ClearAccessList()
}

func (t *transactionContext) AddAddressToAccessList(address Address) {
	t.state.AddAddressToAccessList(common.Address(address))
}

func (t *transactionContext) AddSlotToAccessList(address Address, key Key) {
	t.state.AddSlotToAccessList(common.Address(address), common.Key(key))
}

func (t *transactionContext) IsAddressInAccessList(address Address) bool {
	return t.state.IsAddressInAccessList(common.Address(address))
}

func (t *transactionContext) IsSlotInAccessList(address Address, key Key) (addressPresent bool, slotPresent bool) {
	return t.state.IsSlotInAccessList(common.Address(address), common.Key(key))
}

func (t *transactionContext) Snapshot() int {
	return t.state.Snapshot()
}

func (t *transactionContext) RevertToSnapshot(snapshot int) {
	t.state.RevertToSnapshot(snapshot)
}

func (t *transactionContext) Commit() error {
	t.state.EndTransaction() // < commits changes
	return t.Abort()         // < releases resources
}

func (t *transactionContext) Abort() error {
	if t.blockContext == nil {
		return nil
	}
	// inform block context of ended transaction
	t.blockContext.transactionActive.Store(false)
	t.blockContext = nil
	err := t.state.Check()
	t.state = nil
	return err
}
