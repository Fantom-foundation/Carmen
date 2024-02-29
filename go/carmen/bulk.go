package carmen

import (
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

type bulkLoad struct {
	nested state.BulkLoad
}

func (l *bulkLoad) CreateAccount(address Address) {
	l.nested.CreateAccount(common.Address(address))
}

func (l *bulkLoad) SetBalance(address Address, balance *big.Int) {
	l.nested.SetBalance(common.Address(address), balance)
}

func (l *bulkLoad) SetNonce(address Address, nonce uint64) {
	l.nested.SetNonce(common.Address(address), nonce)
}

func (l *bulkLoad) SetState(address Address, key Key, value Value) {
	l.nested.SetState(common.Address(address), common.Key(key), common.Value(value))
}

func (l *bulkLoad) SetCode(address Address, code []byte) {
	l.nested.SetCode(common.Address(address), code)
}

func (l *bulkLoad) Finalize() error {
	// TODO:
	// - signal end of bulk load, allowing other transactions to run
	// - fail if called multiple times
	return l.nested.Close()
}
