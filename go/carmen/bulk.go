package carmen

import (
	"fmt"
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

type bulkLoad struct {
	nested state.BulkLoad
	db     *database
	block  int64
}

func (l *bulkLoad) CreateAccount(address Address) {
	if l.db != nil {
		l.nested.CreateAccount(common.Address(address))
	}
}

func (l *bulkLoad) SetBalance(address Address, balance *big.Int) {
	if l.db != nil {
		l.nested.SetBalance(common.Address(address), balance)
	}
}

func (l *bulkLoad) SetNonce(address Address, nonce uint64) {
	if l.db != nil {
		l.nested.SetNonce(common.Address(address), nonce)
	}
}

func (l *bulkLoad) SetState(address Address, key Key, value Value) {
	if l.db != nil {
		l.nested.SetState(common.Address(address), common.Key(key), common.Value(value))
	}
}

func (l *bulkLoad) SetCode(address Address, code []byte) {
	if l.db != nil {
		l.nested.SetCode(common.Address(address), code)
	}
}

func (l *bulkLoad) Finalize() error {
	if l.db == nil {
		return fmt.Errorf("bulk load already closed")
	}

	err := l.nested.Close()
	l.db.moveBlockAndReleaseHead(l.block)
	l.db = nil
	return err
}
