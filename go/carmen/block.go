package carmen

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Fantom-foundation/Carmen/go/state"
)

type blockContext struct {
	transactionActive atomic.Bool
}
type headBlockContext struct {
	blockContext
	db    *database
	block uint64
	state state.StateDB
}

func (c *headBlockContext) BeginTransaction(number int) (TransactionContext, error) {
	// TODO: test that the transaction number is > the last transaction
	if c.db == nil {
		return nil, fmt.Errorf("cannot start transaction in invalid block context")
	}
	if !c.transactionActive.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("cannot start transaction while another is active")
	}
	return &transactionContext{
		blockContext: &c.blockContext,
		state:        c.state,
	}, nil
}

func (c *headBlockContext) RunTransaction(number int, run func(TransactionContext) error) error {
	return runTransaction(c, number, run)
}

func (c *headBlockContext) Commit() error {
	if c.db == nil {
		return fmt.Errorf("cannot commit invalid block context")
	}
	c.state.EndBlock(c.block)
	return c.Abort() // < invalidates this context
}

func (c *headBlockContext) Abort() error {
	if c.db == nil {
		return nil
	}
	err := c.state.Check()
	c.db.headStateInUse.Store(false)
	c.db = nil
	return err
}

type archiveBlockContext struct {
	state state.NonCommittableStateDB
}

func (c *archiveBlockContext) BeginTransaction(number int) (TransactionContext, error) {
	// TODO:
	// - check that the transaction number is valid
	// - check that the context is still valid
	// - check that there is only one active transaction
	return &transactionContext{
		state: c.state,
	}, nil
}

func (c *archiveBlockContext) RunTransaction(number int, run func(TransactionContext) error) error {
	return runTransaction(c, number, run)
}

func (c *archiveBlockContext) Close() error {
	var err error
	if c.state != nil {
		err = c.state.Check()
		c.state.Release()
		c.state = nil
	}
	return err
}

func runTransaction(blockContext BlockContext, number int, run func(TransactionContext) error) error {
	ctxt, err := blockContext.BeginTransaction(number)
	if err != nil {
		return fmt.Errorf("failed to create transaction %d: %w", number, err)
	}
	if err := run(ctxt); err != nil {
		return errors.Join(
			fmt.Errorf("failed to process transaction %d: %w", number, err),
			ctxt.Abort(),
		)
	}
	return ctxt.Commit()
}
