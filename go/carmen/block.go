package carmen

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/state"
	"sync"
)

type commonContext struct {
	lock              sync.Mutex
	db                *database
	transactionActive bool
}

func (c *commonContext) releaseTxsContext() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.transactionActive = false
}

type headBlockContext struct {
	commonContext
	block int64
	state state.StateDB
}

func (c *headBlockContext) BeginTransaction() (TransactionContext, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db == nil {
		return nil, fmt.Errorf("cannot start transaction in invalid block context")
	}

	if c.transactionActive == true {
		return nil, fmt.Errorf("another transaction is running")
	}

	c.transactionActive = true
	return &transactionContext{
		blockContext: &c.commonContext,
		state:        c.state,
	}, nil
}

func (c *headBlockContext) RunTransaction(run func(TransactionContext) error) error {
	return runTransaction(c, run)
}

func (c *headBlockContext) Commit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.transactionActive {
		return errTransactionRunning
	}

	if c.db == nil {
		return fmt.Errorf("cannot commit invalid block context")
	}
	c.state.EndBlock(uint64(c.block))
	c.db.moveBlockNumber(c.block)

	return c.end() // < invalidates this context
}

func (c *headBlockContext) Abort() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.state.ResetBlockContext()
	return c.end()
}

func (c *headBlockContext) end() error {
	if c.db == nil {
		return nil
	}
	err := c.state.Check()
	c.db.releaseHeadState()

	c.db = nil

	return err
}

type archiveBlockContext struct {
	commonContext
	state state.NonCommittableStateDB
}

func (c *archiveBlockContext) BeginTransaction() (TransactionContext, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db == nil {
		return nil, fmt.Errorf("cannot start transaction in invalid block context")
	}

	if c.transactionActive == true {
		return nil, fmt.Errorf("another transaction is running")
	}

	c.transactionActive = true
	return &transactionContext{
		blockContext: &c.commonContext,
		state:        c.state,
	}, nil
}

func (c *archiveBlockContext) RunTransaction(run func(TransactionContext) error) error {
	return runTransaction(c, run)
}

func (c *archiveBlockContext) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	if c.db != nil {
		err = c.state.Check()
		c.state.Release()
		c.db.releaseArchiveQuery()
		c.db = nil
	}
	return err
}

func runTransaction(blockContext blockContext, run func(TransactionContext) error) error {
	ctxt, err := blockContext.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	if err := run(ctxt); err != nil {
		return errors.Join(
			fmt.Errorf("failed to process transaction: %w", err),
			ctxt.Abort(),
		)
	}
	return ctxt.Commit()
}