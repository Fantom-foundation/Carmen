// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/common"

	"github.com/Fantom-foundation/Carmen/go/state"
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

	// Obtain exclusive (write) access to the head state.
	c.db.headStateCommitLock.Lock()
	headStateCommitLockReleased := false
	defer func() {
		// release the lock in case EndBlock panics.
		if !headStateCommitLockReleased {
			c.db.headStateCommitLock.Unlock()
		}
	}()

	c.state.EndBlock(uint64(c.block))
	c.db.headStateCommitLock.Unlock()
	headStateCommitLockReleased = true

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
	state        state.NonCommittableStateDB
	archiveState state.State
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

func (c *archiveBlockContext) GetProof(address Address, keys ...Key) (WitnessProof, error) {
	c.lock.Lock()
	if c.db == nil {
		c.lock.Unlock()
		return nil, fmt.Errorf("cannot get proof in invalid block context")
	}
	c.lock.Unlock()

	commonKeys := make([]common.Key, len(keys))
	for i, k := range keys {
		commonKeys[i] = common.Key(k)
	}

	proof, err := c.archiveState.CreateWitnessProof(common.Address(address), commonKeys...)
	if err != nil {
		return nil, err
	}

	return witnessProof{proof}, nil
}

func (c *archiveBlockContext) CreateLiveDBGenesis(out io.Writer) (Hash, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db == nil {
		return Hash{}, fmt.Errorf("cannot create genesis in invalid block context")
	}

	h, err := c.archiveState.CreateLiveDBGenesis(out)
	if err != nil {
		return Hash{}, err
	}
	return Hash(h), nil
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
