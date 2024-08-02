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
	"context"
	"io"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
	"github.com/Fantom-foundation/Carmen/go/state"
)

//go:generate mockgen -source carmen.go -destination carmen_mock.go -package carmen

// UnsupportedConfiguration is the error returned if unsupported configurations
// are identified. The text may contain further details regarding the
// unsupported feature.
const UnsupportedConfiguration = state.UnsupportedConfiguration

// OpenDatabase opens a database located in the given directory. If the directory is
// empty, a new empty database is initialized. If the directory does not exist,
// it is created first.
//
// Opening a Database may fail if
//  1. the target database is already opened by this or some other process
//  2. the database is marked as corrupted and should not be accessed
//  3. the configuration does not match the database in the given directory
//  4. an IO error prevented the database to be opened
//
// Any database successfully opened by this function must be eventually closed.
func OpenDatabase(directory string, implementation Configuration, properties Properties) (Database, error) {
	return openDatabase(directory, implementation, properties)
}

// Database provides access to the blockchain state.
// It can query historic state referring to existing blocks
// and append new blocks with modified state at the head of the chain.
//
// At any time, only one thread can run a transaction adding a new block.
// Many threads may, however, query the history at the same time.
type Database interface {
	// QueryHeadState provides read-only query access to the current
	// blockchain head's state. All operations within the query are
	// guaranteed to be based on a consistent block state. Multiple
	// queries may be conducted concurrently. However, query operations
	// should be of short duration, since long-running queries may
	// prevent the head state from being updated. If this call produces
	// an error, the data retrieved in the query should be considered
	// invalid.
	QueryHeadState(query func(QueryContext)) error

	// BeginBlock starts a new block context, which is used
	// to access and possibly modify the current world state.
	// The world state is accessed via transactions that can
	// be created using the returned context.
	// This method lends the context to the caller, and the caller
	// is eventually required to return the context by either
	// committing or aborting it.
	// Only one context at a time can be opened, and an error
	// is produced for opening a context before another one was
	// either committed or aborted.
	BeginBlock(block uint64) (HeadBlockContext, error)

	// AddBlock appends a new block to the blockchain.
	// The input callback function accesses a new block context,
	// which allows for modification of the state via one or more transactions.
	// This method commits the provided block context before it terminates,
	// unless the callback function returns an error. If the callback function
	// returns an error, the creation of the block is aborted and callback's
	// error is returned by the AddBlock call. In this case no block is created.
	AddBlock(block uint64, run func(HeadBlockContext) error) error

	// GetArchiveBlockHeight returns the current last block number of the blockchain.
	// This value is available only when the archive is enabled.
	GetArchiveBlockHeight() (int64, error)

	// GetHistoricStateHash returns state root hash for the input block number.
	// This value is available only when the archive is enabled.
	// Deprecated: use QueryHistoricState
	GetHistoricStateHash(block uint64) (Hash, error)

	// QueryHistoricState provides read-only query access to a historic
	// state in the block chain in the range [0 .. GetArchiveBlockHeight()].
	// All operations within the query are guaranteed to be based on a
	// consistent block state. Multiple queries may be conducted concurrently.
	// If this call produces an error, the data retrieved in the query should
	// be considered invalid.
	QueryHistoricState(block uint64, query func(QueryContext)) error

	// GetHistoricContext returns a block context, which accesses
	// the world state history as it was for the input block number.
	// This method lends the context to the caller, and the caller
	// is eventually required to return the context closing it.
	// Many historic contexts can be opened in parallel.
	// This context is available only when the archive is enabled.
	GetHistoricContext(block uint64) (HistoricBlockContext, error)

	// QueryBlock accesses a block context that may query the world state
	// history as it was for the input block number.
	// The context is provided to the caller via the input callback function.
	// The context is released before this function terminates,
	// i.e. the user does not have to close it.
	// This context is available only when the archive is enabled.
	QueryBlock(block uint64, run func(HistoricBlockContext) error) error

	// GetMemoryFootprint returns an approximation of the memory used by the database.
	// It contains footprint of both LiveDB and Archive if present.
	GetMemoryFootprint() MemoryFootprint

	// Flush persists all committed HeadBlockContexts to the database.
	// This method blocks until all changes are persisted.
	// If archive is enabled, this function also waits until
	// all updates to the archive are persisted.
	Flush() error

	// Close flushes and releases this database.
	// No methods of the database should be called
	// after it is closed, a new instance must be
	// created.
	Close() error

	// --- Legacy features ---

	// StartBulkLoad creates a specific context for fast filling of a database.
	// It directly inserts values into the database bypassing all checks.
	// This feature should be used in specific scenarios only where fast population
	// of a database is needed.
	// It cannot replace regular block processing.
	// Only one bulk load can run at a time. Also, no head-block transaction may be active while a BulkLoad operation is in progress.
	StartBulkLoad(block uint64) (BulkLoad, error)
}

// blockContext is an interface that accesses transactions of a block.
// This can be used for applying transactions to a new block, which is eventually added
// to the blockchain, or for querying world state history referred to a block.
type blockContext interface {

	// BeginTransaction opens a new transaction context.
	// It allows querying state information and performing volatile modifications within this block context.
	// This method lends the context to the caller, and the caller
	// is eventually required to return the context by either committing or aborting it.
	// Only one context at a time can be opened, and an error is produced for opening
	// a context before another one was either committed or aborted.
	BeginTransaction() (TransactionContext, error)

	// RunTransaction provides a transaction context.
	// It allows querying state information and performing volatile modifications within this block context.
	// This method begins a new transaction context, provides the context to the caller via the callback,
	// and either commits or aborts the context at the end. The context is aborted if the callback
	// returns error, otherwise it is committed.
	// This method does not require the caller to drive the lifecycle of the transaction context,
	// as the context is opened and closed within this method call.
	RunTransaction(run func(TransactionContext) error) error
}

// HeadBlockContext provides an environment to create a new head-block of the blockchain.
// It can be modified via a set of transactions and eventually committed or aborted.
// If the context is committed, it forms a new block at the head of the blockchain.
// This new block is added to the archive should the archive be enabled.
// If the context is aborted, no change happens.
//
// Only one head block context may be created at the time.
// Attempts to create another context before the previous one was either committed or aborted
// produce an error.
type HeadBlockContext interface {
	blockContext

	// Commit writes the changes of this block into the database, progressing the
	// head world state and making it (eventually) visible in the archive state.
	// It also releases resources bound to this context. This context is invalid
	// after this call and should be discarded.
	Commit() error

	// Abort releases this context without committing changes. This context is
	// invalid after this call and should be discarded.
	Abort() error
}

// HistoricBlockContext provides access to the world state of a block of a blockchain.
// This context allows the caller to open a transaction
// and query state of the blockchain as it was for this particular block withing the history.
// The caller can as well modify the context via a transaction, i.e. simulating
// an alternative history.
// These changes cannot be, however, committed as this context can not be committed, only be closed.
// When the context is closed, all possibly held resources are released, but no change
// to the blockchain happens.
type HistoricBlockContext interface {
	blockContext

	// GetProof creates a witness proof for the given account and keys.
	// Error may be produced when it occurs in the underlying database;
	// otherwise, the proof is returned.
	GetProof(address Address, keys ...Key) (WitnessProof, error)

	// Export writes data LiveDB for given block into out.
	// Data created by exported can be used to sync a fresh
	// LiveDB to a certain block.
	// Cancelling given ctx will gracefully cancel the export.
	// Bear in mind that cancelling the interrupt will result
	// in returning error interrupt.ErrCanceled.
	Export(ctx context.Context, out io.Writer) (Hash, error)

	// Close releases resources held by this context. All modifications made
	// within this context are discarded. This context is invalid after this
	// call and should be discarded. Every historic block context needs to be
	// closed once.
	Close() error
}

// TransactionContext represents a transaction within a block.
// Transactions may manipulate and read the state.
// Both head of the blockchain and historic blocks  can be queried
// and updated via transactions, but only a transaction bound to
// the head block context can eventually update the blockchain.
// A block context can have only one active transaction context
// at a time.
// The caller is required to commit or abort the transaction
// context before another one can be opened.
// Methods of this interface are not thread safe.
type TransactionContext interface {

	// CreateAccount creates a new account with the given address.
	// If the account already exists, its fields are emptied.
	// It means balance is set to zero, nonce is set to zero,
	// associated code is removed, and storage is cleared.
	CreateAccount(Address)

	// CreateContract marks an account with the given address as a contract.
	// It might be preceded by CreateAccount if the account does not exist.
	// This method enables the support of EIP-6780 self-destruct mechanism.
	CreateContract(Address)

	// Exist checks if the account with the given address exists.
	Exist(Address) bool

	// Empty checks if the account with the given address is empty.
	// The account is empty if its balance is zero, nonce is zero,
	// and there is no associated code with this account.
	Empty(Address) bool

	// SelfDestruct invalidates the account with the given address.
	// It clears its balance, and marks the account as destructed.
	SelfDestruct(Address) bool

	// SelfDestruct6780 implements the EIP-6780 self-destruct mechanism.
	// If called in the same transaction scope as the CreateContract method,
	// it will act as SelfDestruct, otherwise it will act as a no-op.
	SelfDestruct6780(Address) bool

	// HasSelfDestructed checks if the account with the given address
	// was destructed.
	HasSelfDestructed(Address) bool

	// GetBalance returns the current balance of an account with
	// the given address.
	GetBalance(Address) Amount

	// AddBalance increases the balance of an account with
	// the given address of the input increment.
	AddBalance(Address, Amount)

	// SubBalance decreases the balance of an account with
	// the given address of the input decrement.
	SubBalance(Address, Amount)

	// GetNonce returns current nonce of an account with
	// the given address.
	GetNonce(Address) uint64

	// SetNonce sets nonce of an account with
	// the given address to the input value.
	SetNonce(Address, uint64)

	// GetCommittedState returns a value of the input key
	// stored in the account with the given address.
	// This method returns a value committed before this
	// transaction.
	GetCommittedState(Address, Key) Value

	// GetState returns a value of the input key
	// stored in the account with the given address.
	// This method returns an ongoing value that could be
	// updated in the current transaction.
	GetState(Address, Key) Value

	// SetState updates a value for the input key
	// stored in the account with the given address.
	SetState(Address, Key, Value)

	// GetTransientState retrieves the value associated with a specific
	// key within the transient state storage at a given address.
	// Transient State is an in-memory storage
	// that gets reset after each transaction.
	GetTransientState(Address, Key) Value

	// SetTransientState sets a value in the transient state
	// storage for a specific key at a given address.
	// Transient State is an in-memory storage
	// that gets reset after each transaction.
	SetTransientState(Address, Key, Value)

	// GetCode returns smart contract byte-code
	// of an account with the given address.
	GetCode(Address) []byte

	// SetCode updates smart contract byte-code
	// of an account with the given address.
	SetCode(Address, []byte)

	// GetCodeHash returns a hash of smart contract
	// byte-code for an account with the given address.
	GetCodeHash(Address) Hash

	// GetCodeSize returns the size of smart contract
	// byte-code for an account with the given address.
	GetCodeSize(Address) int

	// AddRefund increases refund of input amount.
	// It allows for tracking possible refund of
	// the current transaction.
	AddRefund(uint64)

	// SubRefund decreases refund of input amount.
	// It allows for tracking possible refund
	// fo current transaction.
	SubRefund(uint64)

	// GetRefund returns refund accumulated so far.
	// It allows for tracking possible refund
	// fo current transaction.
	GetRefund() uint64

	// AddLog adds a log into the current transaction.
	AddLog(*Log)

	// GetLogs provides logs produced in the current transaction.
	GetLogs() []*Log

	// ClearAccessList empties the list of accounts accessed
	// in this transaction.
	ClearAccessList()

	// AddAddressToAccessList stores in a temporary list
	// that the input account was accessed.
	// This method is used for tracking accounts accessed
	// in the current transaction.
	AddAddressToAccessList(Address)

	// AddSlotToAccessList stores in a temporary list
	// that the input account and its storage was accessed.
	// This method is used for tracking slots accessed
	// in the current transaction.
	AddSlotToAccessList(Address, Key)

	// IsAddressInAccessList checks if the input account
	// was accessed.
	IsAddressInAccessList(Address) bool

	// IsSlotInAccessList checks if the input account
	// and its storage was accessed.
	IsSlotInAccessList(Address, Key) (addressPresent bool, slotPresent bool)

	// Snapshot marks a snapshot of a current transaction state.
	// The snapshot is marked by a number, which is returned.
	Snapshot() int

	// RevertToSnapshot rollbacks current state to the previous snapshot
	// marked by call to Snapshot.
	RevertToSnapshot(int)

	// Commit completes this transaction. Pending changes are considered
	// committed for subsequent transactions in the same block. However,
	// changes are only persisted when committing the enclosing block.
	// A call to Commit also implicitly releases underlying resources,
	// invalidating this instance. The instance should be discarded after
	// this call.
	Commit() error

	// Abort releases underlying resources without committing results. This
	// invalidates this instance, which should be discarded after this call.
	Abort() error
}

// QueryContext is a context provided to query operations for retrieving
// information for a selected state. All operations are thread-safe and
// responses are guaranteed to be derived from a consistent state. Thus,
// access is synchronized regarding potential concurrent updates.
type QueryContext interface {
	// GetBalance returns the current balance of an account with
	// the given address.
	GetBalance(Address) Amount

	// GetNonce returns current nonce of an account with
	// the given address.
	GetNonce(Address) uint64

	// GetState returns a value of the input key
	// stored in the account with the given address.
	// This method returns an ongoing value that could be
	// updated in the current transaction.
	GetState(Address, Key) Value

	// GetCode returns smart contract byte-code
	// of an account with the given address.
	GetCode(Address) []byte

	// GetCodeHash returns a hash of smart contract
	// byte-code for an account with the given address.
	GetCodeHash(Address) Hash

	// GetCodeSize returns the size of smart contract
	// byte-code for an account with the given address.
	GetCodeSize(Address) int

	// GetStateHash get the state root hash for the state queried
	// by this context.
	GetStateHash() Hash
}

// BulkLoad provides a context for fast filling of the database.
// It allows for direct insertion of values into the database bypassing all checks.
// Only one bulk load can run at a time.
// Methods of this interface are not thread safe.
// WARNING: One BulkLoad should not exceed 100k updates. Client should
// break large bulk-loads into smaller ones not exceeding this value.
type BulkLoad interface {

	// CreateAccount creates a new account with the given address.
	// If the account already exists, its fields are emptied.
	// It means balance is set to zero, nonce is set to zero,
	// associated code is removed, and storage is cleared.
	CreateAccount(Address)

	// SetBalance sets the balance of the given account address
	// to the input value.
	SetBalance(Address, Amount)

	// SetNonce sets the nonce of the given account address
	// to the input value.
	SetNonce(Address, uint64)

	// SetState sets the storage slot of the given account
	// address and the input storage key to the input value.
	SetState(Address, Key, Value)

	// SetCode sets the smart contract byte-code of
	// the given account address to the input value.
	SetCode(Address, []byte)

	// Finalize applies so far accumulated bulk load
	// into the database.
	// Furthermore, it invalidates the BulkLoad
	// and releases its hold on the Database.
	Finalize() error
}

// Address is a 20byte account address.
type Address common.Address

// Key is a 32byte storage address.
type Key common.Key

// Value is a 32byte storage value.
type Value common.Value

// Hash is a 32byte hash.
type Hash common.Hash

type Tribool tribool.Tribool

// Log summarizes a log message recorded during the execution of a contract.
type Log struct {
	// -- payload --
	// Address of the contract that generated the event.
	Address Address
	// List of topics the log message should be tagged by.
	Topics []Hash
	// The actual log message.
	Data []byte

	// -- metadata --
	// Index of the log in the block.
	Index uint
}
