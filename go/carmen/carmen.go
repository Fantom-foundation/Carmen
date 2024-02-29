package carmen

import (
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
)

//go:generate mockgen -source carmen.go -destination carmen_mock.go -package carmen

// UnsupportedConfiguration is the error returned if unsupported configurations
// are identified. The text may contain further details regarding the
// unsupported feature.
const UnsupportedConfiguration = state.UnsupportedConfiguration

// OpenDatabase opens a database located in the given directory. If the directory is
// empty, a new empty database will be initialized. If the directory does not exist,
// it will be created first.
//
// Opening a Database may fail if the database located in the given directory is already
// opened by a different process, marked as corrupted, or if the given implementation is
// unknown or does not match the implementation of the database in the provided directory.
//
// Any database successfully opened by this function needs to be eventually closed.
func OpenDatabase(directory string, implementation Configuration, properties Properties) (Database, error) {
	return openDatabase(directory, implementation, properties)
}

// TODO: document
type Database interface {
	// --- head state features ---

	GetHeadStateHash() (Hash, error)
	BeginBlock(block uint64) (HeadBlockContext, error)
	AddBlock(block uint64, run func(HeadBlockContext) error) error

	// --- archive features ---

	GetBlockHeight() (int64, error)
	GetHistoricStateHash(block uint64) (Hash, error)
	GetHistoricContext(block uint64) (HistoricBlockContext, error)
	QueryBlock(block uint64, run func(HistoricBlockContext) error) error

	// --- DB features ---

	Flush() error
	Close() error

	// --- Legacy features ---

	// Document:
	// - no concurrent transactions or other bulk-loads
	// - faster load
	// - no consistency check
	// - limits on bulk-load size
	// - should be deprecated
	StartBulkLoad(block uint64) (BulkLoad, error)
}

type BlockContext interface {
	BeginTransaction(number int) (TransactionContext, error)
	RunTransaction(number int, run func(TransactionContext) error) error
}

type HeadBlockContext interface {
	BlockContext

	// Commit writes the changes of this block into the database, progressing the
	// head world state and making it (eventually) visible in the archive state.
	// It also releases resources bound to this context. This context is invalid
	// after this call and should be discarded.
	Commit() error

	// Abort releases this context without committing changes. This context is
	// invalid after this call and should be discarded.
	Abort() error
}

type HistoricBlockContext interface {
	BlockContext

	// Close releases resources held by this context. All modifications made
	// within this context are discarded. This context is invalid after this
	// call and should be discarded. Every historic block context needs to be
	// closed exactly once.
	Close() error
}

type TransactionContext interface {
	// Account management.
	CreateAccount(Address)
	Exist(Address) bool
	Empty(Address) bool

	SelfDestruct(Address) bool
	HasSelfDestructed(Address) bool

	// Balance
	// TODO: use Value as type for balances
	GetBalance(Address) *big.Int
	AddBalance(Address, *big.Int)
	SubBalance(Address, *big.Int)

	// Nonce
	GetNonce(Address) uint64
	SetNonce(Address, uint64)

	// Read and update storage.
	GetCommittedState(Address, Key) Value
	GetState(Address, Key) Value
	SetState(Address, Key, Value)

	// Code management.
	GetCode(Address) []byte
	SetCode(Address, []byte)
	GetCodeHash(Address) Hash
	GetCodeSize(Address) int

	// Refund tracking.
	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	// Log management:
	// AddLog adds a log into the current transaction.
	AddLog(*Log)
	// GetLogs provides logs produced in the current transaction.
	GetLogs() []*Log

	// Access list tracking.
	ClearAccessList()
	AddAddressToAccessList(Address)
	AddSlotToAccessList(Address, Key)
	IsAddressInAccessList(Address) bool
	IsSlotInAccessList(Address, Key) (addressPresent bool, slotPresent bool)

	// Transaction scope management.
	Snapshot() int
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

type BulkLoad interface {
	CreateAccount(Address)
	SetBalance(Address, *big.Int)
	SetNonce(Address, uint64)
	SetState(Address, Key, Value)
	SetCode(Address, []byte)
	Finalize() error
}

type Address common.Address
type Key common.Key
type Value common.Value
type Hash common.Hash
type Log common.Log
