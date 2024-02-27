package state

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// VmStateDB defines the basic operations that can be conducted on a StateDB as
// required by an EVM implementation.
type VmStateDB interface {
	// Account management.
	CreateAccount(common.Address)
	Exist(common.Address) bool
	Empty(common.Address) bool

	Suicide(common.Address) bool
	HasSuicided(common.Address) bool

	// Balance
	GetBalance(common.Address) *big.Int
	AddBalance(common.Address, *big.Int)
	SubBalance(common.Address, *big.Int)

	// Nonce
	GetNonce(common.Address) uint64
	SetNonce(common.Address, uint64)

	// Read and update storage.
	GetCommittedState(common.Address, common.Key) common.Value
	GetState(common.Address, common.Key) common.Value
	SetState(common.Address, common.Key, common.Value)

	// Code management.
	GetCode(common.Address) []byte
	SetCode(common.Address, []byte)
	GetCodeHash(common.Address) common.Hash
	GetCodeSize(common.Address) int

	// Refund tracking.
	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	// Log management:
	// AddLog adds a log into the current transaction.
	AddLog(*common.Log)
	// GetLogs provides logs produced in the current transaction.
	GetLogs() []*common.Log

	// Access list tracking.
	ClearAccessList()
	AddAddressToAccessList(common.Address)
	AddSlotToAccessList(common.Address, common.Key)
	IsAddressInAccessList(common.Address) bool
	IsSlotInAccessList(common.Address, common.Key) (addressPresent bool, slotPresent bool)

	// Transaction scope management.
	Snapshot() int
	RevertToSnapshot(int)

	BeginTransaction()
	EndTransaction()

	// GetTransactionChanges provides a set of accounts and their slots, which have been
	// potentially changed in the current transaction.
	// Must be called before EndTransaction call.
	GetTransactionChanges() map[common.Address][]common.Key

	// Deprecated: not necessary, to be removed
	AbortTransaction()

	// GetHash obtains a cryptographically unique hash of the committed state.
	GetHash() common.Hash
}

// StateDB serves as the public interface definition of a Carmen StateDB.
type StateDB interface {
	VmStateDB

	BeginBlock()
	EndBlock(number uint64)

	BeginEpoch()
	EndEpoch(number uint64)

	// Check checks the state of the DB and reports an error if issues have been
	// encountered. Check should be called periodically to validate all interactions
	// with a StateDB instance. If an error is reported, all operations since the
	// last successful check need to be considered invalid.
	Check() error

	// Flushes committed state to disk.
	Flush() error
	Close() error

	// StartBulkLoad initiates a bulk load operation by-passing internal caching and
	// snapshot, transaction, block, or epoch handling to support faster initialization
	// of StateDB instances. All updates of a bulk-load call are committed to the DB
	// as a single block with the given block number. Bulk-loads may only be started
	// outside the scope of any block.
	StartBulkLoad(block uint64) BulkLoad

	// GetArchiveStateDB provides a historical state view for given block.
	// An error is returned if the archive is not enabled or if it is empty.
	GetArchiveStateDB(block uint64) (NonCommittableStateDB, error)

	// GetArchiveBlockHeight provides the last block height available in the archive.
	// An empty archive is signaled by an extra return value. An error is returned if the
	// archive is not enabled or some other issue has occurred.
	GetArchiveBlockHeight() (height uint64, empty bool, err error)

	// GetMemoryFootprint computes an approximation of the memory used by this state.
	GetMemoryFootprint() *common.MemoryFootprint
}

// NonCommittableStateDB is the public interface offered for views on states that can not
// be permanently modified. The prime example for those are views on historic blocks backed
// by an archive. While volatile transaction internal changes are supported, there is no
// way offered for committing those.
type NonCommittableStateDB interface {
	VmStateDB

	// Copy creates a copy of the StateDB, including all uncommitted changes.
	// Should be used only in-between transactions, as the tx context is not copied.
	// Any change to the copy does not affect the original StateDB, except the state caches.
	// Available for non-committable states only, as a commit to the backing state
	// makes all other StateDBs with the same backing state invalid.
	Copy() NonCommittableStateDB

	// Release should be called whenever this instance is no longer needed to allow
	// held resources to be reused for future requests. After the release, no more
	// operations may be conducted on this StateDB instance.
	Release()
}

// BulkLoad serves as the public interface for loading preset data into the state DB.
type BulkLoad interface {
	CreateAccount(common.Address)
	SetBalance(common.Address, *big.Int)
	SetNonce(common.Address, uint64)
	SetState(common.Address, common.Key, common.Value)
	SetCode(common.Address, []byte)
	Close() error
}

// stateDB is the internal implementation of the StateDB interface.
type stateDB struct {
	// The underlying state data is read/written to.
	state State

	// A transaction local cache for account states to avoid double-fetches and support rollbacks.
	accounts map[common.Address]*accountState

	// A transaction local cache of balances to avoid double-fetches and support rollbacks.
	balances map[common.Address]*balanceValue

	// A transaction local cache of nonces to avoid double-fetches and support rollbacks.
	nonces map[common.Address]*nonceValue

	// A transaction local cache of storage values to avoid double-fetches and support rollbacks.
	data *common.FastMap[slotId, *slotValue]

	// A transaction local cache of contract codes and their properties.
	codes map[common.Address]*codeValue

	// A list of accounts to be deleted at the end of the transaction.
	accountsToDelete []common.Address

	// Tracks the clearing state of individual accounts.
	clearedAccounts map[common.Address]accountClearingState

	// A list of operations undoing modifications applied on the inner state if a snapshot revert needs to be performed.
	undo []func()

	// The refund accumulated in the current transaction.
	refund uint64

	// The list of log messages recorded for the current transaction.
	logs []*common.Log

	// The amount of logs in the current block.
	logsInBlock uint

	// A set of accessed addresses in the current transaction.
	accessedAddresses map[common.Address]bool

	// A set of accessed slots in the current transaction.
	accessedSlots *common.FastMap[slotId, bool]

	// A set of slots with current value (possibly) different from the committed value - for needs of committing.
	writtenSlots map[*slotValue]bool

	// A non-transactional local cache of stored storage values.
	storedDataCache *common.LruCache[slotId, storedDataCacheValue]

	// A non-transactional reincarnation counter for accounts. This is used to efficiently invalidate data in
	// the storedDataCache upon account deletion. The maintained values are internal information only.
	reincarnation map[common.Address]uint64

	// A list of addresses, which have possibly become empty in the transaction
	emptyCandidates []common.Address

	// True, if this state DB is allowed to apply changes to the underlying state, false otherwise.
	canApplyChanges bool

	// A list of errors encountered during DB interactions.
	errors []error
}

type accountLifeCycleState int

// The life-cycle states of an account as seen by the StateDB
//  - unknown     ... the state has not been fetched from the DB; only valid for the original field in the account state
//  - NonExisting ... the account is known to not exist
//  - Exists      ... the account is known to exist
//  - Suicided    ... the account existed during the current transaction, but suicided
//
// The following transitions are allowed:
//
//    Unknown -- Load --> NonExisting
//    Unknown -- Load --> Exists
//
//    NonExisting -- CreateAccount --> Exists
//
//    Exists -- CreateAccount --> Exists
//    Exists -- Suicide --> Suicided
//    Exists -- EndTransaction --> NonExisting    // if account was empty
//
//    Suicided -- CreateAccount --> Exists
//    Suicided -- EndTransaction --> NonExisting
//
// Accounts with the state Suicided can only exist during a transaction. At the end of a
// transaction, Suicided accounts transition automatically into NonExisting accounts.

const (
	kNonExisting accountLifeCycleState = 1
	kExists      accountLifeCycleState = 2
	kSuicided    accountLifeCycleState = 3 // TODO: rename to self-destructed
)

func (s accountLifeCycleState) String() string {
	switch s {
	case kNonExisting:
		return "NonExisting"
	case kExists:
		return "Exists"
	case kSuicided:
		return "Suicided"
	}
	return "?"
}

// accountState maintains the state of an account during a transaction.
type accountState struct {
	// The committed account state, set to kUnknown if never fetched.
	original accountLifeCycleState
	// The current account state visible to the state DB users.
	current accountLifeCycleState
}

type accountClearingState int

const (
	// noClearing is the state of an account not be to cleared (make sure this has the default value 0)
	noClearing accountClearingState = 0
	// pendingClearing is the state of an account that is scheduled for clearing at the end of the current transaction but should still appear like it exists.
	pendingClearing accountClearingState = 1
	// cleared is the state of an account that should appear as it has been cleared.
	cleared accountClearingState = 2
	// same as cleared, but some SetState has been invoked on the account after its cleaning. So the cached state may be tainted.
	clearedAndTainted accountClearingState = 3
)

func (s accountClearingState) String() string {
	switch s {
	case noClearing:
		return "noClearing"
	case pendingClearing:
		return "pendingClearing"
	case cleared:
		return "cleared"
	case clearedAndTainted:
		return "clearedAndTainted"
	}
	return "?"
}

// balanceVale maintains a balance during a transaction.
type balanceValue struct {
	// The committed balance of an account, missing if never fetched.
	original *big.Int
	// The current value of the account balance visible to the state DB users.
	current big.Int
}

// nonceValue maintains a nonce during a transaction.
type nonceValue struct {
	// The committed nonce of an account, missing if never fetched.
	original *uint64
	// The current nonce of an account visible to the state DB users.
	current uint64
}

// slotId identifies a storage location.
type slotId struct {
	addr common.Address
	key  common.Key
}

type slotHasher struct{}

func (h slotHasher) Hash(id slotId) uint16 {
	return uint16(id.addr[19])<<8 | uint16(id.key[31])
}

func (s *slotId) Compare(other *slotId) int {
	c := s.addr.Compare(&other.addr)
	if c < 0 {
		return -1
	}
	if c > 0 {
		return 1
	}
	return s.key.Compare(&other.key)
}

// slotValue maintains the value of a slot.
type slotValue struct {
	// The value in the DB, missing if never fetched.
	stored common.Value
	// The value committed by the last completed transaction.
	committed common.Value
	// The current value as visible to the state DB users.
	current common.Value
	// Whether the stored value is known.
	storedKnown bool
	// Whether the committed value is known.
	committedKnown bool
}

// codeValue maintains the code associated to a given address.
type codeValue struct {
	code      []byte
	size      int
	hash      *common.Hash
	dirty     bool // < set if code has been updated in transaction
	codeValid bool // < set if code is loaded from the state (or written as dirty)
	sizeValid bool // < set if size is loaded from the state (or written as dirty)
}

const defaultStoredDataCacheSize = 1000000 // ~ 100 MiB of memory for this cache.
const nonCommittableStoredDataCacheSize = 100

// storedDataCacheValue maintains the cached version of a value in the store. To
// support the efficient clearing of values cached for accounts being deleted, an
// additional account reincarnation counter is added.
type storedDataCacheValue struct {
	value         common.Value // < the cached version of the value in the store
	reincarnation uint64       // < the reincarnation the cached value belongs to
}

// CreateStateDBUsing creates a StateDB instance wrapping the given state supporting
// all operations including end-of-block operations mutating the underlying state.
// Note: any StateDB instanced becomes invalid if the underlying state is
// modified by any other StateDB instance or through any other direct modification.
func CreateStateDBUsing(state State) StateDB {
	return CreateCustomStateDBUsing(state, defaultStoredDataCacheSize)
}

// CreateCustomStateDBUsing is the same as CreateStateDBUsing but allows the caller to specify
// the capacity of the stored Data cache used in the resulting instance. The default
// cache size used by CreateCustomStateDBUsing may be too large if StateDB instances
// only have a short live time. In such cases, the initialization and destruction of
// the maintained data cache may dominate execution time.
func CreateCustomStateDBUsing(state State, storedDataCacheSize int) StateDB {
	if storedDataCacheSize <= 0 {
		storedDataCacheSize = defaultStoredDataCacheSize
	}
	return createStateDBWith(state, storedDataCacheSize, true)
}

// CreateNonCommittableStateDBUsing creates a read-only StateDB instance wrapping
// the given state supporting all operations specified by the VmStateDB interface.
// Note: any StateDB instanced becomes invalid if the underlying state is
// modified by any other StateDB instance or through any other direct modification.
func CreateNonCommittableStateDBUsing(state State) NonCommittableStateDB {
	// Since StateDB instances are big objects costly to create we reuse those using
	// a pool of objects. However, instances need to be properly reset.
	db := nonCommittableStateDbPool.Get().(*stateDB)
	db.resetState(state)
	return &nonCommittableStateDB{db}
}

func createStateDBWith(state State, storedDataCacheCapacity int, canApplyChanges bool) *stateDB {
	return &stateDB{
		state:             state,
		accounts:          map[common.Address]*accountState{},
		balances:          map[common.Address]*balanceValue{},
		nonces:            map[common.Address]*nonceValue{},
		data:              common.NewFastMap[slotId, *slotValue](slotHasher{}),
		storedDataCache:   common.NewLruCache[slotId, storedDataCacheValue](storedDataCacheCapacity),
		reincarnation:     map[common.Address]uint64{},
		codes:             map[common.Address]*codeValue{},
		refund:            0,
		accessedAddresses: map[common.Address]bool{},
		accessedSlots:     common.NewFastMap[slotId, bool](slotHasher{}),
		writtenSlots:      map[*slotValue]bool{},
		accountsToDelete:  make([]common.Address, 0, 100),
		undo:              make([]func(), 0, 100),
		clearedAccounts:   make(map[common.Address]accountClearingState),
		emptyCandidates:   make([]common.Address, 0, 100),
		canApplyChanges:   canApplyChanges,
	}
}

func (s *stateDB) setAccountState(addr common.Address, state accountLifeCycleState) {
	s.Exist(addr) // < make sure s.accounts[addr] is initialized
	val := s.accounts[addr]
	if val.current == state {
		return
	}
	oldState := val.current
	val.current = state
	s.undo = append(s.undo, func() {
		val.current = oldState
	})
}

func (s *stateDB) Exist(addr common.Address) bool {
	if val, exists := s.accounts[addr]; exists {
		return val.current == kExists || val.current == kSuicided // Suicided accounts still exist till the end of the transaction.
	}
	exists, err := s.state.Exists(addr)
	if err != nil {
		s.errors = append(s.errors, fmt.Errorf("failed to get account state for %v: %w", addr, err))
		return false
	}
	state := kNonExisting
	if exists {
		state = kExists
	}
	s.accounts[addr] = &accountState{
		original: state,
		current:  state,
	}
	return exists
}

func (s *stateDB) CreateAccount(addr common.Address) {
	s.setNonceInternal(addr, 0)
	s.setCodeInternal(addr, []byte{})

	exists := s.Exist(addr)
	s.setAccountState(addr, kExists)

	// Created because touched - will be deleted at the end of the transaction if it stays empty
	s.emptyCandidates = append(s.emptyCandidates, addr)

	// Initialize the balance with 0, unless the account existed before.
	// Thus, accounts previously marked as unknown (default) or deleted
	// will get their balance reset. In particular, deleted accounts that
	// are restored will have an empty balance. However, for accounts that
	// already existed before this create call the balance is preserved.
	if !exists {
		s.resetBalance(addr)
	}

	// Reset storage of the account, to purge any potential former values.
	// TODO: this full-map iteration may be slow; if so, use the account's reincarnation number in the slotId
	s.data.ForEach(func(slot slotId, value *slotValue) {
		if slot.addr == addr {
			// Support rollback of account creation.
			backup := slotValue(*value)
			s.undo = append(s.undo, func() {
				*value = backup
			})

			// Clear cached values.
			value.stored = common.Value{}
			value.storedKnown = true
			value.committed = common.Value{}
			value.committedKnown = true
			value.current = common.Value{}
		}
	})

	// Mark account to be treated like if was already committed.
	oldState := s.clearedAccounts[addr]
	s.clearedAccounts[addr] = cleared
	s.undo = append(s.undo, func() {
		s.clearedAccounts[addr] = oldState
	})
}

func (s *stateDB) createAccountIfNotExists(addr common.Address) bool {
	if s.Exist(addr) {
		return false
	}
	s.setAccountState(addr, kExists)

	// Initialize the balance with 0, unless the account existed before.
	// Thus, accounts previously marked as unknown (default) or deleted
	// will get their balance reset. In particular, deleted accounts that
	// are restored will have an empty balance. However, for accounts that
	// already existed before this create call the balance is preserved.
	s.resetBalance(addr)

	return true
}

// Suicide marks the given account as suicided.
// This clears the account balance.
// The account still exist until the state is committed.
func (s *stateDB) Suicide(addr common.Address) bool {
	if !s.Exist(addr) {
		return false
	}

	s.setAccountState(addr, kSuicided)

	s.resetBalance(addr)
	deleteListLength := len(s.accountsToDelete)
	s.accountsToDelete = append(s.accountsToDelete, addr)
	s.undo = append(s.undo, func() {
		s.accountsToDelete = s.accountsToDelete[0:deleteListLength]
	})

	// Mark account for clearing to plan its removing on commit and
	// to avoid fetching new data into the cache during the ongoing block.
	oldState := s.clearedAccounts[addr]
	if oldState == noClearing {
		s.clearedAccounts[addr] = pendingClearing
		s.undo = append(s.undo, func() {
			s.clearedAccounts[addr] = oldState
		})
	}

	return true
}

func (s *stateDB) HasSuicided(addr common.Address) bool {
	state := s.accounts[addr]
	return state != nil && state.current == kSuicided
}

func (s *stateDB) Empty(addr common.Address) bool {
	// Defined as balance == nonce == code == 0
	return s.GetBalance(addr).Sign() == 0 && s.GetNonce(addr) == 0 && s.GetCodeSize(addr) == 0
}

func clone(val *big.Int) *big.Int {
	res := new(big.Int)
	res.Set(val)
	return res
}

func (s *stateDB) GetBalance(addr common.Address) *big.Int {
	// Check cache first.
	if val, exists := s.balances[addr]; exists {
		return clone(&val.current) // Do not hand out a pointer to the internal copy!
	}
	// Since the value is not present, we need to fetch it from the store.
	balance, err := s.state.GetBalance(addr)
	if err != nil {
		s.errors = append(s.errors, fmt.Errorf("failed to load balance for address %v: %w", addr, err))
		return new(big.Int) // We need to return something that allows the VM to continue.
	}
	res := balance.ToBigInt()
	s.balances[addr] = &balanceValue{
		original: res,
		current:  *res,
	}
	return clone(res) // Do not hand out a pointer to the internal copy!
}

func (s *stateDB) AddBalance(addr common.Address, diff *big.Int) {
	s.createAccountIfNotExists(addr)

	if diff == nil || diff.Sign() == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
		return
	}
	if diff.Sign() < 0 {
		s.SubBalance(addr, diff.Abs(diff))
		return
	}

	oldValue := s.GetBalance(addr)
	newValue := new(big.Int).Add(oldValue, diff)

	s.balances[addr].current = *newValue
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *oldValue
	})
}

func (s *stateDB) SubBalance(addr common.Address, diff *big.Int) {
	s.createAccountIfNotExists(addr)

	if diff == nil || diff.Sign() == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
		return
	}
	if diff.Sign() < 0 {
		s.AddBalance(addr, diff.Abs(diff))
		return
	}

	oldValue := s.GetBalance(addr)
	newValue := new(big.Int).Sub(oldValue, diff)

	if newValue.Sign() == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
	}

	s.balances[addr].current = *newValue
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *oldValue
	})
}

func (s *stateDB) resetBalance(addr common.Address) {
	if val, exists := s.balances[addr]; exists {
		if val.current.Sign() != 0 {
			oldValue := val.current
			val.current = *big.NewInt(0)
			s.undo = append(s.undo, func() {
				val.current = oldValue
			})
		}
	} else {
		s.balances[addr] = &balanceValue{
			original: nil,
			current:  *big.NewInt(0),
		}
		s.undo = append(s.undo, func() {
			delete(s.balances, addr)
		})
	}
}

func (s *stateDB) GetNonce(addr common.Address) uint64 {
	// Check cache first.
	if val, exists := s.nonces[addr]; exists {
		return val.current
	}

	// Since the value is not present, we need to fetch it from the store.
	nonce, err := s.state.GetNonce(addr)
	if err != nil {
		s.errors = append(s.errors, fmt.Errorf("failed to load nonce for address %v: %w", addr, err))
		return 0
	}
	res := nonce.ToUint64()
	s.nonces[addr] = &nonceValue{
		original: &res,
		current:  res,
	}
	return res
}

func (s *stateDB) SetNonce(addr common.Address, nonce uint64) {
	s.setNonceInternal(addr, nonce)
	if s.createAccountIfNotExists(addr) && nonce == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
	}
}

func (s *stateDB) setNonceInternal(addr common.Address, nonce uint64) {
	if val, exists := s.nonces[addr]; exists {
		if val.current != nonce {
			oldValue := val.current
			val.current = nonce
			s.undo = append(s.undo, func() {
				val.current = oldValue
			})
		}
	} else {
		s.nonces[addr] = &nonceValue{
			original: nil,
			current:  nonce,
		}
		s.undo = append(s.undo, func() {
			delete(s.nonces, addr)
		})
	}
}

func (s *stateDB) GetCommittedState(addr common.Address, key common.Key) common.Value {
	// Check cache first.
	sid := slotId{addr, key}
	val, exists := s.data.Get(sid)
	if exists && val.committedKnown {
		return val.committed
	}
	// If the value is not present, fetch it from the store.
	return s.loadStoredState(sid, val)
}

func (s *stateDB) loadStoredState(sid slotId, val *slotValue) common.Value {
	if clearingState, found := s.clearedAccounts[sid.addr]; found && (clearingState == cleared || clearingState == clearedAndTainted) {
		// If the account has been cleared in a committed transaction within the current block,
		// the effects are not yet updated in the data base. So it must not be read from the DB
		// before the next block.
		return common.Value{}
	}
	reincarnation := s.reincarnation[sid.addr]
	var stored storedDataCacheValue
	stored, found := s.storedDataCache.Get(sid)
	if !found {
		var err error
		stored.value, err = s.state.GetStorage(sid.addr, sid.key)
		if err != nil {
			s.errors = append(s.errors, fmt.Errorf("failed to load storage location %v/%v: %w", sid.addr, sid.key, err))
			return common.Value{}
		}
		stored.reincarnation = reincarnation
		s.storedDataCache.Set(sid, stored)
	}
	// If the cached value is out-dated, the current value is zero. If the same slot would
	// have been updated since the clearing, it would have also been updated in the cache.
	if stored.reincarnation < reincarnation {
		stored.value = common.Value{}
	}

	// Remember the stored value for future accesses.
	if val != nil {
		val.committed, val.committedKnown = stored.value, true
		val.stored, val.storedKnown = stored.value, true
	} else {
		s.data.Put(sid, &slotValue{
			stored:         stored.value,
			committed:      stored.value,
			current:        stored.value,
			storedKnown:    true,
			committedKnown: true,
		})
	}
	return stored.value
}

func (s *stateDB) GetState(addr common.Address, key common.Key) common.Value {
	// Check whether the slot is already cached/modified.
	sid := slotId{addr, key}
	if val, exists := s.data.Get(sid); exists {
		return val.current
	}
	// Fetch missing slot values (will also populate the cache).
	return s.loadStoredState(sid, nil)
}

func (s *stateDB) SetState(addr common.Address, key common.Key, value common.Value) {
	if s.createAccountIfNotExists(addr) {
		// The account was implicitly created and may have to be removed at the end of the block.
		s.emptyCandidates = append(s.emptyCandidates, addr)
	}
	sid := slotId{addr, key}
	if entry, exists := s.data.Get(sid); exists {
		if entry.current != value {
			oldValue := entry.current
			entry.current = value
			s.writtenSlots[entry] = true
			s.undo = append(s.undo, func() {
				entry.current = oldValue
			})
		}
	} else {
		entry = &slotValue{current: value}
		s.data.Put(sid, entry)
		s.writtenSlots[entry] = true
		s.undo = append(s.undo, func() {
			entry, _ := s.data.Get(sid)
			if entry.committedKnown {
				entry.current = entry.committed
			} else {
				s.data.Remove(sid)
			}
			delete(s.writtenSlots, entry)
		})
	}
	oldState := s.clearedAccounts[addr]
	if oldState == cleared {
		s.clearedAccounts[addr] = clearedAndTainted
		s.undo = append(s.undo, func() { s.clearedAccounts[addr] = oldState })
	}
}

func (s *stateDB) GetCode(addr common.Address) []byte {
	val, exists := s.codes[addr]
	if !exists {
		val = &codeValue{}
		s.codes[addr] = val
	}
	if !val.codeValid {
		code, err := s.state.GetCode(addr)
		if err != nil {
			s.errors = append(s.errors, fmt.Errorf("unable to obtain code for %v: %w", addr, err))
			return nil
		}
		val.code, val.codeValid = code, true
		val.size, val.sizeValid = len(code), true
	}
	return val.code
}

func (s *stateDB) SetCode(addr common.Address, code []byte) {
	s.createAccountIfNotExists(addr)
	s.setCodeInternal(addr, code)
	if len(code) == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
	}
}

func (s *stateDB) setCodeInternal(addr common.Address, code []byte) {
	val, exists := s.codes[addr]
	if !exists {
		val = &codeValue{dirty: true}
		val.code, val.codeValid = code, true
		val.size, val.sizeValid = len(code), true
		s.codes[addr] = val
		s.undo = append(s.undo, func() {
			delete(s.codes, addr)
		})
	} else {
		old := *val
		val.code, val.codeValid = code, true
		val.size, val.sizeValid = len(code), true
		val.hash = nil
		val.dirty = true
		s.undo = append(s.undo, func() {
			*(s.codes[addr]) = old
		})
	}
}

func (s *stateDB) GetCodeHash(addr common.Address) common.Hash {
	// The hash of the code of a non-existing account is always zero.
	if !s.Exist(addr) {
		return common.Hash{}
	}
	val, exists := s.codes[addr]
	if !exists {
		val = &codeValue{}
		s.codes[addr] = val
	}
	if val.dirty && val.hash == nil {
		// If the code is dirty (=uncommitted) the hash needs to be computed on the fly.
		hash := common.GetKeccak256Hash(val.code)
		val.hash = &hash
	}
	if val.hash == nil {
		// hash not loaded, code not dirty - needs to load the hash from the state
		hash, err := s.state.GetCodeHash(addr)
		if err != nil {
			s.errors = append(s.errors, fmt.Errorf("unable to obtain code hash for %v: %w", addr, err))
			return common.Hash{}
		}
		val.hash = &hash
	}
	return *val.hash
}

func (s *stateDB) GetCodeSize(addr common.Address) int {
	val, exists := s.codes[addr]
	if !exists {
		val = &codeValue{}
		s.codes[addr] = val
	}
	if !val.sizeValid {
		size, err := s.state.GetCodeSize(addr)
		if err != nil {
			s.errors = append(s.errors, fmt.Errorf("unable to obtain code size for %v: %w", addr, err))
			return 0
		}
		val.size, val.sizeValid = size, true
	}
	return val.size
}

func (s *stateDB) AddRefund(amount uint64) {
	old := s.refund
	s.refund += amount
	s.undo = append(s.undo, func() {
		s.refund = old
	})
}
func (s *stateDB) SubRefund(amount uint64) {
	if amount > s.refund {
		s.errors = append(s.errors, fmt.Errorf("failed to lower refund, attempted to removed %d from current refund %d", amount, s.refund))
		return
	}
	old := s.refund
	s.refund -= amount
	s.undo = append(s.undo, func() {
		s.refund = old
	})
}

func (s *stateDB) GetRefund() uint64 {
	return s.refund
}

func (s *stateDB) AddLog(log *common.Log) {
	size := len(s.logs)
	log.Index = s.logsInBlock
	s.logs = append(s.logs, log)
	s.logsInBlock++
	s.undo = append(s.undo, func() {
		s.logs = s.logs[0:size]
		s.logsInBlock--
	})
}

func (s *stateDB) GetLogs() []*common.Log {
	return s.logs
}

func (s *stateDB) ClearAccessList() {
	if len(s.accessedAddresses) > 0 {
		s.accessedAddresses = make(map[common.Address]bool)
	}
	if s.accessedSlots.Size() > 0 {
		s.accessedSlots.Clear()
	}
}

func (s *stateDB) AddAddressToAccessList(addr common.Address) {
	_, found := s.accessedAddresses[addr]
	if !found {
		s.accessedAddresses[addr] = true
		s.undo = append(s.undo, func() {
			delete(s.accessedAddresses, addr)
		})
	}
}

func (s *stateDB) AddSlotToAccessList(addr common.Address, key common.Key) {
	s.AddAddressToAccessList(addr)
	sid := slotId{addr, key}
	_, found := s.accessedSlots.Get(sid)
	if !found {
		s.accessedSlots.Put(sid, true)
		s.undo = append(s.undo, func() {
			s.accessedSlots.Remove(sid)
		})
	}
}

func (s *stateDB) IsAddressInAccessList(addr common.Address) bool {
	_, found := s.accessedAddresses[addr]
	return found
}

func (s *stateDB) IsSlotInAccessList(addr common.Address, key common.Key) (addressPresent bool, slotPresent bool) {
	_, found := s.accessedSlots.Get(slotId{addr, key})
	if found {
		return true, true
	}
	return s.IsAddressInAccessList(addr), false
}

func (s *stateDB) Snapshot() int {
	return len(s.undo)
}

func (s *stateDB) RevertToSnapshot(id int) {
	if id < 0 || len(s.undo) < id {
		s.errors = append(s.errors, fmt.Errorf("failed to revert to invalid snapshot id %d, allowed range 0 - %d", id, len(s.undo)))
		return
	}
	for len(s.undo) > id {
		s.undo[len(s.undo)-1]()
		s.undo = s.undo[:len(s.undo)-1]
	}
}

func (s *stateDB) BeginTransaction() {
	// Ignored
}

func (s *stateDB) EndTransaction() {
	// Updated committed state of storage.
	for value := range s.writtenSlots {
		value.committed, value.committedKnown = value.current, true
	}

	// EIP-161: At the end of the transaction, any account touched by the execution of that transaction
	// which is now empty SHALL instead become non-existent (i.e. deleted).
	for _, addr := range s.emptyCandidates {
		if s.Empty(addr) {
			s.accountsToDelete = append(s.accountsToDelete, addr)
			// Mark the account storage state to be cleaned below.
			s.clearedAccounts[addr] = pendingClearing
		}
	}

	// Delete accounts scheduled for deletion - by suicide or because they are empty.
	if len(s.accountsToDelete) > 0 {
		for _, addr := range s.accountsToDelete {
			// Transition accounts marked by suicide to be deleted.
			if s.HasSuicided(addr) {
				s.setAccountState(addr, kNonExisting)
				s.setCodeInternal(addr, []byte{})
				s.clearedAccounts[addr] = pendingClearing
			}

			// If the account was already cleared because it was recreated, we skip this part.
			if state, found := s.clearedAccounts[addr]; found && (state == cleared || state == clearedAndTainted) {
				continue
			}
			// Note: storage state is handled through the clearedAccount map
			// the clearing of the data and storedDataCache at various phases
			// of the block processing.
			s.setAccountState(addr, kNonExisting)
			s.resetBalance(addr) // reset balance if balance is set after suicide
			s.setNonceInternal(addr, 0)
			s.setCodeInternal(addr, []byte{})

			// Clear cached value states for the targeted account.
			// TODO: this full-map iteration may be slow; if so, some index may be required.
			s.data.ForEach(func(slot slotId, value *slotValue) {
				if slot.addr == addr {
					// Clear cached values.
					value.stored = common.Value{}
					value.storedKnown = true
					value.committed = common.Value{}
					value.committedKnown = true
					value.current = common.Value{}
				}
			})

			// Signal to future fetches in this block that this account should be considered cleared.
			s.clearedAccounts[addr] = cleared
		}

		s.accountsToDelete = s.accountsToDelete[0:0]
	}

	s.writtenSlots = map[*slotValue]bool{}
	// Reset state, in particular seal effects by forgetting undo list.
	s.resetTransactionContext()
}

func (s *stateDB) GetTransactionChanges() map[common.Address][]common.Key {
	changes := make(map[common.Address][]common.Key)
	for addr := range s.accounts {
		changes[addr] = nil
	}
	for addr := range s.balances {
		changes[addr] = nil
	}
	for addr := range s.nonces {
		changes[addr] = nil
	}
	for addr := range s.codes {
		changes[addr] = nil
	}
	s.data.ForEach(func(slot slotId, value *slotValue) {
		if !value.committedKnown || value.committed != value.current {
			changes[slot.addr] = append(changes[slot.addr], slot.key)
		}
	})
	return changes
}

// Deprecated: not necessary, to be removed
func (s *stateDB) AbortTransaction() {
	// Revert all effects and reset transaction context.
	s.RevertToSnapshot(0)
	s.resetTransactionContext()
}

func (s *stateDB) BeginBlock() {
	// ignored
}

func (s *stateDB) EndBlock(block uint64) {
	if !s.canApplyChanges {
		// TODO: return this error directly once the interface got changed
		err := fmt.Errorf("unable to process EndBlock event in StateDB without permission to apply changes")
		s.errors = append(s.errors, err)
		return
	}
	update := common.Update{}

	// Clear all accounts that have been deleted at some point during this block.
	// This will cause all storage slots of that accounts to be reset before new
	// values may be written in the subsequent updates.
	nonExistingAccounts := map[common.Address]bool{}
	for addr, clearingState := range s.clearedAccounts {
		if clearingState == cleared || clearingState == clearedAndTainted {
			if s.accounts[addr].original == kExists {
				// Pretend this account was originally deleted, such that in the loop below
				// it would be detected as re-created in case its new state is Existing.
				s.accounts[addr].original = kNonExisting
				// If the account was not later re-created, we mark it for deletion.
				if s.accounts[addr].current != kExists {
					update.AppendDeleteAccount(addr)
				}
			} else {
				nonExistingAccounts[addr] = true
			}
			// Increment the reincarnation counter of cleared addresses to invalidate
			// cached entries in the stored data cache.
			s.reincarnation[addr] = s.reincarnation[addr] + 1
		}
	}

	// (Re-)create new or resurrected accounts.
	for addr, value := range s.accounts {
		if value.original != value.current {
			if value.current == kExists {
				update.AppendCreateAccount(addr)
				delete(nonExistingAccounts, addr)
			}
		}
	}

	// Update balances.
	for addr, value := range s.balances {
		if _, found := nonExistingAccounts[addr]; found {
			continue
		}
		if value.original == nil || value.original.Cmp(&value.current) != 0 {
			newBalance, err := common.ToBalance(&value.current)
			if err != nil {
				s.errors = append(s.errors, fmt.Errorf("unable to convert big.Int balance %v to common.Balance: %w", &value.current, err))
			} else {
				update.AppendBalanceUpdate(addr, newBalance)
			}
		}
	}

	// Update nonces.
	for addr, value := range s.nonces {
		if _, found := nonExistingAccounts[addr]; found {
			continue
		}
		if value.original == nil || *value.original != value.current {
			update.AppendNonceUpdate(addr, common.ToNonce(s.nonces[addr].current))
		}
	}

	// Update storage values in state DB
	s.data.ForEach(func(slot slotId, value *slotValue) {
		if !value.storedKnown || value.stored != value.current {
			update.AppendSlotUpdate(slot.addr, slot.key, value.current)
			s.storedDataCache.Set(slot, storedDataCacheValue{value.current, s.reincarnation[slot.addr]})
		}
	})

	// Update modified codes.
	for addr, value := range s.codes {
		if _, found := nonExistingAccounts[addr]; found {
			continue
		}
		if value.dirty {
			update.AppendCodeUpdate(addr, s.codes[addr].code)
		}
	}

	// Skip applying changes if there have been any issues.
	if err := s.Check(); err != nil {
		return
	}

	// Send the update to the state.
	if err := s.state.Apply(block, update); err != nil {
		s.errors = append(s.errors, fmt.Errorf("failed to apply update for block %d: %w", block, err))
		return
	}

	// Reset internal state for next block
	s.resetBlockContext()
}

func (s *stateDB) BeginEpoch() {
	// ignored
}

func (s *stateDB) EndEpoch(uint64) {
	// ignored
}

func (s *stateDB) GetHash() common.Hash {
	hash, err := s.state.GetHash()
	if err != nil {
		s.errors = append(s.errors, fmt.Errorf("failed to compute hash: %w", err))
		return common.Hash{}
	}
	return hash
}

func (s *stateDB) Check() error {
	return errors.Join(s.errors...)
}

func (s *stateDB) Flush() error {
	return errors.Join(
		s.Check(),
		s.state.Flush(),
	)
}

func (s *stateDB) Close() error {
	return errors.Join(
		s.Flush(),
		s.state.Close(),
	)
}

func (s *stateDB) StartBulkLoad(block uint64) BulkLoad {
	s.storedDataCache.Clear()
	return &bulkLoad{s.state, common.Update{}, block, nil}
}

func (s *stateDB) GetMemoryFootprint() *common.MemoryFootprint {
	const addressSize = 20
	const keySize = 32
	const hashSize = 32
	const slotIdSize = addressSize + keySize

	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("state", s.state.GetMemoryFootprint())

	// For account-states, balances, and nonces an over-approximation should be sufficient.
	mf.AddChild("accounts", common.NewMemoryFootprint(uintptr(len(s.accounts))*(addressSize+unsafe.Sizeof(accountState{})+unsafe.Sizeof(common.AccountState(0)))))
	mf.AddChild("balances", common.NewMemoryFootprint(uintptr(len(s.balances))*(addressSize+unsafe.Sizeof(balanceValue{}))))
	mf.AddChild("nonces", common.NewMemoryFootprint(uintptr(len(s.nonces))*(addressSize+unsafe.Sizeof(nonceValue{})+8)))
	mf.AddChild("slots", common.NewMemoryFootprint(uintptr(s.data.Size())*(slotIdSize+unsafe.Sizeof(slotValue{}))))

	var sum uintptr = 0
	for _, value := range s.codes {
		sum += addressSize
		if value.hash != nil {
			sum += hashSize
		}
		sum += uintptr(len(value.code))
	}
	mf.AddChild("codes", common.NewMemoryFootprint(sum))

	var boolean bool
	const boolSize = unsafe.Sizeof(boolean)
	mf.AddChild("accessedAddresses", common.NewMemoryFootprint(uintptr(len(s.accessedAddresses))*(addressSize+boolSize)))
	mf.AddChild("accessedSlots", common.NewMemoryFootprint(uintptr(s.accessedSlots.Size())*(slotIdSize+boolSize)))
	mf.AddChild("writtenSlots", common.NewMemoryFootprint(uintptr(len(s.writtenSlots))*(boolSize+unsafe.Sizeof(&slotValue{}))))
	mf.AddChild("storedDataCache", s.storedDataCache.GetMemoryFootprint(0))
	mf.AddChild("reincarnation", common.NewMemoryFootprint(uintptr(len(s.reincarnation))*(addressSize+unsafe.Sizeof(uint64(0)))))
	mf.AddChild("emptyCandidates", common.NewMemoryFootprint(uintptr(len(s.emptyCandidates))*(addressSize)))

	return mf
}

func (s *stateDB) GetArchiveStateDB(block uint64) (NonCommittableStateDB, error) {
	archiveState, err := s.state.GetArchiveState(block)
	if err != nil {
		return nil, err
	}
	return CreateNonCommittableStateDBUsing(archiveState), nil
}

func (s *stateDB) GetArchiveBlockHeight() (uint64, bool, error) {
	return s.state.GetArchiveBlockHeight()
}

func (s *stateDB) resetTransactionContext() {
	s.refund = 0
	s.ClearAccessList()
	s.undo = s.undo[0:0]
	s.emptyCandidates = s.emptyCandidates[0:0]
	s.logs = s.logs[0:0]
}

func (s *stateDB) resetBlockContext() {
	s.accounts = make(map[common.Address]*accountState, len(s.accounts))
	s.balances = make(map[common.Address]*balanceValue, len(s.balances))
	s.nonces = make(map[common.Address]*nonceValue, len(s.nonces))
	s.data.Clear()
	s.clearedAccounts = make(map[common.Address]accountClearingState)
	s.codes = make(map[common.Address]*codeValue)
	s.logsInBlock = 0
	s.resetTransactionContext()
}

func (s *stateDB) resetState(state State) {
	s.resetBlockContext()
	s.storedDataCache.Clear()
	s.reincarnation = map[common.Address]uint64{}
	s.state = state
}

type bulkLoad struct {
	state  State
	update common.Update
	block  uint64
	errs   []error
}

func (l *bulkLoad) CreateAccount(addr common.Address) {
	l.update.AppendCreateAccount(addr)
}

func (l *bulkLoad) SetBalance(addr common.Address, value *big.Int) {
	newBalance, err := common.ToBalance(value)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("unable to convert big.Int balance to common.Balance: %w", err))
		return
	}
	l.update.AppendBalanceUpdate(addr, newBalance)
}

func (l *bulkLoad) SetNonce(addr common.Address, value uint64) {
	l.update.AppendNonceUpdate(addr, common.ToNonce(value))
}

func (l *bulkLoad) SetState(addr common.Address, key common.Key, value common.Value) {
	l.update.AppendSlotUpdate(addr, key, value)
}

func (l *bulkLoad) SetCode(addr common.Address, code []byte) {
	l.update.AppendCodeUpdate(addr, code)
}

func (l *bulkLoad) apply() {
	// Apply the update to the DB as one new block.
	if err := l.update.Normalize(); err != nil {
		l.errs = append(l.errs, err)
		return
	}
	err := l.state.Apply(l.block, l.update)
	l.update = common.Update{}
	if err != nil {
		l.errs = append(l.errs, err)
	}
}

func (l *bulkLoad) Close() error {
	l.apply()
	// Return if errors occurred
	if l.errs != nil {
		return errors.Join(l.errs...)
	}

	// Flush out all inserted data.
	if err := l.state.Flush(); err != nil {
		return err
	}
	// Compute hash to bring cached hashes up-to-date.
	_, err := l.state.GetHash()
	return err
}

var nonCommittableStateDbPool = sync.Pool{
	New: func() any {
		// We use a smaller stored-data cache size to support faster initialization
		// and resetting of instances. NonCommittable instances are expected to live
		// only for the duration of a few transactions.
		return createStateDBWith(nil, nonCommittableStoredDataCacheSize, false)
	},
}

type nonCommittableStateDB struct {
	*stateDB
}

func (db *nonCommittableStateDB) Copy() NonCommittableStateDB {
	cp := nonCommittableStateDbPool.Get().(*stateDB)
	cp.resetState(db.state)

	maps.Copy(cp.accounts, db.accounts)
	maps.Copy(cp.balances, db.balances)
	maps.Copy(cp.nonces, db.nonces)
	db.data.CopyTo(cp.data)
	maps.Copy(cp.codes, db.codes)
	maps.Copy(cp.clearedAccounts, db.clearedAccounts)
	maps.Copy(cp.reincarnation, db.reincarnation)
	cp.logsInBlock = db.logsInBlock
	// we suppose ended tx - we may skip members,
	// which are reset at the end of every tx

	return &nonCommittableStateDB{cp}
}

func (db *nonCommittableStateDB) Release() {
	if db.stateDB != nil {
		nonCommittableStateDbPool.Put(db.stateDB)
		db.stateDB = nil
	}
}
