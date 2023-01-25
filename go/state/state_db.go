package state

import (
	"fmt"
	"math/big"
	"sort"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// StateDB serves as the public interface definition of a Carmen StateDB.
type StateDB interface {
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
	AbortTransaction()

	BeginBlock()
	EndBlock(number uint64)

	BeginEpoch()
	EndEpoch(number uint64)

	// GetHash obtains a cryptographically unique hash of this state.
	GetHash() common.Hash

	// Flushes committed state to disk.
	Flush() error
	Close() error

	// StartBulkLoad initiates a bulk load operation by-passing internal caching and
	// snapshot, transaction, block, or epoch handling to support faster initialization
	// of StateDB instances. BulkLoads must not run while there open blocks.
	StartBulkLoad() BulkLoad

	// GetMemoryFootprint computes an approximation of the memory used by this state.
	GetMemoryFootprint() *common.MemoryFootprint
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

	// A set of accessed addresses in the current transaction.
	accessedAddresses map[common.Address]bool

	// A set of accessed slots in the current transaction.
	accessedSlots map[slotId]bool

	// A set of slots with current value (possibly) different from the committed value - for needs of committing.
	writtenSlots map[*slotValue]bool

	// A non-transactional local cache of stored storage values.
	storedDataCache *common.Cache[slotId, storedDataCacheValue]

	// A non-transactional reincarnation counter for accounts. This is used to efficiently invalidate data in
	// the storedDataCache upon account deletion. The maintained values are internal information only.
	reincarnation map[common.Address]uint64

	// A list of addresses, which have possibly become empty in the transaction
	emptyCandidates []common.Address
}

// accountState maintains the state of an account during a transaction.
type accountState struct {
	// The committed account state, missing if never fetched.
	original *common.AccountState
	// The current account state visible to the state DB users.
	current common.AccountState
}

type accountClearingState int

const (
	// noClearing is the state of an account not be to cleared (make sure this has the default value 0)
	noClearing accountClearingState = 0
	// pendingClearing is the state of an account that is scheduled for clearing at the end of the current transaction but should still appear like it exists.
	pendingClearing accountClearingState = 1
	// cleared is the state of an account that should appear as it has been cleared.
	cleared accountClearingState = 2
)

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

const StoredDataCacheSize = 1000000 // ~ 100 MiB of memory for this cache.

// storedDataCacheValue maintains the cached version of a value in the store. To
// support the efficient clearing of values cached for accounts being deleted, an
// additional account reincarnation counter is added.
type storedDataCacheValue struct {
	value         common.Value // < the cached version of the value in the store
	reincarnation uint64       // < the reincarnation the cached value blongs to
}

func CreateStateDBUsing(state State) *stateDB {
	return &stateDB{
		state:             state,
		accounts:          map[common.Address]*accountState{},
		balances:          map[common.Address]*balanceValue{},
		nonces:            map[common.Address]*nonceValue{},
		data:              common.NewFastMap[slotId, *slotValue](slotHasher{}),
		storedDataCache:   common.NewCache[slotId, storedDataCacheValue](StoredDataCacheSize),
		reincarnation:     map[common.Address]uint64{},
		codes:             map[common.Address]*codeValue{},
		refund:            0,
		accessedAddresses: map[common.Address]bool{},
		accessedSlots:     map[slotId]bool{},
		writtenSlots:      map[*slotValue]bool{},
		accountsToDelete:  make([]common.Address, 0, 100),
		undo:              make([]func(), 0, 100),
		clearedAccounts:   make(map[common.Address]accountClearingState),
		emptyCandidates:   make([]common.Address, 0, 100),
	}
}

func (s *stateDB) setAccountState(addr common.Address, state common.AccountState) {
	if val, exists := s.accounts[addr]; exists {
		if val.current == state {
			return
		}
		oldState := val.current
		val.current = state
		s.undo = append(s.undo, func() {
			val.current = oldState
		})
	} else {
		val = &accountState{
			original: nil,
			current:  state,
		}
		s.accounts[addr] = val
		s.undo = append(s.undo, func() {
			if val.original != nil {
				val.current = *val.original
			} else {
				delete(s.accounts, addr)
			}
		})
	}
}

func (s *stateDB) getAccountState(addr common.Address) common.AccountState {
	if val, exists := s.accounts[addr]; exists {
		return val.current
	}
	state, err := s.state.GetAccountState(addr)
	if err != nil {
		panic(fmt.Errorf("failed to get account state for address %v: %v", addr, err))
	}
	s.accounts[addr] = &accountState{
		original: &state,
		current:  state,
	}
	return state
}

func (s *stateDB) CreateAccount(addr common.Address) {
	s.setNonceInternal(addr, 0)
	s.setCodeInternal(addr, []byte{})

	exists := s.getAccountState(addr) == common.Exists
	suicided := s.HasSuicided(addr)
	if exists && !suicided {
		return
	}
	s.setAccountState(addr, common.Exists)

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

	// Reset storage in case this account was destroyed in this transaction.
	if suicided {
		// TODO: this full-map iteration may be slow; if so, some index may be required.
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
}

func (s *stateDB) createAccountIfNotExists(addr common.Address) {
	if s.getAccountState(addr) == common.Exists {
		return
	}
	s.setAccountState(addr, common.Exists)

	// Initialize the balance with 0, unless the account existed before.
	// Thus, accounts previously marked as unknown (default) or deleted
	// will get their balance reset. In particular, deleted accounts that
	// are restored will have an empty balance. However, for accounts that
	// already existed before this create call the balance is preserved.
	s.resetBalance(addr)
}

func (s *stateDB) Exist(addr common.Address) bool {
	return s.getAccountState(addr) == common.Exists
}

// Suicide marks the given account as suicided.
// This clears the account balance.
// The account still exist until the state is committed.
func (s *stateDB) Suicide(addr common.Address) bool {
	if !s.Exist(addr) {
		return false
	}
	s.resetBalance(addr)
	deleteListLength := len(s.accountsToDelete)
	s.accountsToDelete = append(s.accountsToDelete, addr)
	s.undo = append(s.undo, func() {
		s.accountsToDelete = s.accountsToDelete[0:deleteListLength]
	})

	// Mark account for clearing to plan its removing on commit and
	// to avoid fetching new data into the cache during the ongoing block.
	oldState := s.clearedAccounts[addr]
	s.clearedAccounts[addr] = pendingClearing
	s.undo = append(s.undo, func() {
		s.clearedAccounts[addr] = oldState
	})

	return true
}

func (s *stateDB) HasSuicided(addr common.Address) bool {
	// An account has suicided within the current transaction if its clearing state is
	// set to 'pending'. If it is 'cleared', it was either in a previous transaction of
	// the current block or the account has already been re-created within the current
	// transaction since it was deleted.
	return s.clearedAccounts[addr] == pendingClearing
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
		panic(fmt.Errorf("failed to load balance for address %v: %v", addr, err))
	}
	res := balance.ToBigInt()
	s.balances[addr] = &balanceValue{
		original: res,
		current:  *res,
	}
	return clone(res) // Do not hand out a pointer to the internal copy!
}

func (s *stateDB) AddBalance(addr common.Address, diff *big.Int) {
	if diff == nil || diff.Sign() == 0 {
		return
	}
	if diff.Sign() < 0 {
		s.SubBalance(addr, diff.Abs(diff))
		return
	}

	s.createAccountIfNotExists(addr)

	oldValue := s.GetBalance(addr)
	newValue := new(big.Int).Add(oldValue, diff)

	s.balances[addr].current = *newValue
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *oldValue
	})
}

func (s *stateDB) SubBalance(addr common.Address, diff *big.Int) {
	if diff == nil || diff.Sign() == 0 {
		return
	}
	if diff.Sign() < 0 {
		s.AddBalance(addr, diff.Abs(diff))
		return
	}

	oldValue := s.GetBalance(addr)
	newValue := new(big.Int).Sub(oldValue, diff)

	s.balances[addr].current = *newValue
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *oldValue
	})
	if newValue.Sign() == 0 {
		s.emptyCandidates = append(s.emptyCandidates, addr)
	}
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
		panic(fmt.Errorf("failed to load nonce for address %v: %v", addr, err))
	}
	res := nonce.ToUint64()
	s.nonces[addr] = &nonceValue{
		original: &res,
		current:  res,
	}
	return res
}

func (s *stateDB) SetNonce(addr common.Address, nonce uint64) {
	s.createAccountIfNotExists(addr)
	s.setNonceInternal(addr, nonce)
	if nonce == 0 {
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
	return s.LoadStoredState(sid, val)
}

func (s *stateDB) LoadStoredState(sid slotId, val *slotValue) common.Value {
	if clearingState, found := s.clearedAccounts[sid.addr]; found && clearingState == cleared {
		// If the account has been cleared in a committed transaction within the current block,
		// the effects are not yet updated in the data base. So it must not be read from the DB
		// before the next block.
		return common.Value{}
	}
	reincarnation := s.reincarnation[sid.addr]
	stored, found := s.storedDataCache.Get(sid)
	if !found {
		var err error
		stored.value, err = s.state.GetStorage(sid.addr, sid.key)
		if err != nil {
			panic(fmt.Errorf("failed to load storage location %v/%v: %v", sid.addr, sid.key, err))
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
	return s.LoadStoredState(sid, nil)
}

func (s *stateDB) SetState(addr common.Address, key common.Key, value common.Value) {
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
			panic(fmt.Sprintf("Unable to obtain code for %v: %v", addr, err))
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
			panic(fmt.Sprintf("Unable to obtain code hash for %v: %v", addr, err))
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
			panic(fmt.Sprintf("Unable to obtain code size for %v: %v", addr, err))
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
		panic(fmt.Sprintf("Refund counter below zero (to be removed: %d > refund: %d)", amount, s.refund))
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

func (s *stateDB) ClearAccessList() {
	if len(s.accessedAddresses) > 0 {
		s.accessedAddresses = make(map[common.Address]bool)
	}
	if len(s.accessedSlots) > 0 {
		s.accessedSlots = make(map[slotId]bool)
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
	_, found := s.accessedSlots[sid]
	if !found {
		s.accessedSlots[sid] = true
		s.undo = append(s.undo, func() {
			delete(s.accessedSlots, sid)
		})
	}
}

func (s *stateDB) IsAddressInAccessList(addr common.Address) bool {
	_, found := s.accessedAddresses[addr]
	return found
}

func (s *stateDB) IsSlotInAccessList(addr common.Address, key common.Key) (addressPresent bool, slotPresent bool) {
	_, found := s.accessedSlots[slotId{addr, key}]
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
		panic(fmt.Sprintf("Invalid snapshot id: %d, allowed range 0 - %d", id, len(s.undo)))
	}
	for len(s.undo) > id {
		s.undo[len(s.undo)-1]()
		s.undo = s.undo[:len(s.undo)-1]
	}
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
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
		}
	}

	// Delete accounts scheduled for deletion.
	if len(s.accountsToDelete) > 0 {
		for _, addr := range s.accountsToDelete {
			// If the account was already cleared because it was recreated, we skip this part.
			if state, found := s.clearedAccounts[addr]; found && state == cleared {
				continue
			}
			// Note: balance was already set to zero during suicide call.
			// Note: storage state is handled through the clearedAccount map
			// the clearing of the data and storedDataCache at various phases
			// of the block processing.
			s.setAccountState(addr, common.Deleted)
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

func (s *stateDB) AbortTransaction() {
	// Revert all effects and reset transaction context.
	s.RevertToSnapshot(0)
	s.resetTransactionContext()
}

func (s *stateDB) BeginBlock() {
	// ignored
}

func (s *stateDB) EndBlock(block uint64) {
	// Write all changes to the store. Note, the store's state hash depends on the insertion order.
	// Thus, the insertion must be performed in a deterministic order. Maps in Go have an undefined
	// order and are deliberately randomized. Thus, updates need to be ordered before being written
	// to the underlying state.
	update := Update{}

	// A list of addresses reused for different purposes in this function.
	addresses := make([]common.Address, 0, max(max(max(len(s.accounts), len(s.balances)), len(s.nonces)), len(s.clearedAccounts)))
	sortAddresses := func() {
		sort.Slice(addresses, func(i, j int) bool { return addresses[i].Compare(&addresses[j]) < 0 })
	}

	// Clear all accounts that have been deleted at some point during this block.
	// This will cause all storage slots of that accounts to be reset before new
	// values may be written in the subsequent updates.
	deletedAccountState := common.Deleted
	clearedAccounts := map[common.Address]bool{}
	for addr, clearingState := range s.clearedAccounts {
		if clearingState == cleared {
			addresses = append(addresses, addr)
			clearedAccounts[addr] = true
			s.accounts[addr].original = &deletedAccountState
		}
	}
	sortAddresses()
	update.AppendDeleteAccounts(addresses)

	// Increment the reincarnation counter of cleared addresses to invalidate cached entries in
	// the stored data cache.
	for addr := range clearedAccounts {
		s.reincarnation[addr] = s.reincarnation[addr] + 1
	}

	// Update account stats in deterministic order in state DB
	addresses = addresses[0:0]
	for addr, value := range s.accounts {
		if value.original == nil || *value.original != value.current {
			if value.current == common.Exists {
				addresses = append(addresses, addr)
			} else if value.current == common.Deleted {
				// Delete was already conducted above.
			} else {
				panic(fmt.Sprintf("Unknown account state: %v", value.current))
			}
		}
	}
	sortAddresses()
	update.AppendCreateAccounts(addresses)

	// Update balances in a deterministic order.
	addresses = addresses[0:0]
	for addr, value := range s.balances {
		if value.original == nil || value.original.Cmp(&value.current) != 0 {
			addresses = append(addresses, addr)
		}
	}
	sortAddresses()
	for _, addr := range addresses {
		newBalance, err := common.ToBalance(&s.balances[addr].current)
		if err != nil {
			panic(fmt.Sprintf("Unable to convert big.Int balance to common.Balance: %v", err))
		}
		update.AppendBalanceUpdate(addr, newBalance)
	}

	// Update nonces in a deterministic order.
	addresses = addresses[0:0]
	for addr, value := range s.nonces {
		if value.original == nil || *value.original != value.current {
			addresses = append(addresses, addr)
		}
	}
	sortAddresses()
	for _, addr := range addresses {
		update.AppendNonceUpdate(addr, common.ToNonce(s.nonces[addr].current))
	}

	// Update storage values in state DB
	slots := make([]slotId, 0, s.data.Size())
	s.data.ForEach(func(slot slotId, value *slotValue) {
		if !value.storedKnown || value.stored != value.current {
			slots = append(slots, slot)
		}
	})
	sort.Slice(slots, func(i, j int) bool { return slots[i].Compare(&slots[j]) < 0 })
	for _, slot := range slots {
		value, _ := s.data.Get(slot)
		update.AppendSlotUpdate(slot.addr, slot.key, value.current)
		s.storedDataCache.Set(slot, storedDataCacheValue{value.current, s.reincarnation[slot.addr]})
	}

	// Update modified codes.
	addresses = addresses[0:0]
	for addr, value := range s.codes {
		if value.dirty {
			addresses = append(addresses, addr)
		}
	}
	sortAddresses()
	for _, addr := range addresses {
		update.AppendCodeUpdate(addr, s.codes[addr].code)
	}

	// Send the update to the state.
	if err := s.state.Apply(block, update); err != nil {
		panic(fmt.Sprintf("Failed to apply update: %v", err))
	}

	// Reset internal state for next block
	s.reset()
}

func (s *stateDB) BeginEpoch() {
	// ignored
}

func (s *stateDB) EndEpoch(uint64) {
	// Simulate the creation of a state snapshot by computing the hash.
	s.GetHash()
}

func (s *stateDB) GetHash() common.Hash {
	hash, err := s.state.GetHash()
	if err != nil {
		panic(fmt.Sprintf("Failed to compute hash: %v", err))
	}
	return hash
}

func (s *stateDB) Flush() error {
	return s.state.Flush()
}

func (s *stateDB) Close() error {
	return s.state.Close()
}

func (s *stateDB) StartBulkLoad() BulkLoad {
	s.EndBlock(0)
	s.storedDataCache.Clear()
	return &bulkLoad{s.state.(directUpdateState)}
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
	mf.AddChild("accessedSlots", common.NewMemoryFootprint(uintptr(len(s.accessedSlots))*(slotIdSize+boolSize)))
	mf.AddChild("writtenSlots", common.NewMemoryFootprint(uintptr(len(s.writtenSlots))*(boolSize+unsafe.Sizeof(&slotValue{}))))
	mf.AddChild("storedDataCache", s.storedDataCache.GetMemoryFootprint(0))
	mf.AddChild("reincarnation", common.NewMemoryFootprint(uintptr(len(s.reincarnation))*(addressSize+unsafe.Sizeof(uint64(0)))))

	return mf
}

func (s *stateDB) resetTransactionContext() {
	s.refund = 0
	s.ClearAccessList()
	s.undo = s.undo[0:0]
	s.emptyCandidates = s.emptyCandidates[0:0]
}

func (s *stateDB) reset() {
	s.accounts = make(map[common.Address]*accountState, len(s.accounts))
	s.balances = make(map[common.Address]*balanceValue, len(s.balances))
	s.nonces = make(map[common.Address]*nonceValue, len(s.nonces))
	s.data.Clear()
	s.clearedAccounts = make(map[common.Address]accountClearingState)
	s.codes = make(map[common.Address]*codeValue)
	s.resetTransactionContext()
}

type bulkLoad struct {
	state directUpdateState
}

func (l *bulkLoad) CreateAccount(addr common.Address) {
	err := l.state.createAccount(addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create account: %v", err))
	}
}

func (l *bulkLoad) SetBalance(addr common.Address, value *big.Int) {
	newBalance, err := common.ToBalance(value)
	if err != nil {
		panic(fmt.Sprintf("Unable to convert big.Int balance to common.Balance: %v", err))
	}
	err = l.state.setBalance(addr, newBalance)
	if err != nil {
		panic(fmt.Sprintf("Failed to set balance: %v", err))
	}
}

func (l *bulkLoad) SetNonce(addr common.Address, value uint64) {
	err := l.state.setNonce(addr, common.ToNonce(value))
	if err != nil {
		panic(fmt.Sprintf("Failed to set nonce: %v", err))
	}
}

func (l *bulkLoad) SetState(addr common.Address, key common.Key, value common.Value) {
	err := l.state.setStorage(addr, key, value)
	if err != nil {
		panic(fmt.Sprintf("Failed to set storage: %v", err))
	}
}
func (l *bulkLoad) SetCode(addr common.Address, code []byte) {
	err := l.state.setCode(addr, code)
	if err != nil {
		panic(fmt.Sprintf("Failed to set code: %v", err))
	}
}

func (l *bulkLoad) Close() error {
	// Flush out all inserted data.
	if err := l.state.Flush(); err != nil {
		return err
	}
	// Compute hash to bring cached hashes up-to-date.
	_, err := l.state.GetHash()
	return err
}
