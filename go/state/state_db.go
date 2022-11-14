package state

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// StateDB serves as the public interface defintion of a Carmen StateDB.
type StateDB interface {
	// Account management.
	CreateAccount(common.Address)
	Exist(common.Address) bool
	Empty(common.Address) bool

	Suicide(common.Address)
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
	EndTransaction()
	AbortTransaction()

	// GetHash obtains a cryptographically unique hash of this state.
	GetHash() common.Hash

	// Flushes committed state to disk.
	Flush() error
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
	data map[slotId]*slotValue

	// A transaction local cache of contract codes and their properties.
	codes map[common.Address]*codeValue

	// A list of operations undoing modifications applied on the inner state if a snapshot revert needs to be performed.
	undo []func()

	// The refund accumulated in the current transaction.
	refund uint64

	// A set of accessed addresses in the current transaction.
	accessed_addresses map[common.Address]int

	// A set of accessed slots in the current transaction.
	accessed_slots map[slotId]int
}

// accountState maintains the state of an account during a transaction.
type accountState struct {
	// The committed account state, missing if never fetched.
	original *common.AccountState
	// The current account state visible to the state DB users.
	current common.AccountState
}

// balanceVale maintains a balance during a transaction.
type balanceValue struct {
	// The commited balance of an account, missing if never fetched.
	original *big.Int
	// The current value of the account balance visibile to the state DB users.
	current big.Int
}

// nonceValue maintains a nonce during a transaction.
type nonceValue struct {
	// The commited nonce of an account, missing if never fetched.
	original *uint64
	// The current nonce of an account visible to the state DB users.
	current uint64
}

// slotId identifies a storage location.
type slotId struct {
	addr common.Address
	key  common.Key
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
	// The committed value in the DB, missing if never fetched.
	original *common.Value
	// The current value as visible to the state DB users.
	current common.Value
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

func CreateStateDB(directory string) (StateDB, error) {
	state, err := NewCppFileBasedState(directory)
	if err != nil {
		return nil, err
	}
	return CreateStateDBUsing(state), nil
}

func CreateStateDBUsing(state State) *stateDB {
	return &stateDB{
		state:              state,
		accounts:           map[common.Address]*accountState{},
		balances:           map[common.Address]*balanceValue{},
		nonces:             map[common.Address]*nonceValue{},
		data:               map[slotId]*slotValue{},
		codes:              map[common.Address]*codeValue{},
		refund:             0,
		accessed_addresses: map[common.Address]int{},
		accessed_slots:     map[slotId]int{},
		undo:               make([]func(), 0, 100),
	}
}

func (s *stateDB) SetAccountState(addr common.Address, state common.AccountState) {
	if val, exists := s.accounts[addr]; exists {
		if val.current == state {
			return
		}
		old_state := val.current
		val.current = state
		s.undo = append(s.undo, func() {
			val.current = old_state
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

func (s *stateDB) GetAccountState(addr common.Address) common.AccountState {
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
	s.SetAccountState(addr, common.Exists)
}

func (s *stateDB) Exist(addr common.Address) bool {
	return s.GetAccountState(addr) == common.Exists
}

func (s *stateDB) Suicide(addr common.Address) {
	s.SetAccountState(addr, common.Deleted)
}

func (s *stateDB) HasSuicided(addr common.Address) bool {
	return s.GetAccountState(addr) == common.Deleted
}

func (s *stateDB) Empty(addr common.Address) bool {
	// Defined as balance == nonce == code == 0
	// TODO: check for empty code once available.
	return s.GetBalance(addr).Sign() == 0 && s.GetNonce(addr) == 0
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

	old_value := s.GetBalance(addr)
	new_value := new(big.Int).Add(old_value, diff)

	s.balances[addr].current = *new_value
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *old_value
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

	old_value := s.GetBalance(addr)
	new_value := new(big.Int).Sub(old_value, diff)

	s.balances[addr].current = *new_value
	s.undo = append(s.undo, func() {
		s.balances[addr].current = *old_value
	})
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
	if val, exists := s.nonces[addr]; exists {
		if val.current != nonce {
			old_value := val.current
			val.current = nonce
			s.undo = append(s.undo, func() {
				val.current = old_value
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
	val, exists := s.data[sid]
	if exists && val.original != nil {
		return *val.original
	}

	// If the value is not present, fetch it from the store.
	original, err := s.state.GetStorage(addr, key)
	if err != nil {
		panic(fmt.Errorf("failed to load storage location %v/%v: %v", addr, key, err))
	}

	// Remember the original value for future accesses.
	if exists {
		val.original = &original
	} else {
		s.data[sid] = &slotValue{
			original: &original,
			current:  original,
		}
	}
	return original
}

func (s *stateDB) GetState(addr common.Address, key common.Key) common.Value {
	// Check whether the slot is already cached/modified.
	if val, exists := s.data[slotId{addr, key}]; exists {
		return val.current
	}
	// Fetch missing slot values (will also populate the cache).
	return s.GetCommittedState(addr, key)
}

func (s *stateDB) SetState(addr common.Address, key common.Key, value common.Value) {
	sid := slotId{addr, key}
	if entry, exists := s.data[sid]; exists {
		if entry.current != value {
			old_value := entry.current
			entry.current = value
			s.undo = append(s.undo, func() {
				entry.current = old_value
			})
		}
	} else {
		s.data[sid] = &slotValue{current: value}
		s.undo = append(s.undo, func() {
			entry := s.data[sid]
			if entry.original != nil {
				entry.current = *entry.original
			} else {
				delete(s.data, sid)
			}
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
	if len(s.accessed_addresses) > 0 {
		s.accessed_addresses = make(map[common.Address]int)
	}
	if len(s.accessed_slots) > 0 {
		s.accessed_slots = make(map[slotId]int)
	}
}

func (s *stateDB) AddAddressToAccessList(addr common.Address) {
	_, found := s.accessed_addresses[addr]
	if !found {
		s.accessed_addresses[addr] = 0
		s.undo = append(s.undo, func() {
			delete(s.accessed_addresses, addr)
		})
	}
}

func (s *stateDB) AddSlotToAccessList(addr common.Address, key common.Key) {
	s.AddAddressToAccessList(addr)
	sid := slotId{addr, key}
	_, found := s.accessed_slots[sid]
	if !found {
		s.accessed_slots[sid] = 0
		s.undo = append(s.undo, func() {
			delete(s.accessed_slots, sid)
		})
	}
}

func (s *stateDB) IsAddressInAccessList(addr common.Address) bool {
	_, found := s.accessed_addresses[addr]
	return found
}

func (s *stateDB) IsSlotInAccessList(addr common.Address, key common.Key) (addressPresent bool, slotPresent bool) {
	_, found := s.accessed_slots[slotId{addr, key}]
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

func (s *stateDB) EndTransaction() {
	// Write all changes to the store. Note, the store's state hash depends on the insertion order.
	// Thus, the insertion must be performed in a deterministic order. Maps in Go have an undefined
	// order and are deliberately randomized. Thus, updates need to be ordered before being written
	// to the underlying state.

	// Update account stats in deterministic order in state DB
	addresses := make([]common.Address, 0, max(max(len(s.accounts), len(s.balances)), len(s.nonces)))
	sort_addresses := func() {
		sort.Slice(addresses, func(i, j int) bool { return addresses[i].Compare(&addresses[j]) < 0 })
	}
	for addr, value := range s.accounts {
		if value.original == nil || *value.original != value.current {
			if value.current == common.Exists {
				addresses = append(addresses, addr)
			} else if value.current == common.Deleted {
				// We can delete accounts in an arbitrary order since deleting does not
				// introduce new values in the address -> addr_id map.
				err := s.state.DeleteAccount(addr)
				if err != nil {
					panic(fmt.Sprintf("Failed to delete account: %v", err))
				}
			} else {
				panic(fmt.Sprintf("Unknown account state: %v", value.current))
			}
		}
	}
	sort_addresses()
	for _, address := range addresses {
		err := s.state.CreateAccount(address)
		if err != nil {
			panic(fmt.Sprintf("Failed to create account: %v", err))
		}
	}

	// Update balances in a deterministic order.
	addresses = addresses[0:0]
	for addr, value := range s.balances {
		if value.original == nil || value.original.Cmp(&value.current) != 0 {
			addresses = append(addresses, addr)
		}
	}
	sort_addresses()
	for _, addr := range addresses {
		new_balance, err := common.ToBalance(&s.balances[addr].current)
		if err != nil {
			panic(fmt.Sprintf("Unable to convert big.Int balance to common.Balance: %v", err))
		}
		s.state.SetBalance(addr, new_balance)
	}

	// Update nonces in a deterministic order.
	addresses = addresses[0:0]
	for addr, value := range s.nonces {
		if value.original == nil || *value.original != value.current {
			addresses = append(addresses, addr)
		}
	}
	sort_addresses()
	for _, addr := range addresses {
		s.state.SetNonce(addr, common.ToNonce(s.nonces[addr].current))
	}

	// Update storage values in state DB
	slots := make([]slotId, 0, len(s.data))
	for slot, value := range s.data {
		if value.original == nil || *value.original != value.current {
			slots = append(slots, slot)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].Compare(&slots[j]) < 0 })
	for _, slot := range slots {
		s.state.SetStorage(slot.addr, slot.key, s.data[slot].current)
	}

	// Update modified codes.
	addresses = addresses[0:0]
	for addr, value := range s.codes {
		if value.dirty {
			addresses = append(addresses, addr)
		}
	}
	sort_addresses()
	for _, addr := range addresses {
		s.state.SetCode(addr, s.codes[addr].code)
	}

	// Reset internal state for next transaction
	s.ResetTransaction()
}

func (s *stateDB) AbortTransaction() {
	s.ResetTransaction()
}

func (s *stateDB) ResetTransaction() {
	s.accounts = make(map[common.Address]*accountState, len(s.accounts))
	s.balances = make(map[common.Address]*balanceValue, len(s.balances))
	s.nonces = make(map[common.Address]*nonceValue, len(s.nonces))
	s.data = make(map[slotId]*slotValue, len(s.data))
	s.codes = make(map[common.Address]*codeValue)
	s.refund = 0
	s.ClearAccessList()
	s.undo = s.undo[0:0]
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
