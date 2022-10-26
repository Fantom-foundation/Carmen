package state

import (
	"fmt"
	"math/big"

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

	// A list of operations undoing modifications applied on the inner state if a snapshot revert needs to be performed.
	undo []func()
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

// slotValue maintains the value of a slot.
type slotValue struct {
	// The committed value in the DB, missing if never fetched.
	original *common.Value
	// The current value as visible to the state DB users.
	current common.Value
}

func CreateStateDB(directory string) (StateDB, error) {
	state, err := NewCppFileBasedState(directory)
	if err != nil {
		return nil, err
	}
	return CreateStateDBUsing(state), nil
}

func CreateStateDBUsing(state State) StateDB {
	return &stateDB{
		state:    state,
		accounts: map[common.Address]*accountState{},
		balances: map[common.Address]*balanceValue{},
		nonces:   map[common.Address]*nonceValue{},
		data:     map[slotId]*slotValue{},
		undo:     make([]func(), 0, 100),
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

func (s *stateDB) EndTransaction() {
	// Write all changes to the store
	for addr, value := range s.accounts {
		if value.original == nil || *value.original != value.current {
			if value.current == common.Exists {
				err := s.state.CreateAccount(addr)
				if err != nil {
					panic(fmt.Sprintf("Failed to create account: %v", err))
				}
			} else if value.current == common.Deleted {
				err := s.state.DeleteAccount(addr)
				if err != nil {
					panic(fmt.Sprintf("Failed to delete account: %v", err))
				}
			} else {
				panic(fmt.Sprintf("Unknown account state: %v", value.current))
			}
		}
	}
	for addr, value := range s.balances {
		if value.original == nil || value.original.Cmp(&value.current) != 0 {
			new_balance, err := common.ToBalance(&value.current)
			if err != nil {
				panic(fmt.Sprintf("Unable to convert big.Int balance to common.Balance: %v", err))
			}
			s.state.SetBalance(addr, new_balance)
		}
	}
	for addr, value := range s.nonces {
		if value.original == nil || *value.original != value.current {
			s.state.SetNonce(addr, common.ToNonce(value.current))
		}
	}
	for slot, value := range s.data {
		if value.original == nil || *value.original != value.current {
			s.state.SetStorage(slot.addr, slot.key, value.current)
		}
	}
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
