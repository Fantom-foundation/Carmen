package state

import (
	"fmt"
	"math/big"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// This interface serves as a placeholder until the Aida interface is ready.
type StateDB interface {
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
}

// The Carmen implementation of the StateDB interface.
type stateDB struct {
	// The underlying state data is read/written to.
	state State

	// A transaction local cache of balances to avoid double-fetches and support rollbacks.
	balances map[common.Address]*balanceValue

	// A transaction local cache of nonces to avoid double-fetches and support rollbacks.
	nonces map[common.Address]*nonceValue

	// A transaction local cache of storage values to avoid double-fetches and support rollbacks.
	data map[slotId]*slotValue

	// A list of operations undoing modifications applied on the inner state if a snapshot revert needs to be performed.
	undo []func()
}

// Maintains a balance during a transaction.
type balanceValue struct {
	// The commited balance of an account, missing if never fetched.
	original *big.Int
	// The current value of the account balance visibile to the state DB users.
	current big.Int
}

// Maintains a nonce during a transaction.
type nonceValue struct {
	// The commited nonce of an account, missing if never fetched.
	original *uint64
	// The current nonce of an account visible to the state DB users.
	current uint64
}

// A slot ID identifies a storage location.
type slotId struct {
	addr common.Address
	key  common.Key
}

// Maintains the value of a slot.
type slotValue struct {
	// The committed value in the DB, missing if never fetched.
	original *common.Value
	// The current value as visible to the state DB users.
	current common.Value
}

func CreateStateDB() (StateDB, error) {
	state, err := NewCppInMemoryState()
	if err != nil {
		return nil, err
	}
	return CreateStateDBUsing(state), nil
}

func CreateStateDBUsing(state State) StateDB {
	return &stateDB{
		state:    state,
		balances: map[common.Address]*balanceValue{},
		nonces:   map[common.Address]*nonceValue{},
		data:     map[slotId]*slotValue{},
		undo:     make([]func(), 0, 100),
	}
}

func clone(val *big.Int) *big.Int {
	res := new(big.Int)
	res.Set(val)
	return res
}

func (s *stateDB) GetBalance(addr common.Address) *big.Int {
	// Check cache first.
	val, exists := s.balances[addr]
	if exists {
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
	val, exists := s.nonces[addr]
	if exists {
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
	val, exists := s.nonces[addr]
	if exists {
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
	sid := slotId{addr, key}
	val, exists := s.data[sid]
	if exists {
		return val.current
	}
	// Fetch missing slot values (will also populate the cache).
	return s.GetCommittedState(addr, key)
}

func (s *stateDB) SetState(addr common.Address, key common.Key, value common.Value) {
	sid := slotId{addr, key}
	entry, exists := s.data[sid]
	if exists {
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
	s.data = map[slotId]*slotValue{}
	s.undo = s.undo[0:0]
}
