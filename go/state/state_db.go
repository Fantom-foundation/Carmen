package state

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// This interface serves as a placeholder until the Aida interface is ready.
type StateDB interface {
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

	// A transaction local cache of storage values to avoid double-fetches and support shadow writes.
	data map[slotId]*slotValue

	// A list of operations undoing modifications applied on the inner state if a snapshot revert needs to be performed.
	undo []func()
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
		state: state,
		data:  map[slotId]*slotValue{},
		undo:  make([]func(), 0, 100),
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
