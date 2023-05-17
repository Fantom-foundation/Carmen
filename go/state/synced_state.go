package state

import (
	"sync"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// syncedState wraps a state implemententation with a lock restricting the
// number of concurrent access to one for the underlying state.
type syncedState struct {
	state directUpdateState
	mu    sync.Mutex
}

// wrapIntoSyncedState wraps the given state into a synchronizied state
// ensuring mutual exclusive access to the underlying state.
func wrapIntoSyncedState(state directUpdateState) directUpdateState {
	if _, ok := state.(*syncedState); ok {
		return state
	}
	return &syncedState{
		state: state,
	}
}

func (s *syncedState) Exists(address common.Address) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Exists(address)
}

func (s *syncedState) GetBalance(address common.Address) (common.Balance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetBalance(address)
}

func (s *syncedState) GetNonce(address common.Address) (common.Nonce, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetNonce(address)
}

func (s *syncedState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetStorage(address, key)
}

func (s *syncedState) GetCode(address common.Address) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetCode(address)
}

func (s *syncedState) GetCodeSize(address common.Address) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetCodeSize(address)
}

func (s *syncedState) GetCodeHash(address common.Address) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetCodeHash(address)
}

func (s *syncedState) Apply(block uint64, update common.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Apply(block, update)
}

func (s *syncedState) GetHash() (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetHash()
}

func (s *syncedState) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Flush()
}

func (s *syncedState) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Close()
}

func (s *syncedState) GetMemoryFootprint() *common.MemoryFootprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetMemoryFootprint()
}

func (s *syncedState) GetArchiveState(block uint64) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetArchiveState(block)
}

func (s *syncedState) GetProof() (backend.Proof, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetProof()
}

func (s *syncedState) CreateSnapshot() (backend.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.CreateSnapshot()
}

func (s *syncedState) Restore(data backend.SnapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Restore(data)
}

func (s *syncedState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.GetSnapshotVerifier(metadata)
}

func (s *syncedState) createAccount(address common.Address) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.createAccount(address)
}

func (s *syncedState) deleteAccount(address common.Address) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.deleteAccount(address)
}

func (s *syncedState) setBalance(address common.Address, balance common.Balance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.setBalance(address, balance)
}

func (s *syncedState) setNonce(address common.Address, nonce common.Nonce) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.setNonce(address, nonce)
}

func (s *syncedState) setStorage(address common.Address, key common.Key, value common.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.setStorage(address, key, value)
}

func (s *syncedState) setCode(address common.Address, code []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.setCode(address, code)
}
