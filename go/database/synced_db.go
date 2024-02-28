package database

import (
	"sync"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// syncedDatabase wraps a database implementation with a lock restricting the
// number of concurrent access to one for the underlying database.
type syncedDatabase struct {
	db Database
	mu sync.Mutex
}

// WrapIntoSyncedDatabase wraps the given database into a synchronized DB
// ensuring mutual exclusive access to the underlying database.
func WrapIntoSyncedDatabase(db Database) Database {
	if _, ok := db.(*syncedDatabase); ok {
		return db
	}
	return &syncedDatabase{
		db: db,
	}
}

// UnsafeUnwrapSyncedDatabase obtains a reference to a potentially nested
// synchronized database from the given database.
// Note: extracting the database from within a synchronized DB breaks
// the synchronization guarantees for the synced DB. Concurrent
// operations on the given database and the resulting database are no longer
// mutual exclusive.
func UnsafeUnwrapSyncedDatabase(db Database) Database {
	if synced, ok := db.(*syncedDatabase); ok {
		return synced.db
	}
	return db
}

func (s *syncedDatabase) Exists(address common.Address) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Exists(address)
}

func (s *syncedDatabase) GetBalance(address common.Address) (common.Balance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetBalance(address)
}

func (s *syncedDatabase) GetNonce(address common.Address) (common.Nonce, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetNonce(address)
}

func (s *syncedDatabase) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetStorage(address, key)
}

func (s *syncedDatabase) GetCode(address common.Address) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetCode(address)
}

func (s *syncedDatabase) GetCodeSize(address common.Address) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetCodeSize(address)
}

func (s *syncedDatabase) GetCodeHash(address common.Address) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetCodeHash(address)
}

func (s *syncedDatabase) Apply(block uint64, update common.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Apply(block, update)
}

func (s *syncedDatabase) GetHash() (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetHash()
}

func (s *syncedDatabase) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Flush()
}

func (s *syncedDatabase) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *syncedDatabase) GetMemoryFootprint() *common.MemoryFootprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetMemoryFootprint()
}

func (s *syncedDatabase) GetArchiveState(block uint64) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetArchiveState(block)
}

func (s *syncedDatabase) GetArchiveBlockHeight() (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetArchiveBlockHeight()
}

func (s *syncedDatabase) GetProof() (backend.Proof, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetProof()
}

func (s *syncedDatabase) CreateSnapshot() (backend.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.CreateSnapshot()
}

func (s *syncedDatabase) Restore(data backend.SnapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Restore(data)
}

func (s *syncedDatabase) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.GetSnapshotVerifier(metadata)
}
