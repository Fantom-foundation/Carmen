package gostate

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"golang.org/x/crypto/sha3"
)

// ArchiveState represents a historical State. Loads data from the Archive.
type ArchiveState struct {
	archive archive.Archive
	block   uint64
}

func (s *ArchiveState) Exists(address common.Address) (bool, error) {
	return s.archive.Exists(s.block, address)
}

func (s *ArchiveState) GetBalance(address common.Address) (balance common.Balance, err error) {
	return s.archive.GetBalance(s.block, address)
}

func (s *ArchiveState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	return s.archive.GetNonce(s.block, address)
}

func (s *ArchiveState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.archive.GetStorage(s.block, address, key)
}

func (s *ArchiveState) GetCode(address common.Address) (value []byte, err error) {
	return s.archive.GetCode(s.block, address)
}

func (s *ArchiveState) GetCodeSize(address common.Address) (size int, err error) {
	code, err := s.archive.GetCode(s.block, address)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (s *ArchiveState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	code, err := s.archive.GetCode(s.block, address)
	if err != nil || len(code) == 0 {
		return emptyCodeHash, err
	}
	hasher := sha3.NewLegacyKeccak256()
	codeHash := common.GetHash(hasher, code)
	return codeHash, nil
}

func (s *ArchiveState) Apply(block uint64, update common.Update) error {
	panic("ArchiveState does not support Apply operation")
}

func (s *ArchiveState) GetHash() (hash common.Hash, err error) {
	return s.archive.GetHash(s.block)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *ArchiveState) GetMemoryFootprint() *common.MemoryFootprint {
	return common.NewMemoryFootprint(unsafe.Sizeof(*s))
}

func (s *ArchiveState) Flush() error {
	panic("ArchiveState does not support Flush operation")
}

func (s *ArchiveState) Close() error {
	// no-op in ArchiveState
	return nil
}

func (s *ArchiveState) GetProof() (backend.Proof, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *ArchiveState) CreateSnapshot() (backend.Snapshot, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *ArchiveState) Restore(data backend.SnapshotData) error {
	return backend.ErrSnapshotNotSupported
}

func (s *ArchiveState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	return nil, backend.ErrSnapshotNotSupported
}

func (s *ArchiveState) GetArchiveState(block uint64) (state.State, error) {
	height, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		return nil, fmt.Errorf("failed to get block height from the archive; %s", err)
	}
	if empty || block > height {
		return nil, fmt.Errorf("block %d is not present in the archive (height %d)", block, height)
	}
	return &ArchiveState{
		archive: s.archive,
		block:   block,
	}, nil
}

func (s *ArchiveState) GetArchiveBlockHeight() (uint64, bool, error) {
	height, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		return 0, false, fmt.Errorf("failed to get last block in the archive; %s", err)
	}
	return height, empty, nil
}

func (s *ArchiveState) Check() error {
	// TODO implement - collect errors throughout other method calls and return here
	return nil
}
