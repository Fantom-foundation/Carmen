// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package gostate

import (
	"errors"
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
	archive      archive.Archive
	block        uint64
	archiveError error
}

func (s *ArchiveState) Exists(address common.Address) (bool, error) {
	if err := s.archiveError; err != nil {
		return false, err
	}

	exists, err := s.archive.Exists(s.block, address)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return exists, s.archiveError
}

func (s *ArchiveState) GetBalance(address common.Address) (common.Balance, error) {
	if err := s.archiveError; err != nil {
		return common.Balance{}, err
	}

	balance, err := s.archive.GetBalance(s.block, address)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return balance, s.archiveError
}

func (s *ArchiveState) GetNonce(address common.Address) (common.Nonce, error) {
	if err := s.archiveError; err != nil {
		return common.Nonce{}, err
	}

	nonce, err := s.archive.GetNonce(s.block, address)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return nonce, s.archiveError
}

func (s *ArchiveState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	if err := s.archiveError; err != nil {
		return common.Value{}, err
	}

	storage, err := s.archive.GetStorage(s.block, address, key)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return storage, s.archiveError
}
func (s *ArchiveState) HasEmptyStorage(common.Address) (bool, error) {
	panic("HasEmptyStorage: not implemented for ArchiveState")
}
func (s *ArchiveState) GetCode(address common.Address) ([]byte, error) {
	if err := s.archiveError; err != nil {
		return []byte{}, err
	}

	code, err := s.archive.GetCode(s.block, address)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return code, s.archiveError
}

func (s *ArchiveState) GetCodeSize(address common.Address) (size int, err error) {
	if err := s.archiveError; err != nil {
		return 0, err
	}

	code, err := s.archive.GetCode(s.block, address)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
		return 0, s.archiveError
	}
	return len(code), s.archiveError
}

func (s *ArchiveState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	if err := s.archiveError; err != nil {
		return common.Hash{}, err
	}

	code, err := s.archive.GetCode(s.block, address)
	if err != nil || len(code) == 0 {
		s.archiveError = errors.Join(s.archiveError, err)
		return emptyCodeHash, s.archiveError
	}
	hasher := sha3.NewLegacyKeccak256()
	codeHash := common.GetHash(hasher, code)
	return codeHash, s.archiveError
}

func (s *ArchiveState) Apply(block uint64, update common.Update) error {
	panic("ArchiveState does not support Apply operation")
}

func (s *ArchiveState) GetHash() (common.Hash, error) {
	if err := s.archiveError; err != nil {
		return common.Hash{}, err
	}

	hash, err := s.archive.GetHash(s.block)
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
	}
	return hash, s.archiveError
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
	if err := s.archiveError; err != nil {
		return nil, err
	}

	height, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
		return nil, errors.Join(fmt.Errorf("failed to get block height from the archive: %w", s.archiveError))
	}
	if empty || block > height {
		return nil, fmt.Errorf("block %d is not present in the archive (empty: %v, height %d)", block, empty, height)
	}
	return &ArchiveState{
		archive:      s.archive,
		block:        block,
		archiveError: s.archiveError,
	}, s.archiveError
}

func (s *ArchiveState) GetArchiveBlockHeight() (uint64, bool, error) {
	if err := s.archiveError; err != nil {
		return 0, false, err
	}

	height, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		s.archiveError = errors.Join(s.archiveError, err)
		return 0, false, errors.Join(fmt.Errorf("failed to get last block in the archive: %w", s.archiveError))
	}
	return height, empty, s.archiveError
}

func (s *ArchiveState) Check() error {
	return s.archiveError
}
