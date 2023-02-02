package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
	"unsafe"
)

// ArchiveState represents a historical State. Loads data from the Archive.
type ArchiveState struct {
	archive archive.Archive
	block   uint64
}

func (s *ArchiveState) GetAccountState(address common.Address) (state common.AccountState, err error) {
	exists, err := s.archive.Exists(s.block, address)
	if exists {
		return common.Exists, err
	} else {
		return common.Unknown, err
	}
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
	return common.Hash{}, err
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

func (s *ArchiveState) GetArchiveState(block uint64) (State, error) {
	return &ArchiveState{
		archive: s.archive,
		block:   block,
	}, nil
}
