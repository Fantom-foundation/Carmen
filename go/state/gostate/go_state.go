package gostate

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"golang.org/x/crypto/sha3"
)

// GoState combines a LiveDB and optional Archive implementation into a common
// Carmen State implementation.
type GoState struct {
	live    state.LiveDB
	archive archive.Archive
	cleanup []func()

	stateError error // collect errors occurred during operation

	// Channels are only present if archive is enabled.
	archiveWriter          chan<- archiveUpdate
	archiveWriterFlushDone <-chan bool
	archiveWriterDone      <-chan bool
	archiveWriterError     <-chan error
}

func newGoState(live state.LiveDB, archive archive.Archive, cleanup []func()) state.State {

	res := &GoState{
		live:    live,
		archive: archive,
		cleanup: cleanup,
	}

	// If there is an archive, start an asynchronous archive writer routine.
	if archive != nil {
		in := make(chan archiveUpdate, 10)
		flush := make(chan bool)
		done := make(chan bool)
		err := make(chan error, 10)

		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			defer close(flush)
			defer close(done)
			// Process all incoming updates, no not stop on errors.
			for update := range in {
				// If there is no update, the state is asking for a flush signal.
				if update.update == nil {
					if issue := res.archive.Flush(); issue != nil {
						err <- issue
					}
					flush <- true
				} else {
					// Otherwise, process the update.
					issue := res.archive.Add(update.block, *update.update, update.updateHints)
					if issue != nil {
						err <- issue
					}
					if update.updateHints != nil {
						update.updateHints.Release()
					}
				}
			}
		}()

		res.archiveWriter = in
		res.archiveWriterDone = done
		res.archiveWriterFlushDone = flush
		res.archiveWriterError = err
	}

	return state.WrapIntoSyncedState(res)
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

type archiveUpdate = struct {
	block       uint64
	update      *common.Update  // nil to signal a flush
	updateHints common.Releaser // an optional field for passing update hints from the LiveDB to the Archive
}

func (s *GoState) Exists(address common.Address) (bool, error) {
	if err := s.stateError; err != nil {
		return false, err
	}

	exist, err := s.live.Exists(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return exist, s.stateError
}

func (s *GoState) GetBalance(address common.Address) (common.Balance, error) {
	if err := s.stateError; err != nil {
		return common.Balance{}, err
	}

	balance, err := s.live.GetBalance(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return balance, s.stateError
}

func (s *GoState) GetNonce(address common.Address) (common.Nonce, error) {
	if err := s.stateError; err != nil {
		return common.Nonce{}, err
	}

	nonce, err := s.live.GetNonce(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return nonce, s.stateError
}

func (s *GoState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	if err := s.stateError; err != nil {
		return common.Value{}, err
	}

	val, err := s.live.GetStorage(address, key)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return val, s.stateError
}

func (s *GoState) GetCode(address common.Address) ([]byte, error) {
	if err := s.stateError; err != nil {
		return []byte{}, err
	}

	code, err := s.live.GetCode(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return code, s.stateError
}

func (s *GoState) GetCodeSize(address common.Address) (int, error) {
	if err := s.stateError; err != nil {
		return 0, err
	}

	size, err := s.live.GetCodeSize(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return size, s.stateError
}

func (s *GoState) GetCodeHash(address common.Address) (common.Hash, error) {
	if err := s.stateError; err != nil {
		return common.Hash{}, err
	}

	h, err := s.live.GetCodeHash(address)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return h, s.stateError
}

func (s *GoState) GetHash() (common.Hash, error) {
	if err := s.stateError; err != nil {
		return common.Hash{}, err
	}

	h, err := s.live.GetHash()
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}
	return h, s.stateError
}

func (s *GoState) Apply(block uint64, update common.Update) error {
	if err := s.stateError; err != nil {
		return err
	}

	// Apply the changes to the LiveDB.
	archiveUpdateHints, err := s.live.Apply(block, update)
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
		return s.stateError
	}

	if s.archive != nil {
		// Send the update to the writer to be processed asynchronously.
		s.archiveWriter <- archiveUpdate{block, &update, archiveUpdateHints}

		// Drain potential errors, but do not wait for them.
		done := false
		for !done {
			select {
			// In case there was an error, process it.
			case err := <-s.archiveWriterError:
				s.stateError = errors.Join(s.stateError, err)
			default:
				// all errors consumed, moving on
				done = true
			}
		}
		if err := s.stateError; err != nil {
			return err
		}
	} else if archiveUpdateHints != nil {
		archiveUpdateHints.Release()
	}
	return nil
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(0)
	mf.AddChild("live", s.live.GetMemoryFootprint())
	if s.archive != nil {
		mf.AddChild("archive", s.archive.GetMemoryFootprint())
	}
	return mf
}

func (s *GoState) Flush() error {
	err := s.live.Flush()
	if err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}

	// Flush the archive.
	if s.archiveWriter != nil {
		// Signal to the archive worker that a flush should be conducted.
		s.archiveWriter <- archiveUpdate{}
		// Wait until the flush was processed.
		<-s.archiveWriterFlushDone
	}

	return s.Check()
}

func (s *GoState) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}

	if err := s.live.Close(); err != nil {
		s.stateError = errors.Join(s.stateError, err)
	}

	// Shut down archive writer background worker.
	if s.archiveWriter != nil {
		// Close archive stream, signaling writer to shut down.
		close(s.archiveWriter)
		// Wait for the shutdown to be complete.
		<-s.archiveWriterDone
		s.archiveWriter = nil
	}

	// Close the archive.
	if s.archive != nil {
		if err := s.archive.Close(); err != nil {
			s.stateError = errors.Join(s.stateError, err)
		}
	}

	if s.cleanup != nil {
		for _, clean := range s.cleanup {
			if clean != nil {
				clean()
			}
		}
	}
	return s.Check()
}

func (s *GoState) GetArchiveState(block uint64) (as state.State, err error) {
	if s.archive == nil {
		return nil, state.NoArchiveError
	}
	if err := s.stateError; err != nil {
		return nil, err
	}
	lastBlock, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		s.stateError = errors.Join(s.stateError, errors.Join(fmt.Errorf("failed to get last block in the archive"), err))
		return nil, s.stateError
	}
	if empty {
		return nil, fmt.Errorf("block %d is not present in the archive (archive is empty)", block)
	}
	if block > lastBlock {
		return nil, fmt.Errorf("block %d is not present in the archive (last block %d)", block, lastBlock)
	}
	return &ArchiveState{
		archive: s.archive,
		block:   block,
	}, nil
}

func (s *GoState) GetArchiveBlockHeight() (uint64, bool, error) {
	if s.archive == nil {
		return 0, false, state.NoArchiveError
	}
	if err := s.stateError; err != nil {
		return 0, false, err
	}
	lastBlock, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		s.stateError = errors.Join(s.stateError, errors.Join(fmt.Errorf("failed to get last block in the archive"), err))
		return 0, false, s.stateError
	}
	return lastBlock, empty, nil
}

func (s *GoState) Check() error {
	// drain errors from archive if present
	// but does not wait
	if s.archive != nil {
		var done bool
		for !done {
			select {
			case err := <-s.archiveWriterError:
				s.stateError = errors.Join(s.stateError, err)
			default:
				done = true
			}
		}
	}
	return s.stateError
}

func (s *GoState) GetProof() (backend.Proof, error) {
	components := s.live.GetSnapshotableComponents()
	if components == nil {
		return nil, backend.ErrSnapshotNotSupported
	}
	proofs := make([]backend.Proof, 0, len(components))
	for _, component := range components {
		proof, err := component.GetProof()
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, proof)
	}
	return backend.GetComposedProof(proofs), nil
}

func (s *GoState) CreateSnapshot() (backend.Snapshot, error) {
	components := s.live.GetSnapshotableComponents()
	if components == nil {
		return nil, backend.ErrSnapshotNotSupported
	}
	snapshots := make([]backend.Snapshot, 0, len(components))
	for _, component := range components {
		snapshot, err := component.CreateSnapshot()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return backend.NewComposedSnapshot(snapshots), nil
}

func (s *GoState) Restore(data backend.SnapshotData) error {
	components := s.live.GetSnapshotableComponents()
	if components == nil {
		return backend.ErrSnapshotNotSupported
	}
	subdata, _, err := backend.SplitCompositeData(data)
	if err != nil {
		return err
	}
	if len(subdata) != len(components) {
		return fmt.Errorf("invalid snapshot data format")
	}
	for i, component := range components {
		if err := component.Restore(subdata[i]); err != nil {
			return err
		}
	}
	return s.live.RunPostRestoreTasks()
}

func (s *GoState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	components := s.live.GetSnapshotableComponents()
	if components == nil {
		return nil, backend.ErrSnapshotNotSupported
	}
	subMetaData, partCounts, err := backend.SplitCompositeMetaData(metadata)
	if err != nil {
		return nil, err
	}
	if len(subMetaData) != len(components) {
		return nil, fmt.Errorf("invalid snapshot data format")
	}

	verifiers := make([]backend.SnapshotVerifier, 0, len(components))
	for i, component := range components {
		verifier, err := component.GetSnapshotVerifier(subMetaData[i])
		if err != nil {
			return nil, err
		}
		verifiers = append(verifiers, verifier)
	}
	return backend.NewComposedSnapshotVerifier(verifiers, partCounts), nil
}
