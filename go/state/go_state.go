package state

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/trace"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

const (
	HashTreeFactor = 32
)

// GoState manages dependencies to other interfaces to build this service
type GoState struct {
	GoSchema
	cleanup []func()
	archive archive.Archive

	// Interaction with asynchronous hash worker.
	hashWorker          chan<- stateUpdate
	hashWorkerFlushDone <-chan bool
	hashWorkerDone      <-chan bool
	hashWorkerError     <-chan error
	hashUpdateMutex     sync.Mutex

	// Channels are only present if archive is enabled.
	archiveWriter          chan<- archiveUpdate
	archiveWriterFlushDone <-chan bool
	archiveWriterDone      <-chan bool
	archiveWriterError     <-chan error
}

type GoSchema interface {
	CreateAccount(address common.Address) (err error)
	Exists(address common.Address) (bool, error)
	DeleteAccount(address common.Address) error
	GetBalance(address common.Address) (balance common.Balance, err error)
	SetBalance(address common.Address, balance common.Balance) (err error)
	GetNonce(address common.Address) (nonce common.Nonce, err error)
	SetNonce(address common.Address, nonce common.Nonce) (err error)
	GetStorage(address common.Address, key common.Key) (value common.Value, err error)
	SetStorage(address common.Address, key common.Key, value common.Value) error
	GetCode(address common.Address) (value []byte, err error)
	GetCodeSize(address common.Address) (size int, err error)
	SetCode(address common.Address, code []byte) (err error)
	GetCodeHash(address common.Address) (hash common.Hash, err error)
	GetHash() (hash common.Hash, err error)
	FinishBlock() (archiveUpdateHints any, err error)
	Flush() error
	Close() error
	common.MemoryFootprintProvider

	// getSnapshotableComponents lists all components required to back-up or restore
	// for snapshotting this schema. Returns nil if snapshotting is not supported.
	getSnapshotableComponents() []backend.Snapshotable

	// Called after synching to a new state, requisting the schema to update cached
	// values or tables not covered by the snapshot synchronization.
	runPostRestoreTasks() error
}

func newGoState(schema GoSchema, archive archive.Archive, cleanup []func()) State {

	hashWorkerJobs := make(chan stateUpdate, 10)
	hashWorkerFlushDone := make(chan bool)
	hashWorkerDone := make(chan bool)
	hashWorkerError := make(chan error, 10)

	res := &GoState{
		GoSchema:            schema,
		cleanup:             cleanup,
		archive:             archive,
		hashWorker:          hashWorkerJobs,
		hashWorkerFlushDone: hashWorkerFlushDone,
		hashWorkerDone:      hashWorkerDone,
		hashWorkerError:     hashWorkerError,
	}

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(hashWorkerFlushDone)
		defer close(hashWorkerError)
		defer close(hashWorkerDone)
		for update := range hashWorkerJobs {
			if update.update == nil {
				hashWorkerFlushDone <- true
				continue
			} else {
				_, task := trace.NewTask(update.context, "hash")
				err := res.updateHash(update)
				task.End()
				update.task.End()
				if err != nil {
					hashWorkerError <- err
				}
			}
			res.hashUpdateMutex.Unlock()
		}
	}()

	return WrapIntoSyncedState(res)
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func (s *GoState) Apply(block uint64, update common.Update) error {

	ctxt, task := trace.NewTask(context.Background(), "apply")

	region := trace.StartRegion(context.Background(), "apply")
	defer region.End()

	trace.Log(context.Background(), "apply", "begin")

	// Wait for hash worker to be ready. This lock is unlocked by
	// the hash worker whenever the update scheduled at the end of
	// this function is completed.
	s.hashUpdateMutex.Lock()

	trace.Log(context.Background(), "apply", "update")

	// Check for errors encountered during the last hashing.
	if err := s.collectHashWorkerErrors(); err != nil {
		return err
	}

	// Apply the update to the state.
	_, updateTask := trace.NewTask(ctxt, "update")
	err := applyUpdate(s, update)
	updateTask.End()
	if err != nil {
		return err
	}

	trace.Log(context.Background(), "apply", "signal")

	// Signal hash worker to start hashing the new block.
	s.hashWorker <- stateUpdate{context: ctxt, task: task, block: block, update: &update}

	trace.Log(context.Background(), "apply", "end")

	return nil
}

func (s *GoState) GetHash() (hash common.Hash, err error) {
	s.hashUpdateMutex.Lock()
	defer s.hashUpdateMutex.Unlock()
	return s.GoSchema.GetHash()
}

type stateUpdate = struct {
	context context.Context
	task    *trace.Task
	block   uint64
	update  *common.Update // nil to signal a flush
}

type archiveUpdate = struct {
	stateUpdate
	updateHints any // an optional field for passing update hints from the LiveDB to the Archive
}

func (s *GoState) updateHash(update stateUpdate) error {

	// Finish the block by refreshing the hash.
	archiveUpdateHints, err := s.FinishBlock()
	if err != nil {
		return err
	}

	if s.archive != nil {
		// If the writer is not yet active, start it.
		if s.archiveWriter == nil {
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
						flush <- true
					} else {
						// Otherwise, process the update.
						issue := s.archive.Add(update.block, *update.update, update.updateHints)
						if issue != nil {
							err <- issue
						}
					}
				}
			}()

			s.archiveWriter = in
			s.archiveWriterDone = done
			s.archiveWriterFlushDone = flush
			s.archiveWriterError = err
		}

		// Send the update to the writer to be processed asynchronously.
		s.archiveWriter <- archiveUpdate{update, archiveUpdateHints}

		// Drain potential errors, but do not wait for them.
		var last error
		done := false
		for !done {
			select {
			// In case there was an error, process it.
			case err := <-s.archiveWriterError:
				last = err
			default:
				// all errors consumed, moving on
				done = true
			}
		}
		if last != nil {
			return last
		}
	}
	return nil
}

func (s *GoState) collectHashWorkerErrors() error {
	errs := []error{}
loop:
	for {
		select {
		case err := <-s.hashWorkerError:
			errs = append(errs, err)
		default:
			break loop
		}
	}
	return errors.Join(errs...)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(0)
	mf.AddChild("GoSchema", s.GoSchema.GetMemoryFootprint())
	if s.archive != nil {
		mf.AddChild("archive", s.archive.GetMemoryFootprint())
	}
	return mf
}

func (s *GoState) Flush() (lastErr error) {

	// Flush the hash worker.
	s.hashWorker <- stateUpdate{}
	<-s.hashWorkerFlushDone

	var errs []error
	if err := s.collectHashWorkerErrors(); err != nil {
		errs = append(errs, err)
	}

	// Flush the database.
	if err := s.GoSchema.Flush(); err != nil {
		errs = append(errs, err)
	}

	// Flush the archive.
	if s.archiveWriter != nil {
		// Signal to the archive worker that a flush should be conducted.
		s.archiveWriter <- archiveUpdate{}
		// Wait until the flush was processed.
		<-s.archiveWriterFlushDone
	}

	return errors.Join(errs...)
}

func (s *GoState) Close() (lastErr error) {
	if err := s.Flush(); err != nil {
		return err
	}

	// Shut down hash worker.
	close(s.hashWorker)
	<-s.hashWorkerDone

	if err := s.GoSchema.Close(); err != nil {
		lastErr = err
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
			lastErr = err
		}
	}

	if s.cleanup != nil {
		for _, clean := range s.cleanup {
			if clean != nil {
				clean()
			}
		}
	}
	return lastErr
}

func (s *GoState) GetArchiveState(block uint64) (as State, err error) {
	if s.archive == nil {
		return nil, fmt.Errorf("archive not enabled for this GoState")
	}
	lastBlock, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		return nil, fmt.Errorf("failed to get last block in the archive; %s", err)
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
		return 0, false, fmt.Errorf("archive not enabled for this GoState")
	}
	lastBlock, empty, err := s.archive.GetBlockHeight()
	if err != nil {
		return 0, false, fmt.Errorf("failed to get last block in the archive; %s", err)
	}
	return lastBlock, empty, nil
}

func (s *GoState) GetProof() (backend.Proof, error) {
	components := s.GoSchema.getSnapshotableComponents()
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
	components := s.GoSchema.getSnapshotableComponents()
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
	components := s.GoSchema.getSnapshotableComponents()
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
	return s.GoSchema.runPostRestoreTasks()
}

func (s *GoState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	components := s.GoSchema.getSnapshotableComponents()
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
