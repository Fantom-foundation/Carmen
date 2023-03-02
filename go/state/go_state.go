package state

import (
	"fmt"
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

	// Channels are only present if archive is enabled.
	archiveWriter          chan<- archiveUpdate
	archiveWriterFlushDone <-chan bool
	archiveWriterDone      <-chan bool
	archiveWriterError     <-chan error
}

type GoSchema interface {
	createAccount(address common.Address) (err error)
	Exists(address common.Address) (bool, error)
	deleteAccount(address common.Address) error
	GetBalance(address common.Address) (balance common.Balance, err error)
	setBalance(address common.Address, balance common.Balance) (err error)
	GetNonce(address common.Address) (nonce common.Nonce, err error)
	setNonce(address common.Address, nonce common.Nonce) (err error)
	GetStorage(address common.Address, key common.Key) (value common.Value, err error)
	setStorage(address common.Address, key common.Key, value common.Value) error
	GetCode(address common.Address) (value []byte, err error)
	GetCodeSize(address common.Address) (size int, err error)
	setCode(address common.Address, code []byte) (err error)
	GetCodeHash(address common.Address) (hash common.Hash, err error)
	GetHash() (hash common.Hash, err error)
	Flush() error
	Close() error
	common.MemoryFootprintProvider
}

func NewGoState(schema GoSchema, archive archive.Archive, cleanup []func()) *GoState {
	return &GoState{
		schema,
		cleanup,
		archive,
		nil,
		nil,
		nil,
		nil,
	}
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

type archiveUpdate = struct {
	block  uint64
	update *common.Update // nil to signal a flush
}

func (s *GoState) Apply(block uint64, update common.Update) error {
	err := applyUpdate(s, update)
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
				defer close(flush)
				defer close(done)
				// Process all incoming updates, no not stop on errors.
				for update := range in {
					// If there is no update, the state is asking for a flush signal.
					if update.update == nil {
						flush <- true
					} else {
						// Otherwise, process the update.
						issue := s.archive.Add(update.block, *update.update)
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

		// Send the update to the writer to be processessed asynchroniously.
		s.archiveWriter <- archiveUpdate{block, &update}

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
	err := s.GoSchema.Flush()
	if err != nil {
		lastErr = err
	}

	// Flush the archive.
	if s.archiveWriter != nil {
		// Signal to the archive worker that a flush should be conducted.
		s.archiveWriter <- archiveUpdate{}
		// Wait until the flush was processed.
		<-s.archiveWriterFlushDone
	}

	return lastErr
}

func (s *GoState) Close() (lastErr error) {
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
	lastBlock, err := s.archive.GetLastBlockHeight()
	if err != nil {
		return nil, fmt.Errorf("failed to get last block in the archive; %s", err)
	}
	if block > lastBlock {
		return nil, fmt.Errorf("block %d is not present in the archive (last block %d)", block, lastBlock)
	}
	return &ArchiveState{
		archive: s.archive,
		block:   block,
	}, nil
}
