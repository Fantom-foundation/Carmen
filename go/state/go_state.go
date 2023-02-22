package state

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"

	"github.com/Fantom-foundation/Carmen/go/backend/multimap"

	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

const (
	HashTreeFactor = 32
)

// GoState manages dependencies to other interfaces to build this service
type GoState struct {
	addressIndex    index.Index[common.Address, uint32]
	keyIndex        index.Index[common.Key, uint32]
	slotIndex       index.Index[common.SlotIdx[uint32], uint32]
	accountsStore   store.Store[uint32, common.AccountState]
	noncesStore     store.Store[uint32, common.Nonce]
	balancesStore   store.Store[uint32, common.Balance]
	valuesStore     store.Store[uint32, common.Value]
	codesDepot      depot.Depot[uint32]
	codeHashesStore store.Store[uint32, common.Hash]
	addressToSlots  multimap.MultiMap[uint32, uint32]
	cleanup         []func()
	hasher          hash.Hash
	archive         archive.Archive

	// Channels are only present if archive is enabled.
	archiveWriter          chan<- archiveUpdate
	archiveWriterFlushDone <-chan bool
	archiveWriterDone      <-chan bool
	archiveWriterError     <-chan error
}

type archiveUpdate = struct {
	block  uint64
	update *common.Update // nil to signal a flush
}

func (s *GoState) createAccount(address common.Address) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	err = s.accountsStore.Set(idx, common.Exists)
	if err != nil {
		return
	}
	return s.clearAccount(idx)
}

func (s *GoState) Exists(address common.Address) (bool, error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	state, err := s.accountsStore.Get(idx)
	return state == common.Exists, err
}

func (s *GoState) deleteAccount(address common.Address) error {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil
		}
		return err
	}
	err = s.accountsStore.Set(idx, common.Unknown)
	if err != nil {
		return err
	}
	return s.clearAccount(idx)
}

func (s *GoState) clearAccount(idx uint32) error {
	slotIdxs, err := s.addressToSlots.GetAll(idx)
	if err != nil {
		return err
	}
	for _, slotIdx := range slotIdxs {
		if err := s.valuesStore.Set(slotIdx, common.Value{}); err != nil {
			return err
		}
	}
	return s.addressToSlots.RemoveAll(idx)
}

func (s *GoState) GetBalance(address common.Address) (balance common.Balance, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Balance{}, nil
		}
		return
	}
	return s.balancesStore.Get(idx)
}

func (s *GoState) setBalance(address common.Address, balance common.Balance) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.balancesStore.Set(idx, balance)
}

func (s *GoState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Nonce{}, nil
		}
		return
	}
	return s.noncesStore.Get(idx)
}

func (s *GoState) setNonce(address common.Address, nonce common.Nonce) (err error) {
	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	return s.noncesStore.Set(idx, nonce)
}

func (s *GoState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	addressIdx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	keyIdx, err := s.keyIndex.Get(key)
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	slotIdx, err := s.slotIndex.Get(common.SlotIdx[uint32]{addressIdx, keyIdx})
	if err != nil {
		if err == index.ErrNotFound {
			return common.Value{}, nil
		}
		return
	}
	return s.valuesStore.Get(slotIdx)
}

func (s *GoState) setStorage(address common.Address, key common.Key, value common.Value) error {
	addressIdx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return err
	}
	keyIdx, err := s.keyIndex.GetOrAdd(key)
	if err != nil {
		return err
	}
	slotIdx, err := s.slotIndex.GetOrAdd(common.SlotIdx[uint32]{addressIdx, keyIdx})
	if err != nil {
		return err
	}
	err = s.valuesStore.Set(slotIdx, value)
	if err != nil {
		return err
	}
	if value == (common.Value{}) {
		err = s.addressToSlots.Remove(addressIdx, slotIdx)
	} else {
		err = s.addressToSlots.Add(addressIdx, slotIdx)
	}
	return err
}

func (s *GoState) GetCode(address common.Address) (value []byte, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return nil, nil
		}
		return
	}
	return s.codesDepot.Get(idx)
}

func (s *GoState) GetCodeSize(address common.Address) (size int, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return 0, nil
		}
		return
	}
	return s.codesDepot.GetSize(idx)
}

func (s *GoState) setCode(address common.Address, code []byte) (err error) {
	var codeHash common.Hash
	if code != nil { // codeHash is zero for empty code
		if s.hasher == nil {
			s.hasher = sha3.NewLegacyKeccak256()
		}
		codeHash = common.GetHash(s.hasher, code)
	}

	idx, err := s.addressIndex.GetOrAdd(address)
	if err != nil {
		return
	}
	err = s.codesDepot.Set(idx, code)
	if err != nil {
		return
	}
	return s.codeHashesStore.Set(idx, codeHash)
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func (s *GoState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	idx, err := s.addressIndex.Get(address)
	if err != nil {
		if err == index.ErrNotFound {
			return emptyCodeHash, nil
		}
		return
	}
	hash, err = s.codeHashesStore.Get(idx)
	if err != nil {
		return hash, err
	}
	// Stores use the default value in cases where there is no value present. Thus,
	// when returning a zero hash, we need to check whether it is indeed the case
	// that this is the hash of the code or whether we should actually return the
	// hash of the empty code.
	if (hash == common.Hash{}) {
		size, err := s.GetCodeSize(address)
		if err != nil {
			return hash, err
		}
		if size == 0 {
			return emptyCodeHash, nil
		}
	}
	return hash, nil
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

func (s *GoState) GetHash() (hash common.Hash, err error) {
	sources := []common.HashProvider{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.balancesStore,
		s.noncesStore,
		s.valuesStore,
		s.accountsStore,
		s.codesDepot,
		// codeHashesStore omitted intentionally
		// addressToSlots omitted intentionally
	}

	h := sha256.New()
	for _, source := range sources {
		if hash, err = source.GetStateHash(); err != nil {
			return
		}
		if _, err = h.Write(hash[:]); err != nil {
			return
		}
	}
	copy(hash[:], h.Sum(nil))
	return hash, nil
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(0)
	mf.AddChild("addressIndex", s.addressIndex.GetMemoryFootprint())
	mf.AddChild("keyIndex", s.keyIndex.GetMemoryFootprint())
	mf.AddChild("slotIndex", s.slotIndex.GetMemoryFootprint())
	mf.AddChild("accountsStore", s.accountsStore.GetMemoryFootprint())
	mf.AddChild("noncesStore", s.noncesStore.GetMemoryFootprint())
	mf.AddChild("balancesStore", s.balancesStore.GetMemoryFootprint())
	mf.AddChild("valuesStore", s.valuesStore.GetMemoryFootprint())
	mf.AddChild("codesDepot", s.codesDepot.GetMemoryFootprint())
	mf.AddChild("codeHashesStore", s.codeHashesStore.GetMemoryFootprint())
	mf.AddChild("addressToSlots", s.addressToSlots.GetMemoryFootprint())
	if s.archive != nil {
		mf.AddChild("archive", s.archive.GetMemoryFootprint())
	}
	return mf
}

func (s *GoState) Flush() error {
	flushables := []common.Flusher{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
		s.addressToSlots,
	}

	var last error = nil
	for _, flushable := range flushables {
		if err := flushable.Flush(); err != nil {
			last = err
		}
	}

	// Flush the archive.
	if s.archiveWriter != nil {
		// Signal to the archive worker that a flush should be conducted.
		s.archiveWriter <- archiveUpdate{}
		// Wait until the flush was processed.
		<-s.archiveWriterFlushDone
	}

	return last
}

func (s *GoState) Close() error {
	closeables := []io.Closer{
		s.addressIndex,
		s.keyIndex,
		s.slotIndex,
		s.accountsStore,
		s.noncesStore,
		s.balancesStore,
		s.valuesStore,
		s.codesDepot,
		s.codeHashesStore,
		s.addressToSlots,
	}

	var last error = nil
	for _, closeable := range closeables {
		if err := closeable.Close(); err != nil {
			last = err
		}
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
			last = err
		}
	}

	if s.cleanup != nil {
		for _, clean := range s.cleanup {
			if clean != nil {
				clean()
			}
		}
	}
	return last
}

func (s *GoState) GetArchiveState(block uint64) (as State, err error) {
	if s.archive == nil {
		return nil, fmt.Errorf("archive not enabled for this GoState")
	}
	return &ArchiveState{
		archive: s.archive,
		block:   block,
	}, nil
}
