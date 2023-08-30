package mpt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// HashStore is an interface for a component capable of storing hashes for
// nodes on secondary storage.
type HashStore interface {
	// Get retrieves the hash of a node retained in this store. Returns the
	// zero-hash if the hash has not been set before.
	Get(NodeId) (common.Hash, error)
	// Set updates the retained hash for a given node.
	Set(NodeId, common.Hash) error

	common.FlushAndCloser
	common.MemoryFootprintProvider
}

// ----------------------------------------------------------------------------
//                          In-Memory Hash Store
// ----------------------------------------------------------------------------

// inMemoryHashStore retains all hashes in an in-memory map and only syncs its
// content with the file system during open, flush, and close operations. It is
// mainly intended for debugging and unit testing since its memory consumption
// is linear in the number of retained nodes.
type inMemoryHashStore struct {
	hashes   map[NodeId]common.Hash
	filename string
	mu       sync.Mutex
}

// OpenInMemoryHashStore opens a HashStore backed by a file and retaining all
// hashes in an in-memory map. It is mainly intended for debugging and unit
// testing since its memory consumption is linear in the number of retained
// nodes.
func OpenInMemoryHashStore(directory string) (HashStore, error) {
	// Create the direcory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	filename := directory + "/hashes.dat"
	hashes, err := loadHashes(filename)
	if err != nil {
		return nil, err
	}

	return &inMemoryHashStore{
		hashes:   hashes,
		filename: filename,
	}, nil
}

func (s *inMemoryHashStore) Get(id NodeId) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hashes[id], nil
}

func (s *inMemoryHashStore) Set(id NodeId, hash common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hashes[id] = hash
	return nil
}

func (s *inMemoryHashStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storeHashes(s.filename, s.hashes)
}

func (s *inMemoryHashStore) Close() error {
	return s.Flush()
}

func (s *inMemoryHashStore) GetMemoryFootprint() *common.MemoryFootprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return common.NewMemoryFootprint(uintptr(len(s.hashes)) * (unsafe.Sizeof(NodeId(0)) + common.HashSize))
}

func loadHashes(file string) (map[NodeId]common.Hash, error) {
	// If there is no file, initialize and return an empty hash map.
	if _, err := os.Stat(file); err != nil {
		return map[NodeId]common.Hash{}, nil
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)

	res := map[NodeId]common.Hash{}
	var id [4]byte
	var hash common.Hash
	for {
		if num, err := reader.Read(id[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, err
		} else if num != len(id) {
			return nil, fmt.Errorf("invalid hash file")
		}
		if num, err := reader.Read(hash[:]); err != nil {
			return nil, err
		} else if num != len(hash) {
			return nil, fmt.Errorf("invalid hash file")
		}

		id := NodeId(binary.BigEndian.Uint32(id[:]))
		res[id] = hash
	}
}

func storeHashes(file string, hashes map[NodeId]common.Hash) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	// Simple file format: [<node-id>,<hash>]*
	var buffer [4]byte
	for id, hash := range hashes {
		binary.BigEndian.PutUint32(buffer[:], uint32(id))
		if _, err := writer.Write(buffer[:]); err != nil {
			return err
		}
		if _, err := writer.Write(hash[:]); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return f.Close()
}

// ----------------------------------------------------------------------------
//                          File-Based Hash Store
// ----------------------------------------------------------------------------

// fileBasedHashStore is a HashStore implementation backed by a file that
// retains a cache of most recently accessed hashes in memory.
type fileBasedHashStore struct {
	cache      *common.NWaysCache[NodeId, common.Hash] // < inherently thread safe
	dirty      map[NodeId]struct{}
	dirtyLock  sync.Mutex // protects access to the dirty map
	branches   *utils.BufferedFile
	extensions *utils.BufferedFile
	accounts   *utils.BufferedFile
	values     *utils.BufferedFile
	fileLock   sync.Mutex // protects access to all files
}

// OpenFileBasedHashStore opens a HashStore backed by a file and retaining a
// cache of the most recently accessed hashes in memory. Its memory utilization
// is thus constant. It is intended to be utilized in production.
func OpenFileBasedHashStore(directory string) (HashStore, error) {
	return openFileBasedHashStore(directory, 10_000_000)
}

// openFileBasedHashStore is the same as OpenFileBasedHashStore but with the
// option to define the utilized cache size. It is mainly intended to perform
// unit tests on reduced caches to stress-test the implementation.
func openFileBasedHashStore(directory string, cacheSize int) (HashStore, error) {
	// Create the direcory if needed.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	branches, err := utils.OpenBufferedFile(directory + "/branches.dat")
	if err != nil {
		return nil, err
	}
	extensions, err := utils.OpenBufferedFile(directory + "/extensions.dat")
	if err != nil {
		branches.Close()
		return nil, err
	}
	accounts, err := utils.OpenBufferedFile(directory + "/accounts.dat")
	if err != nil {
		branches.Close()
		extensions.Close()
		return nil, err
	}
	values, err := utils.OpenBufferedFile(directory + "/values.dat")
	if err != nil {
		branches.Close()
		extensions.Close()
		accounts.Close()
		return nil, err
	}
	return &fileBasedHashStore{
		cache:      common.NewNWaysCache[NodeId, common.Hash](cacheSize, 16),
		dirty:      map[NodeId]struct{}{},
		branches:   branches,
		extensions: extensions,
		accounts:   accounts,
		values:     values,
	}, nil
}

func (s *fileBasedHashStore) Get(id NodeId) (common.Hash, error) {
	if hash, exists := s.cache.Get(id); exists {
		return hash, nil
	}
	var hash common.Hash
	var err error
	s.fileLock.Lock()
	if id.IsAccount() {
		err = s.accounts.Read(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsBranch() {
		err = s.branches.Read(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsExtension() {
		err = s.extensions.Read(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsValue() {
		err = s.values.Read(int64(id.Index())*common.HashSize, hash[:])
	} else {
		err = fmt.Errorf("unsupported node ID type: %v", id)
	}
	s.fileLock.Unlock()
	if err != nil {
		return hash, err
	}
	if err := s.set(id, hash); err != nil {
		return hash, err
	}
	return hash, nil
}

func (s *fileBasedHashStore) Set(id NodeId, hash common.Hash) error {
	s.dirtyLock.Lock()
	s.dirty[id] = struct{}{}
	s.dirtyLock.Unlock()
	return s.set(id, hash)
}

func (s *fileBasedHashStore) set(id NodeId, hash common.Hash) error {
	if evictedId, evictedHash, evicted := s.cache.Set(id, hash); evicted {
		if err := s.sync(evictedId, evictedHash); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileBasedHashStore) sync(id NodeId, hash common.Hash) error {
	// Only write the hash if it is dirty.
	s.dirtyLock.Lock()
	if _, present := s.dirty[id]; !present {
		s.dirtyLock.Unlock()
		return nil
	}
	delete(s.dirty, id)
	s.dirtyLock.Unlock()
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	if id.IsAccount() {
		return s.accounts.Write(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsBranch() {
		return s.branches.Write(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsExtension() {
		return s.extensions.Write(int64(id.Index())*common.HashSize, hash[:])
	} else if id.IsValue() {
		return s.values.Write(int64(id.Index())*common.HashSize, hash[:])
	} else {
		return fmt.Errorf("unsupported node ID type: %v", id)
	}
}

func (s *fileBasedHashStore) Flush() error {
	// Flush dirty hashes in order to reduce random seeking.
	s.dirtyLock.Lock()
	ids := make([]NodeId, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	s.dirtyLock.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		hash, present := s.cache.Get(id)
		if !present {
			return fmt.Errorf("dirty hash for ID %d not present in cache", id)
		}
		if err := s.sync(id, hash); err != nil {
			return err
		}
	}
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	return errors.Join(
		s.branches.Flush(),
		s.extensions.Flush(),
		s.accounts.Flush(),
		s.values.Flush(),
	)
}

func (s *fileBasedHashStore) Close() error {
	return errors.Join(
		s.Flush(),
		s.branches.Close(),
		s.extensions.Close(),
		s.accounts.Close(),
		s.values.Close(),
	)
}

func (s *fileBasedHashStore) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("cache", s.cache.GetMemoryFootprint(unsafe.Sizeof(NodeId(0)+common.HashSize)))
	mf.AddChild("dirty", common.NewMemoryFootprint(uintptr(len(s.dirty))*unsafe.Sizeof(NodeId(0))))
	return mf
}
