package s4

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type HashStore interface {
	Get(NodeId) (common.Hash, error)
	Set(NodeId, common.Hash) error

	common.FlushAndCloser
	common.MemoryFootprintProvider
}

// ----------------------------------------------------------------------------
//                          In-Memory Hash Store
// ----------------------------------------------------------------------------

type inMemoryHashStore struct {
	hashes   map[NodeId]common.Hash
	filename string
}

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
	return s.hashes[id], nil
}

func (s *inMemoryHashStore) Set(id NodeId, hash common.Hash) error {
	s.hashes[id] = hash
	return nil
}

func (s *inMemoryHashStore) Flush() error {
	return storeHashes(s.filename, s.hashes)
}

func (s *inMemoryHashStore) Close() error {
	return s.Flush()
}

func (s *inMemoryHashStore) GetMemoryFootprint() *common.MemoryFootprint {
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

type fileBasedHashStore struct {
	cache      *common.Cache[NodeId, common.Hash]
	dirty      map[NodeId]struct{}
	branches   *utils.BufferedFile
	extensions *utils.BufferedFile
	accounts   *utils.BufferedFile
	values     *utils.BufferedFile
}

func OpenFileBasedHashStore(directory string) (HashStore, error) {
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
		cache:      common.NewCache[NodeId, common.Hash](10_000_000),
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
	if id.IsAccount() {
		err = s.accounts.Read(int64(id.Index()), hash[:])
	} else if id.IsBranch() {
		err = s.branches.Read(int64(id.Index()), hash[:])
	} else if id.IsExtension() {
		err = s.extensions.Read(int64(id.Index()), hash[:])
	} else if id.IsValue() {
		err = s.values.Read(int64(id.Index()), hash[:])
	} else {
		err = fmt.Errorf("unsupported node ID type: %v", id)
	}
	if err != nil {
		return hash, err
	}
	if err := s.set(id, hash); err != nil {
		return hash, err
	}
	return hash, nil
}

func (s *fileBasedHashStore) Set(id NodeId, hash common.Hash) error {
	s.dirty[id] = struct{}{}
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
	if _, present := s.dirty[id]; !present {
		return nil
	}
	delete(s.dirty, id)
	if id.IsAccount() {
		return s.accounts.Write(int64(id.Index()), hash[:])
	} else if id.IsBranch() {
		return s.branches.Write(int64(id.Index()), hash[:])
	} else if id.IsExtension() {
		return s.extensions.Write(int64(id.Index()), hash[:])
	} else if id.IsValue() {
		return s.values.Write(int64(id.Index()), hash[:])
	} else {
		return fmt.Errorf("unsupported node ID type: %v", id)
	}
}

func (s *fileBasedHashStore) Flush() error {
	// Flush dirty hashes in order to reduce random seeking.
	ids := make([]NodeId, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
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
