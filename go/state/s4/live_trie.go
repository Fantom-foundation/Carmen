package s4

import (
	"encoding/json"
	"errors"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type LiveTrie struct {
	// The root node of the Trie.
	root NodeId
	// The file name for storing trie metadata.
	metadatafile string
	// The node structure of the trie.
	forest *Forest
}

func OpenInMemoryLiveTrie(directory string, mode StorageMode) (*LiveTrie, error) {
	forest, err := OpenInMemoryForest(directory, mode)
	if err != nil {
		return nil, err
	}
	return makeTrie(directory, forest)
}

func OpenFileLiveTrie(directory string, mode StorageMode) (*LiveTrie, error) {
	forest, err := OpenFileForest(directory, mode)
	if err != nil {
		return nil, err
	}
	return makeTrie(directory, forest)
}

func getTrieView(root NodeId, forest *Forest) *LiveTrie {
	return &LiveTrie{
		root:   root,
		forest: forest,
	}
}

func makeTrie(
	directory string,
	forest *Forest,
) (*LiveTrie, error) {
	// Parse metadata file.
	metadatafile := directory + "/meta.json"
	metadata, err := readMetadata(metadatafile)
	if err != nil {
		return nil, err
	}
	return &LiveTrie{
		root:         metadata.RootNode,
		metadatafile: metadatafile,
		forest:       forest,
	}, nil
}

func (s *LiveTrie) GetAccountInfo(addr common.Address) (AccountInfo, bool, error) {
	return s.forest.GetAccountInfo(s.root, addr)
}

func (s *LiveTrie) SetAccountInfo(addr common.Address, info AccountInfo) error {
	newRoot, err := s.forest.SetAccountInfo(s.root, addr, info)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *LiveTrie) GetValue(addr common.Address, key common.Key) (common.Value, error) {
	return s.forest.GetValue(s.root, addr, key)
}

func (s *LiveTrie) SetValue(addr common.Address, key common.Key, value common.Value) error {
	newRoot, err := s.forest.SetValue(s.root, addr, key, value)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *LiveTrie) ClearStorage(addr common.Address) error {
	return s.forest.ClearStorage(s.root, addr)
}

func (s *LiveTrie) GetHash() (common.Hash, error) {
	return s.forest.GetHashFor(s.root)
}

func (s *LiveTrie) Flush() error {
	// Update on-disk meta-data.
	metadata, err := json.Marshal(metadata{
		RootNode: s.root,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.metadatafile, metadata, 0600); err != nil {
		return err
	}
	return s.forest.Flush()
}

func (s *LiveTrie) Close() error {
	return errors.Join(
		s.Flush(),
		s.forest.Close(),
	)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *LiveTrie) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("forest", s.forest.GetMemoryFootprint())
	return mf
}

// Dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *LiveTrie) Dump() {
	s.forest.Dump(s.root)
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is read to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behaviour of all other operations is undefined.
func (s *LiveTrie) Check() error {
	return s.forest.Check(s.root)
}

// -- LiveTrie metadata --

// metadata is the helper type to read and write metadata from/to the disk.
type metadata struct {
	RootNode NodeId
}

// readMetadata parses the content of the given file if it exists or returns
// a default-initialized metadata struct if there is no such file.
func readMetadata(filename string) (metadata, error) {

	// If there is no file, initialize and return default metadata.
	if _, err := os.Stat(filename); err != nil {
		return metadata{}, nil
	}

	// If the file exists, parse it and return its content.
	data, err := os.ReadFile(filename)
	if err != nil {
		return metadata{}, err
	}

	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}
