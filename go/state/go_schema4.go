package state

import (
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
	"golang.org/x/crypto/sha3"
)

// GoSchema4 implementation of a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type GoSchema4 struct {
	trie     *s4.StateTrie
	code     map[common.Hash][]byte
	codefile string
	hasher   hash.Hash
}

func newS4State(params Parameters, trie *s4.StateTrie) (State, error) {
	codefile := params.Directory + "/codes.json"
	codes, err := readCodes(codefile)
	if err != nil {
		return nil, err
	}
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}
	return NewGoState(&GoSchema4{
		trie:     trie,
		code:     codes,
		codefile: codefile,
	}, arch, []func(){archiveCleanup}), nil
}

func NewGoMemoryS4State(params Parameters) (State, error) {
	trie, err := s4.OpenInMemoryTrie(params.Directory)
	if err != nil {
		return nil, err
	}
	return newS4State(params, trie)
}

func NewGoFileS4State(params Parameters) (State, error) {
	trie, err := s4.OpenFileTrie(params.Directory)
	if err != nil {
		return nil, err
	}
	return newS4State(params, trie)
}

func (s *GoSchema4) createAccount(address common.Address) (err error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if exists {
		// For existing accounts, only clear the storage, preserve the rest.
		return s.trie.ClearStorage(address)
	}
	// Create account with hash of empty code.
	return s.trie.SetAccountInfo(address, s4.AccountInfo{
		CodeHash: emptyCodeHash,
	})
}

func (s *GoSchema4) Exists(address common.Address) (bool, error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *GoSchema4) deleteAccount(address common.Address) error {
	return s.trie.SetAccountInfo(address, s4.AccountInfo{})
}

func (s *GoSchema4) GetBalance(address common.Address) (balance common.Balance, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return common.Balance{}, err
	}
	return info.Balance, nil
}

func (s *GoSchema4) setBalance(address common.Address, balance common.Balance) (err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Balance == balance {
		return nil
	}
	info.Balance = balance
	if !exists {
		info.CodeHash = emptyCodeHash
	}
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	info, _, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (s *GoSchema4) setNonce(address common.Address, nonce common.Nonce) (err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Nonce == nonce {
		return nil
	}
	info.Nonce = nonce
	if !exists {
		info.CodeHash = emptyCodeHash
	}
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.trie.GetValue(address, key)
}

func (s *GoSchema4) setStorage(address common.Address, key common.Key, value common.Value) error {
	return s.trie.SetValue(address, key, value)
}

func (s *GoSchema4) GetCode(address common.Address) (value []byte, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return s.code[info.CodeHash], nil
}

func (s *GoSchema4) GetCodeSize(address common.Address) (size int, err error) {
	code, err := s.GetCode(address)
	if err != nil {
		return 0, err
	}
	return len(code), err
}

func (s *GoSchema4) setCode(address common.Address, code []byte) (err error) {
	var codeHash common.Hash
	if s.hasher == nil {
		s.hasher = sha3.NewLegacyKeccak256()
	}
	codeHash = common.GetHash(s.hasher, code)

	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if !exists && len(code) == 0 {
		return nil
	}
	if info.CodeHash == codeHash {
		return nil
	}
	info.CodeHash = codeHash
	s.code[codeHash] = code
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return emptyCodeHash, err
	}
	return info.CodeHash, nil
}

func (s *GoSchema4) GetHash() (hash common.Hash, err error) {
	// panic("not implemented")
	return common.Hash{}, nil
}

func (s *GoSchema4) Flush() error {
	// Flush codes and state trie.
	return errors.Join(
		writeCodes(s.code, s.codefile),
		s.trie.Flush(),
	)
}

func (s *GoSchema4) Close() (lastErr error) {
	return errors.Join(
		s.Flush(),
		s.trie.Close(),
	)
}

func (s *GoSchema4) getSnapshotableComponents() []backend.Snapshotable {
	//panic("not implemented")
	return nil
}

func (s *GoSchema4) runPostRestoreTasks() error {
	//panic("not implemented")
	return nil
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *GoSchema4) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("trie", s.trie.GetMemoryFootprint())
	// TODO: add code store
	return mf
}

var errCorruptedCodeFile = errors.New("invalid encoding of code file")

// readCodes parses the content of the given file if it exists or returns
// a an empty code collection if there is no such file.
func readCodes(filename string) (map[common.Hash][]byte, error) {

	// If there is no file, initialize and return an empty code collection.
	if _, err := os.Stat(filename); err != nil {
		return map[common.Hash][]byte{}, nil
	}

	// If the file exists, parse it and return its content.
	res := map[common.Hash][]byte{}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// The format is simple: [<key>, <length>, <code>]*
	var hash common.Hash
	var length [4]byte
	for {
		if num, err := file.Read(hash[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, err
		} else if num != len(hash) {
			return nil, errCorruptedCodeFile
		}
		if num, err := file.Read(length[:]); err != nil {
			return nil, err
		} else if num != len(length) {
			return nil, errCorruptedCodeFile
		}

		size := binary.BigEndian.Uint32(length[:])
		code := make([]byte, size)
		if num, err := file.Read(code[:]); err != nil {
			return nil, err
		} else if num != len(code) {
			return nil, errCorruptedCodeFile
		}

		res[hash] = code
	}
}

// writeCodes write the given map of codes to the given file.
func writeCodes(codes map[common.Hash][]byte, filename string) (err error) {

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	// The format is simple: [<key>, <length>, <code>]*
	for key, code := range codes {
		if _, err := file.Write(key[:]); err != nil {
			return err
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(code)))
		if _, err := file.Write(length[:]); err != nil {
			return err
		}
		if _, err := file.Write(code); err != nil {
			return err
		}
	}
	return nil
}
