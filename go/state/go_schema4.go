package state

import (
	"hash"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
	"golang.org/x/crypto/sha3"
)

// GoSchema4 implementation of a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type GoSchema4 struct {
	trie *s4.StateTrie
	// TODO: have a persistent storage for this
	code   map[common.Hash][]byte
	hasher hash.Hash
}

func newS4State(params Parameters, trie *s4.StateTrie) (State, error) {
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}
	return NewGoState(&GoSchema4{
		trie: trie,
		code: map[common.Hash][]byte{},
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
	// Nothing to do
	return nil
}

func (s *GoSchema4) Exists(address common.Address) (bool, error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return false, err
	}
	return !info.IsEmpty(), nil
}

func (s *GoSchema4) deleteAccount(address common.Address) error {
	return s.trie.SetAccountInfo(address, s4.AccountInfo{})
}

func (s *GoSchema4) GetBalance(address common.Address) (balance common.Balance, err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Balance{}, err
	}
	return info.Balance, nil
}

func (s *GoSchema4) setBalance(address common.Address, balance common.Balance) (err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Balance == balance {
		return nil
	}
	info.Balance = balance
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (s *GoSchema4) setNonce(address common.Address, nonce common.Nonce) (err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Nonce == nonce {
		return nil
	}
	info.Nonce = nonce
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.trie.GetValue(address, key)
}

func (s *GoSchema4) setStorage(address common.Address, key common.Key, value common.Value) error {
	return s.trie.SetValue(address, key, value)
}

func (s *GoSchema4) GetCode(address common.Address) (value []byte, err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return nil, err
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
	if code != nil { // codeHash is zero for empty code
		if s.hasher == nil {
			s.hasher = sha3.NewLegacyKeccak256()
		}
		codeHash = common.GetHash(s.hasher, code)
	}

	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.CodeHash == codeHash {
		return nil
	}
	info.CodeHash = codeHash
	s.code[codeHash] = code
	return s.trie.SetAccountInfo(address, info)
}

func (s *GoSchema4) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	info, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Hash{}, err
	}
	return info.CodeHash, nil
}

func (s *GoSchema4) GetHash() (hash common.Hash, err error) {
	// panic("not implemented")
	return common.Hash{}, nil
}

func (s *GoSchema4) Flush() (lastErr error) {
	return s.trie.Flush()
}

func (s *GoSchema4) Close() (lastErr error) {
	return s.trie.Close()
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
