package mpt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

// MptState implementation of a state utilizes an MPT based data structure. While
// functionally equivalent to the Ethereum State MPT, hashes are computed using
// a configurable algorithm.
//
// The main role of the MptState is to provide an adapter between a LiveTrie and
// Carmen's State interface. Also, it retains an index of contract codes.
type MptState interface {
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
	GetCodeForHash(hash common.Hash) []byte
	GetCodeSize(address common.Address) (size int, err error)
	SetCode(address common.Address, code []byte) (err error)
	GetCodeHash(address common.Address) (hash common.Hash, err error)
	GetHash() (hash common.Hash, err error)
	Apply(block uint64, update common.Update) (archiveUpdateHints any, err error)
	Visit(visitor NodeVisitor) error
	GetCodes() (map[common.Hash][]byte, error)
	Flush() error
	Close() (lastErr error)
	GetSnapshotableComponents() []backend.Snapshotable
	RunPostRestoreTasks() error
	GetMemoryFootprint() *common.MemoryFootprint
}

type mptState struct {
	trie      *LiveTrie
	code      map[common.Hash][]byte
	codeMutex sync.Mutex
	codefile  string
	hasher    hash.Hash
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func newMptState(directory string, trie *LiveTrie) (*mptState, error) {
	codefile := directory + "/codes.json"
	codes, err := readCodes(codefile)
	if err != nil {
		return nil, err
	}
	return &mptState{
		trie:     trie,
		code:     codes,
		codefile: codefile,
	}, nil
}

// OpenGoMemoryState loads state information from the given directory and
// creates a Trie entirely retained in memory.
func OpenGoMemoryState(directory string, config MptConfig) (MptState, error) {
	trie, err := OpenInMemoryLiveTrie(directory, config)
	if err != nil {
		return nil, err
	}
	return newMptState(directory, trie)
}

func OpenGoFileState(directory string, config MptConfig) (MptState, error) {
	trie, err := OpenFileLiveTrie(directory, config)
	if err != nil {
		return nil, err
	}
	return newMptState(directory, trie)
}

func (s *mptState) CreateAccount(address common.Address) (err error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if exists {
		// For existing accounts, only clear the storage, preserve the rest.
		return s.trie.ClearStorage(address)
	}
	// Create account with hash of empty code.
	return s.trie.SetAccountInfo(address, AccountInfo{
		CodeHash: emptyCodeHash,
	})
}

func (s *mptState) Exists(address common.Address) (bool, error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *mptState) DeleteAccount(address common.Address) error {
	return s.trie.SetAccountInfo(address, AccountInfo{})
}

func (s *mptState) GetBalance(address common.Address) (balance common.Balance, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return common.Balance{}, err
	}
	return info.Balance, nil
}

func (s *mptState) SetBalance(address common.Address, balance common.Balance) (err error) {
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

func (s *mptState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	info, _, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (s *mptState) SetNonce(address common.Address, nonce common.Nonce) (err error) {
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

func (s *mptState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.trie.GetValue(address, key)
}

func (s *mptState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	return s.trie.SetValue(address, key, value)
}

func (s *mptState) GetCode(address common.Address) (value []byte, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	s.codeMutex.Lock()
	res := s.code[info.CodeHash]
	s.codeMutex.Unlock()
	return res, nil
}

func (s *mptState) GetCodeForHash(hash common.Hash) []byte {
	s.codeMutex.Lock()
	res := s.code[hash]
	s.codeMutex.Unlock()
	return res
}

func (s *mptState) GetCodeSize(address common.Address) (size int, err error) {
	code, err := s.GetCode(address)
	if err != nil {
		return 0, err
	}
	return len(code), err
}

func (s *mptState) SetCode(address common.Address, code []byte) (err error) {
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
	s.codeMutex.Lock()
	s.code[codeHash] = code
	s.codeMutex.Unlock()
	return s.trie.SetAccountInfo(address, info)
}

func (s *mptState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return emptyCodeHash, err
	}
	return info.CodeHash, nil
}

func (s *mptState) GetHash() (hash common.Hash, err error) {
	hash, _, err = s.trie.UpdateHashes()
	return hash, err
}

func (s *mptState) Apply(block uint64, update common.Update) (archiveUpdateHints any, err error) {
	if err := update.ApplyTo(s); err != nil {
		return nil, err
	}
	_, hints, err := s.trie.UpdateHashes()
	return hints, err
}

func (s *mptState) Visit(visitor NodeVisitor) error {
	return s.trie.VisitTrie(visitor)
}

func (s *mptState) GetCodes() (map[common.Hash][]byte, error) {
	return s.code, nil
}

func (s *mptState) Flush() error {
	// Flush codes and state trie.
	return errors.Join(
		writeCodes(s.code, s.codefile),
		s.trie.Flush(),
	)
}

func (s *mptState) Close() (lastErr error) {
	return errors.Join(
		s.Flush(),
		s.trie.Close(),
	)
}

func (s *mptState) GetSnapshotableComponents() []backend.Snapshotable {
	//panic("not implemented")
	return nil
}

func (s *mptState) RunPostRestoreTasks() error {
	//panic("not implemented")
	return nil
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *mptState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("trie", s.trie.GetMemoryFootprint())
	// TODO: add code store
	return mf
}

// readCodes parses the content of the given file if it exists or returns
// a an empty code collection if there is no such file.
func readCodes(filename string) (map[common.Hash][]byte, error) {
	// If there is no file, initialize and return an empty code collection.
	if _, err := os.Stat(filename); err != nil {
		return map[common.Hash][]byte{}, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	return parseCodes(reader)
}

func parseCodes(reader io.Reader) (map[common.Hash][]byte, error) {
	// If the file exists, parse it and return its content.
	res := map[common.Hash][]byte{}
	// The format is simple: [<key>, <length>, <code>]*
	var hash common.Hash
	var length [4]byte
	for {
		if _, err := io.ReadFull(reader, hash[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, err
		}
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint32(length[:])
		code := make([]byte, size)
		if _, err := io.ReadFull(reader, code[:]); err != nil {
			return nil, err
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
	writer := bufio.NewWriter(file)

	// The format is simple: [<key>, <length>, <code>]*
	for key, code := range codes {
		if _, err := writer.Write(key[:]); err != nil {
			return err
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(code)))
		if _, err := writer.Write(length[:]); err != nil {
			return err
		}
		if _, err := writer.Write(code); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return err
}

// ---------- memory state ----------

func OpenBufferedMptState(directory string, config MptConfig) (MptState, error) {
	trie, err := OpenFileLiveTrie(directory, config)
	if err != nil {
		return nil, err
	}
	base, err := newMptState(directory, trie)
	if err != nil {
		return nil, errors.Join(err, trie.Close())
	}

	stateCodes, err := base.GetCodes()
	if err != nil {
		return nil, errors.Join(err, base.Close())
	}

	// Load all data ...
	start := time.Now()
	fmt.Printf("Loading state into memory ...\n")
	collector := accountCollector{}
	if err := trie.VisitTrie(&collector); err != nil {
		return nil, errors.Join(err, trie.Close())
	}
	fmt.Printf("Loading finished, took %v\n", time.Since(start))

	codes := map[common.Hash][]byte{}
	for hash, code := range stateCodes {
		codes[hash] = code
	}

	// Start worker updating the trie in the background.
	updates := make(chan stateUpdate, 16)
	errors := make(chan error, 16)
	sync := make(chan bool)
	done := make(chan bool)

	go func() {
		defer close(errors)
		defer close(sync)
		defer close(done)
		for update := range updates {
			if update.update == nil {
				sync <- true
			} else {
				_, err := base.Apply(update.block, *update.update)
				if err != nil {
					select {
					case errors <- err:
					default:
					}
				}
			}
		}
	}()

	// Combine data and backend data structure.
	return &memoryState{
		state:    base,
		accounts: collector.accounts,
		codes:    codes,
		updates:  updates,
		errors:   errors,
		sync:     sync,
		done:     done,
	}, nil
}

type memoryState struct {
	// the full MPT state
	state *mptState
	// the in-memory copy of the world state
	accounts map[common.Address]account
	codes    map[common.Hash][]byte

	// infrastructure to perform asynchronous db updates
	updates chan<- stateUpdate
	errors  chan<- error
	sync    <-chan bool
	done    <-chan bool
}

type stateUpdate struct {
	block  uint64
	update *common.Update // nil to signal a sync
}

func (s *memoryState) CreateAccount(address common.Address) (err error) {
	account, exists := s.accounts[address]
	account.storage = nil
	if !exists {
		account.info.CodeHash = emptyCodeHash
	}
	s.accounts[address] = account
	return nil
}

func (s *memoryState) Exists(address common.Address) (bool, error) {
	_, exists := s.accounts[address]
	return exists, nil
}

func (s *memoryState) DeleteAccount(address common.Address) error {
	delete(s.accounts, address)
	return nil
}

func (s *memoryState) GetBalance(address common.Address) (balance common.Balance, err error) {
	return s.accounts[address].info.Balance, nil
}

func (s *memoryState) SetBalance(address common.Address, balance common.Balance) (err error) {
	account := s.accounts[address]
	account.info.Balance = balance
	s.accounts[address] = account
	return nil
}

func (s *memoryState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	return s.accounts[address].info.Nonce, nil
}

func (s *memoryState) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	account := s.accounts[address]
	account.info.Nonce = nonce
	s.accounts[address] = account
	return nil
}

func (s *memoryState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.accounts[address].storage[key], nil
}

func (s *memoryState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	account := s.accounts[address]
	if account.storage == nil {
		account.storage = map[common.Key]common.Value{}
	}
	account.storage[key] = value
	s.accounts[address] = account
	return nil
}

func (s *memoryState) GetCode(address common.Address) (value []byte, err error) {
	account, exists := s.accounts[address]
	if !exists {
		return nil, nil
	}
	return s.GetCodeForHash(account.info.CodeHash), nil
}

func (s *memoryState) GetCodeForHash(hash common.Hash) []byte {
	return s.codes[hash]
}

func (s *memoryState) GetCodeSize(address common.Address) (size int, err error) {
	code, _ := s.GetCode(address)
	return len(code), nil
}

func (s *memoryState) SetCode(address common.Address, code []byte) (err error) {
	hash := common.Keccak256(code)
	account := s.accounts[address]
	account.info.CodeHash = hash
	s.accounts[address] = account
	s.codes[hash] = code
	return nil
}

func (s *memoryState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	account, exists := s.accounts[address]
	if exists {
		return account.info.CodeHash, nil
	}
	return emptyCodeHash, nil
}

func (s *memoryState) GetHash() (hash common.Hash, err error) {
	s.waitForStateWriter()
	return s.state.GetHash()
}

func (s *memoryState) Apply(block uint64, update common.Update) (archiveUpdateHints any, err error) {
	// TODO: fix propagation of archive update hints
	if err := update.ApplyTo(s); err != nil {
		return nil, err
	}
	s.updates <- stateUpdate{
		block:  block,
		update: &update,
	}
	return nil, err
}

func (s *memoryState) Visit(visitor NodeVisitor) error {
	s.waitForStateWriter()
	return s.state.Visit(visitor)
}

func (s *memoryState) GetCodes() (map[common.Hash][]byte, error) {
	return s.codes, nil
}

func (s *memoryState) Flush() error {
	s.waitForStateWriter()
	return s.state.Flush()
}

func (s *memoryState) Close() (lastErr error) {
	close(s.updates)
	<-s.done
	return s.state.Close()
}

func (s *memoryState) GetSnapshotableComponents() []backend.Snapshotable {
	//panic("not implemented")
	return nil
}

func (s *memoryState) RunPostRestoreTasks() error {
	//panic("not implemented")
	return nil
}

func (s *memoryState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("db", s.state.GetMemoryFootprint())

	// Compute the size of the accounts.
	accountEntrySize := uintptr(common.AddressSize + unsafe.Sizeof(account{}))
	valueEntrySize := uintptr(common.KeySize + common.ValueSize)
	var size uintptr = accountEntrySize * uintptr(len(s.accounts))
	for _, account := range s.accounts {
		size += uintptr(len(account.storage)) * valueEntrySize
	}
	mf.AddChild("accounts", common.NewMemoryFootprint(size))

	// Compute the size of the codes.
	size = common.HashSize * uintptr(len(s.codes))
	for _, code := range s.codes {
		size += uintptr(len(code))
	}
	mf.AddChild("codes", common.NewMemoryFootprint(size))
	return mf
}

func (s *memoryState) waitForStateWriter() {
	s.updates <- stateUpdate{}
	<-s.sync
}

type account struct {
	info    AccountInfo
	storage map[common.Key]common.Value
}

type accountCollector struct {
	accounts       map[common.Address]account
	currentAddress common.Address
	currentValues  map[common.Key]common.Value
}

func (c *accountCollector) Visit(node Node, _ NodeInfo) VisitResponse {
	if c.accounts == nil {
		c.accounts = map[common.Address]account{}
	}
	switch cur := node.(type) {
	case *AccountNode:
		c.accounts[cur.address] = account{info: cur.info}
		c.currentAddress = cur.address
		c.currentValues = nil
	case *ValueNode:
		if c.currentValues == nil {
			values := map[common.Key]common.Value{}
			c.currentValues = values
			account := c.accounts[c.currentAddress]
			account.storage = values
			c.accounts[c.currentAddress] = account
		}
		c.currentValues[cur.key] = cur.value
	}
	return VisitResponseContinue
}
