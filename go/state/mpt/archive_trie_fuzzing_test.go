package mpt

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"maps"
	"testing"
)

// FuzzArchiveTrie_RandomAccountOps performs random operations on an archive trie account.
// It is a wrapper function that calls fuzzArchiveTrieRandomAccountOps with the provided testing.F argument.
// This wrapper function is necessary for fuzzing.
func FuzzArchiveTrie_RandomAccountOps(f *testing.F) {
	fuzzArchiveTrieRandomAccountOps(f)
}

// fuzzArchiveTrieRandomAccountOps performs random operations (set, get, delete) on an archive trie account.
// Each set operation randomly modifies balance, nonce, or code hash of a random account.
// Each delete operation deletes a random account.
// Both set and delete operations are applied as an update applied as a new consecutive block.
// A shadow blockchain is maintained that gets applied the same modifications as the archive trie.
// Furthermore, each get operation takes a random block within already created blocks and reads a random account.
// The state of this account is matched with the state stored in the shadow blockchain.
// The account address is limited to one byte only to limit the address space.
// The reason is to increase the chance that the get operation hits an existing account generated by the set operation,
// and also set and delete operations hit a modification of already existing account.
func fuzzArchiveTrieRandomAccountOps(f *testing.F) {
	nonceSerialiser := common.NonceSerializer{}
	balanceSerialiser := common.BalanceSerializer{}

	var opSet = func(_ accountOpType, value archiveAccountPayload, t fuzzing.TestingT, c *archiveTrieAccountFuzzingContext) {
		update := common.Update{}
		var updateAccount func(info *AccountInfo)
		switch value.changedFieldType {
		case changeNonce:
			nonce := nonceSerialiser.FromBytes(value.changePayload)
			update.AppendNonceUpdate(value.GetAddress(), nonce)
			updateAccount = func(info *AccountInfo) {
				info.Nonce = nonce
			}
		case changeBalance:
			balance := balanceSerialiser.FromBytes(value.changePayload)
			update.AppendBalanceUpdate(value.GetAddress(), balance)
			updateAccount = func(info *AccountInfo) {
				info.Balance = balance
			}
		case changeCodeHash:
			update.AppendCodeUpdate(value.GetAddress(), value.changePayload)
			updateAccount = func(info *AccountInfo) {
				info.CodeHash = common.GetKeccak256Hash(value.changePayload)
			}
		}

		// Apply change to the archive trie
		if err := c.archiveTrie.Add(uint64(c.GetNextBlock()), update, nil); err != nil {
			t.Errorf("error to set account: %v -> %v_%v,  block: %d", value.address, value.changedFieldType, value.changePayload, c.GetCurrentBlock())
		}

		// Apply change to the shadow db
		c.AddUpdate(value.address, updateAccount)
	}

	var opGet = func(_ accountOpType, value archiveAccountPayload, t fuzzing.TestingT, c *archiveTrieAccountFuzzingContext) {
		blockHeight, empty, err := c.archiveTrie.GetBlockHeight()
		if err != nil {
			t.Errorf("cannot get block height: %v", blockHeight)
		}

		if c.IsEmpty() {
			if !empty {
				t.Errorf("blockchain should be empty")
			}
			return
		}

		if blockHeight != uint64(c.GetCurrentBlock()) {
			t.Errorf("block height does not match: got: %d != want: %d", blockHeight, c.GetNextBlock())
		}

		block := uint64(value.block % c.GetNextBlock()) // search only within existing blocks
		shadow := c.shadow[block]

		shadowAccount, exists := shadow[value.address]
		if !exists {
			return // the address was not inserted before calling this op, or it was deleted
		}

		fullAddress := value.GetAddress()
		nonce, err := c.archiveTrie.GetNonce(block, fullAddress)
		if err != nil {
			t.Errorf("cannot get nonce: %s", err)
		}
		if nonce != shadowAccount.Nonce {
			t.Errorf("nonces do not match: got %v != want: %v", nonce, shadowAccount.Nonce)
		}

		balance, err := c.archiveTrie.GetBalance(block, fullAddress)
		if err != nil {
			t.Errorf("cannot get balance: %s", err)
		}
		if balance != shadowAccount.Balance {
			t.Errorf("balances do not match: got %v != want: %v", balance, shadowAccount.Balance)
		}

		code, err := c.archiveTrie.GetCode(block, fullAddress)
		if err != nil {
			t.Errorf("cannot get code: %s", err)
		}

		// check code only when it was set before
		if code != nil {
			codeHash := common.GetKeccak256Hash(code)
			if codeHash != shadowAccount.CodeHash {
				t.Errorf("codeHashes do not match: got %v != want: %v", codeHash, shadowAccount.CodeHash)
			}
		}
	}

	var opDelete = func(_ accountOpType, value archiveAccountPayload, t fuzzing.TestingT, c *archiveTrieAccountFuzzingContext) {
		update := common.Update{}
		update.AppendDeleteAccount(value.GetAddress())
		if err := c.archiveTrie.Add(uint64(c.GetNextBlock()), update, nil); err != nil {
			t.Errorf("error to delete account: %v,  block: %d", value.address, c.GetNextBlock())
		}
		c.DeleteAccount(value.address)
	}

	serialiseAddressInfo := func(payload archiveAccountPayload) []byte {
		return payload.SerialiseAddressChange()
	}
	serialiseAddress := func(payload archiveAccountPayload) []byte {
		return payload.SerialiseAddress()
	}
	serialiseBlockAddress := func(payload archiveAccountPayload) []byte {
		return payload.SerialiseBlockAddress()
	}

	deserialiseAddressInfo := func(b *[]byte) archiveAccountPayload {
		var addr tinyAddress
		var changeType accountChangedFieldType
		var change []byte
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		if len(*b) >= 1 {
			changeType = accountChangedFieldType((*b)[0] % 3) // adjust to valid change types only
			*b = (*b)[1:]
		}

		switch changeType {
		case changeBalance:
			change = make([]byte, common.BalanceSize)
		case changeNonce:
			change = make([]byte, common.NonceSize)
		case changeCodeHash:
			change = make([]byte, common.HashSize)
		}
		copy(change, *b) // will copy max length of the 'change' or length of the 'b' bytes
		if len(*b) > len(change) {
			*b = (*b)[len(change):]
		} else {
			*b = (*b)[:] // drain remaining bytes
		}

		return archiveAccountPayload{0, addr, changeType, change}
	}

	deserialiseAddress := func(b *[]byte) archiveAccountPayload {
		var addr tinyAddress
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		var emptyChange []byte
		return archiveAccountPayload{0, addr, 0, emptyChange}
	}

	deserialiseBlockAddress := func(b *[]byte) archiveAccountPayload {
		var blockNumber uint
		var addr tinyAddress
		if len(*b) >= 4 {
			blockNumber = uint(binary.BigEndian.Uint32((*b)[0:4]))
			*b = (*b)[4:]
		}
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		var emptyChange []byte
		return archiveAccountPayload{blockNumber, addr, 0, emptyChange}
	}

	registry := fuzzing.NewRegistry[accountOpType, archiveTrieAccountFuzzingContext]()
	fuzzing.RegisterDataOp(registry, setAccount, serialiseAddressInfo, deserialiseAddressInfo, opSet)
	fuzzing.RegisterDataOp(registry, getAccount, serialiseBlockAddress, deserialiseBlockAddress, opGet)
	fuzzing.RegisterDataOp(registry, deleteAccount, serialiseAddress, deserialiseAddress, opDelete)

	init := func(registry fuzzing.OpsFactoryRegistry[accountOpType, archiveTrieAccountFuzzingContext]) []fuzzing.OperationSequence[archiveTrieAccountFuzzingContext] {
		var nonce1 common.Nonce
		var nonce2 common.Nonce
		var nonce3 common.Nonce

		for i := 0; i < common.NonceSize; i++ {
			nonce2[i] = byte(i + 1)
			nonce3[i] = byte(0xFF)
		}

		var balance1 common.Balance
		var balance2 common.Balance
		var balance3 common.Balance

		for i := 0; i < common.BalanceSize; i++ {
			balance2[i] = byte(i + 1)
			balance3[i] = byte(0xFF)
		}

		var codeHash1 common.Hash
		var codeHash2 common.Hash
		var codeHash3 common.Hash

		for i := 0; i < common.HashSize; i++ {
			codeHash2[i] = byte(i + 1)
			codeHash3[i] = byte(0xFF)
		}

		var seed []fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
		{
			var sequence fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, codeHash := range []common.Hash{codeHash1, codeHash2, codeHash3} {
					sequence = append(sequence, registry.CreateDataOp(setAccount, archiveAccountPayload{0, addr, changeCodeHash, codeHash[:]}))
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, nonce := range []common.Nonce{nonce1, nonce2, nonce3} {
					sequence = append(sequence, registry.CreateDataOp(setAccount, archiveAccountPayload{0, addr, changeNonce, nonce[:]}))
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, balance := range []common.Balance{balance1, balance2, balance3} {
					sequence = append(sequence, registry.CreateDataOp(setAccount, archiveAccountPayload{0, addr, changeBalance, balance[:]}))
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
			var emptyChange []byte
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				sequence = append(sequence, registry.CreateDataOp(deleteAccount, archiveAccountPayload{0, addr, 0, emptyChange}))
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[archiveTrieAccountFuzzingContext]
			var emptyChange []byte
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, block := range []uint{0, 1, 2, 5, 10, 255} {
					sequence = append(sequence, registry.CreateDataOp(getAccount, archiveAccountPayload{block, addr, 0, emptyChange}))
				}
			}
			seed = append(seed, sequence)
		}

		return seed
	}

	create := func(archiveTrie *ArchiveTrie) *archiveTrieAccountFuzzingContext {
		shadow := make([]map[tinyAddress]AccountInfo, 0, 100)
		return &archiveTrieAccountFuzzingContext{archiveTrie, shadow}
	}

	fuzzing.Fuzz[archiveTrieAccountFuzzingContext](f, &archiveTrieAccountFuzzingCampaign[accountOpType, archiveTrieAccountFuzzingContext]{registry: registry, init: init, create: create})
}

// accountChangedFieldType is a type used to represent the field that was changed in an account.
type accountChangedFieldType byte

const (
	changeBalance accountChangedFieldType = iota
	changeNonce
	changeCodeHash
)

// archiveAccountPayload represents the payload used for archiving an account in the ArchiveTrie data structure.
// It contains the following fields:
// - block: an unsigned integer representing the block number
// - address: an instance of the type tinyAddress, which is a byte representing the address of the account
// - changedFieldType: an instance of the type accountChangedFieldType representing the field that was changed in the account
// - changePayload: a byte slice representing the payload of the change
type archiveAccountPayload struct {
	block            uint
	address          tinyAddress
	changedFieldType accountChangedFieldType
	changePayload    []byte
}

// SerialiseAddressInfo serializes the address information of an archiveAccountPayload into a byte slice.
// It constructs a byte slice with a capacity of 1 + length of nonce + code hash + balance.
// It appends the address, nonce, balance, and code hash to the byte slice.
func (a *archiveAccountPayload) SerialiseAddressChange() []byte {
	res := make([]byte, 0, 1+1+len(a.changePayload))
	res = append(res, byte(a.address))
	res = append(res, byte(a.changedFieldType))
	res = append(res, a.changePayload...)
	return res
}

func (a *archiveAccountPayload) SerialiseAddress() []byte {
	return []byte{byte(a.address)}
}

// SerialiseBlockAddress serializes the block address information of an archiveAccountPayload into a byte slice.
// It constructs a byte slice with a capacity of 1 (tiny address) + 4 (block number).
// It appends the four bytes of the block in big endian order, and the address to the byte slice.
func (a *archiveAccountPayload) SerialiseBlockAddress() []byte {
	res := make([]byte, 0, 1+4)
	res = append(res, byte(a.block>>24), byte(a.block>>16), byte(a.block>>8), byte(a.block))
	res = append(res, byte(a.address))
	return res
}

// GetAddress converts the tinyAddress to the output common.Address.
// It assures all bytes of the output are filled with non-empty value,
// while the output is deterministic for all inputs.
// It does this by first getting the Keccak256 hash of the tinyAddress byte and then copying
// the resulting hash into the addr variable of type common.Address.
func (a *archiveAccountPayload) GetAddress() common.Address {
	var addr common.Address
	hash := common.GetKeccak256Hash([]byte{byte(a.address)})
	copy(addr[:], hash[:])
	return addr
}

// archiveTrieAccountFuzzingContext contains the following fields:
// - archiveTrie: a reference to the ArchiveTrie
// - shadow: a slice of maps representing the modified account versions indexed by block number
type archiveTrieAccountFuzzingContext struct {
	archiveTrie *ArchiveTrie
	shadow      []map[tinyAddress]AccountInfo // index is a block number, AccountInfo is a version of the account valid in this block
}

// AddUpdate adds or updates an account in the archiveTrieAccountFuzzingContext.
// It takes an address and an updateAccount function as parameters.
// The function first copies the current block to a new map as a base for a new block.
// Then, it locates if an AccountInfo for the given address exists in this map.
// It creates a new empty AccountInfo if the address does not exist.
// The updateAccount callback is called on this AccountInfo to apply a requested update on the account.
// After the updateAccount function completes, the map is updated with the modified AccountInfo.
// Finally, the updated map is appended to the shadow slice, creating a new block.
func (c *archiveTrieAccountFuzzingContext) AddUpdate(address tinyAddress, updateAccount func(info *AccountInfo)) {
	// copy current block first
	current := make(map[tinyAddress]AccountInfo)
	if len(c.shadow) > 0 {
		current = maps.Clone(c.shadow[c.GetCurrentBlock()])
	}

	// apply change to the right accountInfo
	var accountInfo AccountInfo
	if info, exists := current[address]; exists {
		accountInfo = info
	} else {
		accountInfo = AccountInfo{}
	}
	updateAccount(&accountInfo)
	current[address] = accountInfo

	// assign to the next block
	c.shadow = append(c.shadow, current)
}

// DeleteAccount deletes the account with the given address from a new block.
// It first makes a copy of the current block and assigns it to a new map as a preparation for the next block.
// Then, it deletes the account with the given address from this map and appends
// it to the shadow slice, creating a new block.
func (c *archiveTrieAccountFuzzingContext) DeleteAccount(address tinyAddress) {
	// copy current block first
	current := make(map[tinyAddress]AccountInfo)
	if len(c.shadow) > 0 {
		current = maps.Clone(c.shadow[c.GetCurrentBlock()])
	}

	// delete from current state
	delete(current, address)

	// assign to the next block
	c.shadow = append(c.shadow, current)
}

// GetNextBlock returns the number of next block in this shadow blockchain.
func (c *archiveTrieAccountFuzzingContext) GetNextBlock() uint {
	return uint(len(c.shadow))
}

// GetCurrentBlock returns the current block number of this shadow blockchain.
// If this blockchain is empty, zero is returned the same way when the blockchain has one block.
func (c *archiveTrieAccountFuzzingContext) GetCurrentBlock() uint {
	blocks := len(c.shadow)
	if blocks == 0 {
		return 0
	} else {
		return uint(blocks - 1)
	}
}

// IsEmpty returns true if this shadow blockchain contains any blocks.
func (c *archiveTrieAccountFuzzingContext) IsEmpty() bool {
	return len(c.shadow) == 0
}

// archiveTrieAccountFuzzingCampaign represents a fuzzing campaign for testing the archiveTrie data structure.
// It contains the following fields:
// - registry: an OpsFactoryRegistry that maps operation types to operation factories
// - archiveTrie: a pointer to an ArchiveTrie data structure
// - init: a function that initializes the fuzzing campaign by returning an array of OperationSequences
// - create: a function that creates a new instance of the type C
// The OpsFactoryRegistry is used to register and create operations for the fuzzing campaign.
type archiveTrieAccountFuzzingCampaign[T ~byte, C any] struct {
	registry    fuzzing.OpsFactoryRegistry[T, C]
	archiveTrie *ArchiveTrie
	init        func(fuzzing.OpsFactoryRegistry[T, C]) []fuzzing.OperationSequence[C]
	create      func(*ArchiveTrie) *C
}

// Init initializes the archiveTrieAccountFuzzingCampaign.
// It calls the c.init method with the registry parameter and returns the result.
// The returned value is of type []fuzzing.OperationSequence[C].
func (c *archiveTrieAccountFuzzingCampaign[T, C]) Init() []fuzzing.OperationSequence[C] {
	return c.init(c.registry)
}

// CreateContext creates a new context for the archiveTrieAccountFuzzingCampaign.
// It opens an archive trie at a temporary directory, assigns it to c.archiveTrie, and returns
// the created context.
func (c *archiveTrieAccountFuzzingCampaign[T, C]) CreateContext(t fuzzing.TestingT) *C {
	path := t.TempDir()
	archiveTrie, err := OpenArchiveTrie(path, S5LiveConfig, 10_000)
	if err != nil {
		t.Fatalf("failed to open archive trie: %v", err)
	}
	c.archiveTrie = archiveTrie
	return c.create(archiveTrie)
}

// Deserialize deserializes the given rawData into a slice of fuzzing.Operation[C].
// It uses the c.registry to read all the operations from the rawData.
func (c *archiveTrieAccountFuzzingCampaign[T, C]) Deserialize(rawData []byte) []fuzzing.Operation[C] {
	return c.registry.ReadAllOps(rawData)
}

// Cleanup handles the clean-up operations for the archiveTrieAccountFuzzingCampaign.
// It checks the correctness of the trie and closes the file.
func (c *archiveTrieAccountFuzzingCampaign[T, C]) Cleanup(t fuzzing.TestingT, _ *C) {
	if err := c.archiveTrie.Check(); err != nil {
		t.Errorf("trie verification fails: \n%s", err)
	}
	if err := c.archiveTrie.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}
}
