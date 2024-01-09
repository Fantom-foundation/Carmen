package mpt

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"testing"
)

func FuzzLiveTrie_RandomAccountOps(f *testing.F) {
	var opSet = func(_ opType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		if err := c.liveTrie.SetAccountInfo(value.GetAddress(), value.info); err != nil {
			t.Errorf("error to set account: %s", err)
		}
		if value.info.IsEmpty() {
			delete(c.shadow, value.address)
		} else {
			c.shadow[value.address] = value.info
		}
	}

	var opGet = func(_ opType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		info, exists, err := c.liveTrie.GetAccountInfo(value.GetAddress())
		if err != nil {
			t.Errorf("cannot get account: %s", err)
		}
		shadow, existsShadow := c.shadow[value.address]
		if existsShadow != exists {
			t.Errorf("account existence does not match the shadow: %v-> %v != %v", value.address, existsShadow, exists)
		}
		if shadow != info {
			t.Errorf("accounts do not match: %v -> got: %v != want: %v", value.address, info, shadow)
		}
	}

	var opDelete = func(_ opType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		if err := c.liveTrie.SetAccountInfo(value.GetAddress(), AccountInfo{}); err != nil {
			t.Errorf("error to set account: %s", err)
		}
		delete(c.shadow, value.address)
	}

	serialise := func(payload accountPayload) []byte {
		return payload.Serialise()
	}
	serialiseAddrOnly := func(payload accountPayload) []byte {
		return payload.SerialiseAddress()
	}

	deserialiseAddrOnly := func(b *[]byte) accountPayload {
		var addr tinyAddress
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		return accountPayload{address: addr}
	}

	deserialise := func(b *[]byte) accountPayload {
		addr := deserialiseAddrOnly(b).address
		var nonce common.Nonce
		if len(*b) >= len(nonce) {
			nonce = common.Nonce((*b)[0:len(nonce)])
			*b = (*b)[len(nonce):]
		}
		var balance common.Balance
		if len(*b) >= len(balance) {
			balance = common.Balance((*b)[0:len(balance)])
			*b = (*b)[len(balance):]
		}
		var codeHash common.Hash
		if len(*b) >= len(codeHash) {
			codeHash = common.Hash((*b)[0:len(codeHash)])
			*b = (*b)[len(codeHash):]
		}

		return accountPayload{addr, AccountInfo{nonce, balance, codeHash}}
	}

	registry := fuzzing.NewRegistry[opType, liveTrieAccountFuzzingContext]()
	fuzzing.RegisterDataOp(registry, set, serialise, deserialise, opSet)
	fuzzing.RegisterDataOp(registry, get, serialiseAddrOnly, deserialiseAddrOnly, opGet)
	fuzzing.RegisterDataOp(registry, deleteAddr, serialiseAddrOnly, deserialiseAddrOnly, opDelete)

	fuzzing.Fuzz[liveTrieAccountFuzzingContext](f, &liveTrieAccountFuzzingCampaign{registry: registry})
}

// opType is an operation type to be applied to an MPT.
type opType byte

const (
	set opType = iota
	get
	deleteAddr
)

type liveTrieAccountFuzzingCampaign struct {
	registry fuzzing.OpsFactoryRegistry[opType, liveTrieAccountFuzzingContext]
}

type liveTrieAccountFuzzingContext struct {
	liveTrie *LiveTrie
	shadow   map[tinyAddress]AccountInfo
}

func (c *liveTrieAccountFuzzingCampaign) Init() []fuzzing.OperationSequence[liveTrieAccountFuzzingContext] {
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

	var seed []fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
	{
		var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
			for _, nonce := range []common.Nonce{nonce1, nonce2, nonce3} {
				for _, balance := range []common.Balance{balance1, balance2, balance3} {
					for _, codeHash := range []common.Hash{codeHash1, codeHash2, codeHash3} {
						info := AccountInfo{nonce, balance, codeHash}
						sequence = append(sequence, c.registry.CreateDataOp(set, accountPayload{addr, info}))
					}
				}
			}
		}
		seed = append(seed, sequence)
	}

	{
		var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
			info := AccountInfo{}
			sequence = append(sequence, c.registry.CreateDataOp(deleteAddr, accountPayload{addr, info}))
		}
		seed = append(seed, sequence)
	}

	{
		var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
			info := AccountInfo{}
			sequence = append(sequence, c.registry.CreateDataOp(get, accountPayload{addr, info}))
		}
		seed = append(seed, sequence)
	}

	return seed
}

func (c *liveTrieAccountFuzzingCampaign) CreateContext(t fuzzing.TestingT) *liveTrieAccountFuzzingContext {
	path := t.TempDir()
	liveTrie, err := OpenFileLiveTrie(path, S5LiveConfig, DefaultMptStateCapacity)
	if err != nil {
		t.Fatalf("failed to open live trie: %v", err)
	}
	shadow := make(map[tinyAddress]AccountInfo)
	return &liveTrieAccountFuzzingContext{liveTrie, shadow}
}

func (c *liveTrieAccountFuzzingCampaign) Deserialize(rawData []byte) []fuzzing.Operation[liveTrieAccountFuzzingContext] {
	return c.registry.ReadAllOps(rawData)
}

func (c *liveTrieAccountFuzzingCampaign) Cleanup(t fuzzing.TestingT, context *liveTrieAccountFuzzingContext) {
	if err := context.liveTrie.Check(); err != nil {
		t.Errorf("trie verification fails: \n%s", err)
	}
	if err := context.liveTrie.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}
}

// tinyAddress is a type representing a small address value.
type tinyAddress byte

// accountPayload comprises account address and account info.
type accountPayload struct {
	address tinyAddress
	info    AccountInfo
}

// Serialise lays out the account data as: <shortAddress><nonce><balance><codeHash>
func (a *accountPayload) Serialise() []byte {
	addr := a.SerialiseAddress()
	res := make([]byte, 0, len(addr)+len(a.info.Nonce)+len(a.info.Balance)+len(a.info.CodeHash))
	res = append(res, addr...)
	res = append(res, a.info.Nonce[:]...)
	res = append(res, a.info.Balance[:]...)
	res = append(res, a.info.CodeHash[:]...)
	return res
}

// SerialiseAddress serializes the address of an accountPayload.
// The serialized data is laid out as <tinyAddress>.
func (a *accountPayload) SerialiseAddress() []byte {
	return []byte{byte(a.address)}
}

// GetAddress converts the tinyAddress to the output common.Address.
// It assures all bytes of the output are filled with non-empty value,
// while the output being deterministic for all inputs.
// It does this by first getting the Keccak256 hash of the tinyAddress byte and then copying
// the resulting hash into the addr variable of type common.Address.
func (a *accountPayload) GetAddress() common.Address {
	var addr common.Address
	hash := common.GetKeccak256Hash([]byte{byte(a.address)})
	copy(addr[:], hash[:])
	return addr
}
