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
			if _, existsInShadow := c.shadow[value.address]; existsInShadow {
				delete(c.shadow, value.address)
			}
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

	deserialise := func(b *[]byte) accountPayload {
		var addr shortAddress
		var nonce common.Nonce
		var balance common.Balance
		var codeHash common.Hash
		if len(*b) >= len(addr) {
			addr = shortAddress((*b)[0:len(addr)])
			*b = (*b)[len(addr):]
		}
		if len(*b) >= len(nonce) {
			nonce = common.Nonce((*b)[0:len(nonce)])
			*b = (*b)[len(nonce):]
		}
		if len(*b) >= len(balance) {
			balance = common.Balance((*b)[0:len(balance)])
			*b = (*b)[len(balance):]
		}
		if len(*b) >= len(codeHash) {
			codeHash = common.Hash((*b)[0:len(codeHash)])
			*b = (*b)[len(codeHash):]
		}

		return accountPayload{addr, AccountInfo{nonce, balance, codeHash}}
	}

	deserialiseAddrOnly := func(b *[]byte) accountPayload {
		var addr shortAddress
		if len(*b) >= len(addr) {
			addr = shortAddress((*b)[0:len(addr)])
			*b = (*b)[len(addr):]
		}
		return accountPayload{address: addr}
	}

	registry := fuzzing.NewRegistry[opType, liveTrieAccountFuzzingContext]()
	fuzzing.RegisterDataOp(registry, set, serialise, deserialise, opSet)
	fuzzing.RegisterDataOp(registry, get, serialiseAddrOnly, deserialiseAddrOnly, opGet)
	fuzzing.RegisterDataOp(registry, deleteAddr, serialiseAddrOnly, deserialiseAddrOnly, opDelete)

	fuzzing.Fuzz[liveTrieAccountFuzzingContext](f, &liveTrieAccountFuzzingCampaign{registry: registry})
}

// opType is operation type to be applied to an MPT.
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
	shadow   map[shortAddress]AccountInfo
}

func (c *liveTrieAccountFuzzingCampaign) Init() []fuzzing.OperationSequence[liveTrieAccountFuzzingContext] {

	var addr1 common.Address
	var addr2 common.Address
	var addr3 common.Address

	for i := 0; i < common.AddressSize; i++ {
		addr2[i] = byte(i + 1)
		addr3[i] = byte(0xFF)
	}

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
		for _, addr := range []common.Address{addr1, addr2, addr3} {
			for _, nonce := range []common.Nonce{nonce1, nonce2, nonce3} {
				for _, balance := range []common.Balance{balance1, balance2, balance3} {
					for _, codeHash := range []common.Hash{codeHash1, codeHash2, codeHash3} {
						info := AccountInfo{nonce, balance, codeHash}
						sequence = append(sequence, c.registry.CreateDataOp(set, accountPayload{createShortAddress(addr), info}))
					}
				}
			}
		}
		seed = append(seed, sequence)
	}

	{
		var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		for _, addr := range []common.Address{addr1, addr2, addr3} {
			info := AccountInfo{}
			sequence = append(sequence, c.registry.CreateDataOp(deleteAddr, accountPayload{createShortAddress(addr), info}))
		}
		seed = append(seed, sequence)
	}

	{
		var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		for _, addr := range []common.Address{addr1, addr2, addr3} {
			info := AccountInfo{}
			sequence = append(sequence, c.registry.CreateDataOp(get, accountPayload{createShortAddress(addr), info}))
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
	shadow := make(map[shortAddress]AccountInfo)
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

// shortAddress is the account address which is shrunk to 6-bytes to limit the address space.
type shortAddress [6]byte

func createShortAddress(addr common.Address) shortAddress {
	var res shortAddress
	for i := 0; i < len(res); i++ {
		res[i] = addr[i]
	}

	return res
}

// accountPayload comprises account address and account info.
type accountPayload struct {
	address shortAddress
	info    AccountInfo
}

// Serialise lays out the account data as: <shortAddress><nonce><balance><codeHash>
func (a *accountPayload) Serialise() []byte {
	res := make([]byte, 0, len(a.address)+len(a.info.Nonce)+len(a.info.Balance)+len(a.info.CodeHash))
	res = append(res, a.address[:]...)
	res = append(res, a.info.Nonce[:]...)
	res = append(res, a.info.Balance[:]...)
	res = append(res, a.info.CodeHash[:]...)
	return res
}

// SerialiseAddress serializes the address of an accountPayload.
// The serialized data is laid out as <shortAddress>.
func (a *accountPayload) SerialiseAddress() []byte {
	res := make([]byte, 0, len(a.address))
	res = append(res, a.address[:]...)
	return res
}

// GetAddress returns the address of an accountPayload by copying its address bytes into a new common.Address variable and returning it.
// It loops over the address bytes of the accountPayload and copies each byte into the new common.Address variable.
// The resulting address is then returned.
func (a *accountPayload) GetAddress() common.Address {
	var addr common.Address
	for i := 0; i < len(a.address); i++ {
		addr[i] = a.address[i]
	}
	return addr
}
