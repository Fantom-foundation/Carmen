package s4

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type StateTrie struct {
	// The root node of the Trie.
	root NodeId

	// The stock containers managing individual node types.
	branches   stock.Stock[uint32, BranchNode]
	extensions stock.Stock[uint32, ExtensionNode]
	accounts   stock.Stock[uint32, AccountNode]
	values     stock.Stock[uint32, ValueNode]
}

func OpenInMemoryTrie(directory string) (*StateTrie, error) {
	branches, err := memory.OpenStock[uint32, BranchNode](BranchNodeEncoder{}, directory+"/branches")
	if err != nil {
		return nil, err
	}
	extensions, err := memory.OpenStock[uint32, ExtensionNode](ExtensionNodeEncoder{}, directory+"/extensions")
	if err != nil {
		return nil, err
	}
	accounts, err := memory.OpenStock[uint32, AccountNode](AccountNodeEncoder{}, directory+"/accounts")
	if err != nil {
		return nil, err
	}
	values, err := memory.OpenStock[uint32, ValueNode](ValueNodeEncoder{}, directory+"/values")
	if err != nil {
		return nil, err
	}
	return &StateTrie{
		branches:   branches,
		extensions: extensions,
		accounts:   accounts,
		values:     values,
	}, nil
}

func (s *StateTrie) GetAccountInfo(addr common.Address) (AccountInfo, error) {
	node, err := s.getAccountNode(addr)
	if err != nil || node == nil {
		return AccountInfo{}, err
	}
	return node.account, nil
}

func (s *StateTrie) SetAccountInfo(addr common.Address, info AccountInfo) error {
	node, err := s.getOrCreateAccountNode(addr)
	if err != nil {
		return err
	}
	node.account = info
	// TODO: call stock.Set(node) to commit changes
	return nil
}

func (s *StateTrie) GetValue(addr common.Address, key common.Key) (common.Value, error) {
	// Step 1: navigate to account level.
	account, err := s.getAccountNode(addr)
	if err != nil || account == nil || account.state.IsEmpty() {
		return common.Value{}, err
	}

	// Step 2: navigate within account state trie.
	panic("not implemented")
}

func (s *StateTrie) SetValue(addr common.Address, key common.Key, value common.Value) error {
	panic("not implemented")
}

func (s *StateTrie) Flush() error {
	return errors.Join(
		s.accounts.Flush(),
		s.branches.Flush(),
		s.extensions.Flush(),
		s.values.Flush(),
	)
}

func (s *StateTrie) Close() error {
	return errors.Join(
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

// dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *StateTrie) dump() {
	root, err := s.getNode(s.root)
	if err != nil {
		fmt.Printf("Failed to fetch root: %v", err)
	} else {
		root.Dump(s, "")
	}
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is read to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behaviour of all other operations is undefined.
func (s *StateTrie) Check() error {
	root, err := s.getNode(s.root)
	if err != nil {
		return err
	}
	return root.Check(s, make([]Nibble, 0, common.AddressSize*2))
}

func (s *StateTrie) getAccountNode(addr common.Address) (*AccountNode, error) {
	root, err := s.getNode(s.root)
	if err != nil {
		return nil, err
	}

	path := addressToNibbles(&addr)
	return root.GetAccount(s, &addr, path[:])
}

func (s *StateTrie) getOrCreateAccountNode(addr common.Address) (*AccountNode, error) {
	root, err := s.getNode(s.root)
	if err != nil {
		return nil, err
	}

	path := addressToNibbles(&addr)
	newRoot, res, err := root.GetOrCreateAccount(s, s.root, &addr, path[:])
	if err != nil {
		return nil, err
	}
	s.root = newRoot
	return res, nil
}

// -- NodeManager interface --

func (s *StateTrie) getNode(id NodeId) (Node, error) {
	var node Node
	var err error
	if id.IsValue() {
		node, err = s.values.Get(id.Index())
	} else if id.IsAccount() {
		node, err = s.accounts.Get(id.Index())
	} else if id.IsBranch() {
		node, err = s.branches.Get(id.Index())
	} else if id.IsExtension() {
		node, err = s.extensions.Get(id.Index())
	} else if id.IsEmpty() {
		node = EmptyNode{}
	} else {
		err = fmt.Errorf("unknown node ID: %v", id)
	}
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, fmt.Errorf("no node with ID %d in storage", id)
	}
	return node, nil
}

func (s *StateTrie) createAccount() (NodeId, *AccountNode, error) {
	i, node, err := s.accounts.New()
	return AccountId(i), node, err
}

func (s *StateTrie) createBranch() (NodeId, *BranchNode, error) {
	i, node, err := s.branches.New()
	return BranchId(i), node, err
}

func (s *StateTrie) createExtension() (NodeId, *ExtensionNode, error) {
	i, node, err := s.extensions.New()
	return ExtensionId(i), node, err
}

func (s *StateTrie) createValue() (NodeId, *ValueNode, error) {
	i, node, err := s.values.New()
	return ValueId(i), node, err
}

func (s *StateTrie) release(id NodeId) error {
	if id.IsAccount() {
		return s.accounts.Delete(id.Index())
	}
	if id.IsBranch() {
		return s.branches.Delete(id.Index())
	}
	if id.IsExtension() {
		return s.extensions.Delete(id.Index())
	}
	if id.IsValue() {
		return s.values.Delete(id.Index())
	}
	return fmt.Errorf("unable to release node %v", id)
}
