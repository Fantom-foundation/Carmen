//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package mpt

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"golang.org/x/exp/maps"
)

type Diff map[common.Address]*AccountDiff

type AccountDiff struct {
	Reset   bool
	Balance *common.Balance
	Nonce   *common.Nonce
	Code    *common.Hash
	Storage map[common.Key]common.Value
}

func (d Diff) Equal(other Diff) bool {
	return reflect.DeepEqual(d, other)
}

func (d Diff) String() string {
	addresses := maps.Keys(d)
	sort.Slice(addresses, func(i, j int) bool {
		return string(addresses[i][:]) < string(addresses[j][:])
	})

	builder := strings.Builder{}
	builder.WriteString("Diff {\n")
	for _, address := range addresses {
		builder.WriteString(fmt.Sprintf("\t%x: \n", address[:]))
		diff := d[address]
		if diff.Reset {
			builder.WriteString(fmt.Sprintf("\t\tReset:   %t\n", diff.Reset))
		}
		if diff.Balance != nil {
			builder.WriteString(fmt.Sprintf("\t\tBalance: %x\n", *diff.Balance))
		}
		if diff.Nonce != nil {
			builder.WriteString(fmt.Sprintf("\t\tNonce:   %x\n", *diff.Nonce))
		}
		if diff.Code != nil {
			builder.WriteString(fmt.Sprintf("\t\tCode:    %x\n", *diff.Code))
		}

		if len(diff.Storage) > 0 {
			keys := maps.Keys(diff.Storage)
			sort.Slice(keys, func(i, j int) bool {
				return string(keys[i][:]) < string(keys[j][:])
			})
			for _, key := range keys {
				value := diff.Storage[key]
				builder.WriteString(fmt.Sprintf("\t\t\t%x: %x\n", key[:], value[:]))
			}
		}
	}
	builder.WriteString("}")
	return builder.String()
}

func (d *AccountDiff) Empty() bool {
	return !d.Reset && d.Balance == nil && d.Nonce == nil && d.Code == nil && len(d.Storage) == 0
}

func GetDiff(
	source NodeSource,
	before *NodeReference,
	after *NodeReference,
) (Diff, error) {
	context := &diffContext{
		source: source,
		result: Diff{},
	}

	if before.Id() == after.Id() {
		return context.result, nil
	}

	if err := collectDiff(context, triePosition{ref: *before}, triePosition{ref: *after}); err != nil {
		return nil, err
	}
	return context.result, nil
}

// -----

type triePosition struct {
	ref    NodeReference
	depth  int // the distance from the root node
	offset int // number of Nibbles consumed of Extension Node paths
}

func (p *triePosition) id() NodeId {
	return p.ref.Id()
}

func (p *triePosition) isLeaf() bool {
	id := p.id()
	return id.IsEmpty() || id.IsAccount() || id.IsValue()
}

func (p *triePosition) getReadAccess(source NodeSource) (shared.ReadHandle[Node], error) {
	return source.getReadAccess(&p.ref)
}

func (p *triePosition) getChild(source NodeSource, nibble Nibble) (triePosition, error) {
	if p.id().IsEmpty() {
		return *p, nil
	}

	handle, err := p.getReadAccess(source)
	if err != nil {
		return triePosition{}, err
	}
	defer handle.Release()

	if p.ref.Id().IsAccount() {
		path := AddressToNibblePath(handle.Get().(*AccountNode).address, source)
		if path[p.depth] == nibble {
			return triePosition{ref: p.ref, depth: p.depth + 1}, nil
		}
		return triePosition{ref: emptyNodeReference}, nil
	}

	if p.ref.Id().IsValue() {
		path := KeyToNibblePath(handle.Get().(*ValueNode).key, source)
		if path[p.depth] == nibble {
			return triePosition{ref: p.ref, depth: p.depth + 1}, nil
		}
		return triePosition{ref: emptyNodeReference}, nil
	}

	if p.ref.Id().IsBranch() {
		child := handle.Get().(*BranchNode).children[nibble]
		return triePosition{ref: child, depth: p.depth + 1}, nil
	}

	extension := handle.Get().(*ExtensionNode)

	// If the requested child is deviating from the extension's path, return an empty position.
	if nibble != extension.path.Get(p.offset) {
		return triePosition{ref: emptyNodeReference}, nil
	}

	// If the end of the path would be reached, return the next node.
	if p.offset+1 == extension.path.Length() {
		return triePosition{
			ref:   extension.next,
			depth: p.depth + 1,
		}, nil
	}

	// Otherwise, return a position pointing to the same extension but with increased offset.
	return triePosition{
		ref:    p.ref,
		depth:  p.depth + 1,
		offset: p.offset + 1,
	}, nil
}

// ------

type diffContext struct {
	source         NodeSource
	currentAccount *common.Address
	result         Diff
}

var emptyNodeReference = NewNodeReference(EmptyId())

func collectDiff(
	context *diffContext,
	before triePosition,
	after triePosition,
) error {
	if before.id() == after.id() {
		return nil
	}

	if before.isLeaf() && after.isLeaf() {
		return collectDiffFromLeafs(context, before, after)
	}

	for i := Nibble(0); i < Nibble(16); i++ {
		lhs, err := before.getChild(context.source, i)
		if err != nil {
			return err
		}
		rhs, err := after.getChild(context.source, i)
		if err != nil {
			return err
		}
		if err := collectDiff(context, lhs, rhs); err != nil {
			return err
		}
	}
	return nil
}

func collectDiffFromLeafs(context *diffContext, before triePosition, after triePosition) error {
	lhs := before.id()
	rhs := after.id()

	// Handle newly added accounts and values.
	if lhs.IsEmpty() {
		if rhs.IsAccount() {
			// A new account is present in the after state.
			handle, err := after.getReadAccess(context.source)
			if err != nil {
				return err
			}
			defer handle.Release()
			account := handle.Get().(*AccountNode)
			return recordAddedAccount(context, account)
		}
		if rhs.IsValue() {
			// A new value is present in the after state.
			handle, err := after.getReadAccess(context.source)
			if err != nil {
				return err
			}
			defer handle.Release()
			value := handle.Get().(*ValueNode)
			recordValueUpdate(context, value.key, value.value)
		}
		return nil
	}

	// Handle deleted values.
	if rhs.IsEmpty() {
		if lhs.IsAccount() {
			// An account is removed in the after state.
			handle, err := before.getReadAccess(context.source)
			if err != nil {
				return err
			}
			defer handle.Release()
			account := handle.Get().(*AccountNode)
			context.result[account.address] = &AccountDiff{Reset: true}
		}
		if lhs.IsValue() {
			// A value is removed in the after state.
			handle, err := before.getReadAccess(context.source)
			if err != nil {
				return err
			}
			defer handle.Release()
			value := handle.Get().(*ValueNode)
			recordValueUpdate(context, value.key, common.Value{})
		}
		return nil
	}

	// Handle modified accounts.
	if lhs.IsAccount() && rhs.IsAccount() {
		beforeHandle, err := before.getReadAccess(context.source)
		if err != nil {
			return err
		}
		defer beforeHandle.Release()
		afterHandle, err := after.getReadAccess(context.source)
		if err != nil {
			return err
		}
		defer afterHandle.Release()

		beforeNode := beforeHandle.Get().(*AccountNode)
		afterNode := afterHandle.Get().(*AccountNode)

		if beforeNode.address != afterNode.address {
			// The old account was deleted.
			recordDeletedAccount(context, beforeNode)

			// And a new account was created.
			return recordAddedAccount(context, afterNode)
		}

		diff := &AccountDiff{}
		if beforeNode.info.Balance != afterNode.info.Balance {
			diff.Balance = new(common.Balance)
			*diff.Balance = afterNode.info.Balance
		}
		if beforeNode.info.Nonce != afterNode.info.Nonce {
			diff.Nonce = new(common.Nonce)
			*diff.Nonce = afterNode.info.Nonce
		}
		if beforeNode.info.CodeHash != afterNode.info.CodeHash {
			diff.Code = new(common.Hash)
			*diff.Code = afterNode.info.CodeHash
		}
		if !diff.Empty() {
			context.result[afterNode.address] = diff
		}

		// Also collect storage differences.
		if beforeNode.storage.Id() != afterNode.storage.Id() {
			context.currentAccount = &afterNode.address
			return collectDiff(context, triePosition{ref: beforeNode.storage}, triePosition{ref: afterNode.storage})
		}
		return nil
	}

	if lhs.IsValue() && rhs.IsValue() {
		// Check whether the value got modified.
		beforeHandle, err := before.getReadAccess(context.source)
		if err != nil {
			return err
		}
		defer beforeHandle.Release()
		afterHandle, err := after.getReadAccess(context.source)
		if err != nil {
			return err
		}
		defer afterHandle.Release()

		beforeNode := beforeHandle.Get().(*ValueNode)
		afterNode := afterHandle.Get().(*ValueNode)

		if beforeNode.key == afterNode.key {
			if beforeNode.value != afterNode.value {
				recordValueUpdate(context, afterNode.key, afterNode.value)
			}
		} else {
			recordValueUpdate(context, beforeNode.key, common.Value{})
			recordValueUpdate(context, afterNode.key, afterNode.value)
		}
	}

	return nil
}

// --- Utilities ---

func recordAddedAccount(
	context *diffContext,
	account *AccountNode,
) error {
	// And a new account was created.
	diff := &AccountDiff{}
	if (account.info.Balance != common.Balance{}) {
		diff.Balance = new(common.Balance)
		*diff.Balance = account.info.Balance
	}
	if (account.info.Nonce != common.Nonce{}) {
		diff.Nonce = new(common.Nonce)
		*diff.Nonce = account.info.Nonce
	}
	if (account.info.CodeHash != common.Hash{}) {
		diff.Code = new(common.Hash)
		*diff.Code = account.info.CodeHash
	}
	if !diff.Empty() {
		context.result[account.address] = diff
	}
	context.currentAccount = &account.address
	return collectDiff(context, triePosition{ref: emptyNodeReference}, triePosition{ref: account.storage})
}

func recordDeletedAccount(
	context *diffContext,
	account *AccountNode,
) {
	context.result[account.address] = &AccountDiff{Reset: true}
}

func recordValueUpdate(
	context *diffContext,
	key common.Key,
	value common.Value,
) {
	diff := context.result[*context.currentAccount]
	if diff == nil {
		diff = &AccountDiff{}
		context.result[*context.currentAccount] = diff
	}
	if diff.Storage == nil {
		diff.Storage = map[common.Key]common.Value{}
	}
	diff.Storage[key] = value
}
