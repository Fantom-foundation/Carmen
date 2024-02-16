package mpt

import (
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type Diff map[common.Address]*AccountDiff

type AccountDiff struct {
	Reset   bool
	Balance *common.Balance
	Nonce   *common.Nonce
	Code    *common.Hash
	Storage map[common.Key]common.Value
}

func GetDiff(
	forest *Forest,
	before *NodeReference,
	after *NodeReference,
) (Diff, error) {
	context := &diffContext{
		forest: forest,
	}
	if err := collectDiff(context, before, after); err != nil {
		return nil, err
	}
	return context.result, nil
}

type diffContext struct {
	forest         *Forest
	currentAccount *common.Address
	result         Diff
}

func collectDiff(
	context *diffContext,
	before *NodeReference,
	after *NodeReference,
) error {
	if before.Id() == after.Id() {
		return nil
	}

	beforeHandle, err := context.forest.getReadAccess(before)
	if err != nil {
		return err
	}
	defer beforeHandle.Release()

	afterHandle, err := context.forest.getReadAccess(after)
	if err != nil {
		return err
	}
	defer afterHandle.Release()

	beforeId := before.Id()
	afterId := after.Id()

	beforeNode := beforeHandle.Get()
	afterNode := afterHandle.Get()

	if beforeId.IsAccount() {
		lhs := beforeNode.(*AccountNode)
		if afterId.IsAccount() {
			return diffAccountWithAccount(context, lhs, afterNode.(*AccountNode))
		} else if afterId.IsBranch() {
			return diffAccountWithBranch(context, lhs, afterNode.(*BranchNode))
		} else if afterId.IsEmpty() {
			return diffAccountWithEmpty(context, lhs, afterNode.(EmptyNode))
		} else if afterId.IsExtension() {
			return diffAccountWithExtension(context, lhs, afterNode.(*ExtensionNode))
		} else {
			return diffAccountWithValue(context, lhs, afterNode.(*ValueNode))
		}
	} else if beforeId.IsBranch() {
		lhs := beforeNode.(*BranchNode)
		if afterId.IsAccount() {
			return diffBranchWithAccount(context, lhs, afterNode.(*AccountNode))
		} else if afterId.IsBranch() {
			return diffBranchWithBranch(context, lhs, afterNode.(*BranchNode))
		} else if afterId.IsEmpty() {
			return diffBranchWithEmpty(context, lhs, afterNode.(EmptyNode))
		} else if afterId.IsExtension() {
			return diffBranchWithExtension(context, lhs, afterNode.(*ExtensionNode))
		} else {
			return diffBranchWithValue(context, lhs, afterNode.(*ValueNode))
		}
	} else if beforeId.IsEmpty() {
		lhs := EmptyNode{}
		if afterId.IsAccount() {
			return diffEmptyWithAccount(context, lhs, afterNode.(*AccountNode))
		} else if afterId.IsBranch() {
			return diffEmptyWithBranch(context, lhs, afterNode.(*BranchNode))
		} else if afterId.IsEmpty() {
			return diffEmptyWithEmpty(context, lhs, afterNode.(EmptyNode))
		} else if afterId.IsExtension() {
			return diffEmptyWithExtension(context, lhs, afterNode.(*ExtensionNode))
		} else {
			return diffEmptyWithValue(context, lhs, afterNode.(*ValueNode))
		}
	} else if beforeId.IsExtension() {
		lhs := beforeNode.(*ExtensionNode)
		if afterId.IsAccount() {
			return diffExtensionWithAccount(context, lhs, afterNode.(*AccountNode))
		} else if afterId.IsBranch() {
			return diffExtensionWithBranch(context, lhs, afterNode.(*BranchNode))
		} else if afterId.IsEmpty() {
			return diffExtensionWithEmpty(context, lhs, afterNode.(EmptyNode))
		} else if afterId.IsExtension() {
			return diffExtensionWithExtension(context, lhs, afterNode.(*ExtensionNode))
		} else {
			return diffExtensionWithValue(context, lhs, afterNode.(*ValueNode))
		}
	} else {
		lhs := beforeNode.(*ValueNode)
		if afterId.IsAccount() {
			return diffValueWithAccount(context, lhs, afterNode.(*AccountNode))
		} else if afterId.IsBranch() {
			return diffValueWithBranch(context, lhs, afterNode.(*BranchNode))
		} else if afterId.IsEmpty() {
			return diffValueWithEmpty(context, lhs, afterNode.(EmptyNode))
		} else if afterId.IsExtension() {
			return diffValueWithExtension(context, lhs, afterNode.(*ExtensionNode))
		} else {
			return diffValueWithValue(context, lhs, afterNode.(*ValueNode))
		}
	}
}

// --- Accounts and X ---

func diffAccountWithAccount(
	context *diffContext,
	before *AccountNode,
	after *AccountNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffAccountWithBranch(
	context *diffContext,
	before *AccountNode,
	after *BranchNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffAccountWithEmpty(
	context *diffContext,
	before *AccountNode,
	after EmptyNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffAccountWithExtension(
	context *diffContext,
	before *AccountNode,
	after *ExtensionNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffAccountWithValue(
	context *diffContext,
	before *AccountNode,
	after *ValueNode,
) error {
	return fmt.Errorf("not implemented")
}

// --- Branch and X ---

func diffBranchWithAccount(
	context *diffContext,
	before *BranchNode,
	after *AccountNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffBranchWithBranch(
	context *diffContext,
	before *BranchNode,
	after *BranchNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffBranchWithEmpty(
	context *diffContext,
	before *BranchNode,
	after EmptyNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffBranchWithExtension(
	context *diffContext,
	before *BranchNode,
	after *ExtensionNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffBranchWithValue(
	context *diffContext,
	before *BranchNode,
	after *ValueNode,
) error {
	return fmt.Errorf("not implemented")
}

// --- Empty and X ---

func diffEmptyWithAccount(
	context *diffContext,
	before EmptyNode,
	after *AccountNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffEmptyWithBranch(
	context *diffContext,
	before EmptyNode,
	after *BranchNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffEmptyWithEmpty(
	context *diffContext,
	before EmptyNode,
	after EmptyNode,
) error {
	return nil // no difference
}

func diffEmptyWithExtension(
	context *diffContext,
	before EmptyNode,
	after *ExtensionNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffEmptyWithValue(
	context *diffContext,
	before EmptyNode,
	after *ValueNode,
) error {
	return fmt.Errorf("not implemented")
}

// --- Extension and X ---

func diffExtensionWithAccount(
	context *diffContext,
	before *ExtensionNode,
	after *AccountNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffExtensionWithBranch(
	context *diffContext,
	before *ExtensionNode,
	after *BranchNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffExtensionWithEmpty(
	context *diffContext,
	before *ExtensionNode,
	after EmptyNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffExtensionWithExtension(
	context *diffContext,
	before *ExtensionNode,
	after *ExtensionNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffExtensionWithValue(
	context *diffContext,
	before *ExtensionNode,
	after *ValueNode,
) error {
	return fmt.Errorf("not implemented")
}

// --- Value and X ---

func diffValueWithAccount(
	context *diffContext,
	before *ValueNode,
	after *AccountNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffValueWithBranch(
	context *diffContext,
	before *ValueNode,
	after *BranchNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffValueWithEmpty(
	context *diffContext,
	before *ValueNode,
	after EmptyNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffValueWithExtension(
	context *diffContext,
	before *ValueNode,
	after *ExtensionNode,
) error {
	return fmt.Errorf("not implemented")
}

func diffValueWithValue(
	context *diffContext,
	before *ValueNode,
	after *ValueNode,
) error {
	return fmt.Errorf("not implemented")
}
