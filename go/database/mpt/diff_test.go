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
	"errors"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
)

// TODO [cleanup]:
//  - rewrite to use a node source instead of an archive (using node test infrastructure)

type diffScenario struct {
	before NodeDesc
	after  NodeDesc
	diff   Diff
	err    error
}

func getDiffScenarios() map[string]diffScenario {

	res := map[string]diffScenario{}

	// -- before is empty --

	res["empty_vs_empty"] = diffScenario{
		before: &Empty{},
		after:  &Empty{},
		diff:   Diff{},
	}

	res["empty_vs_account"] = diffScenario{
		before: &Empty{},
		after: &Account{
			address: common.Address{1, 2, 3},
			info: AccountInfo{
				Balance:  common.Balance{4, 5},
				Nonce:    common.Nonce{6, 7},
				CodeHash: common.Hash{8, 9},
			},
		},
		diff: Diff{
			common.Address{1, 2, 3}: &AccountDiff{
				Balance: &common.Balance{4, 5},
				Nonce:   &common.Nonce{6, 7},
				Code:    &common.Hash{8, 9},
			},
		},
	}

	res["empty_vs_branch"] = diffScenario{
		before: &Empty{},
		after: &Branch{children: Children{
			2: &Account{address: common.Address{0x27}, info: AccountInfo{Nonce: common.Nonce{18}}},
			4: &Account{address: common.Address{0x41}, info: AccountInfo{Nonce: common.Nonce{12}}},
		}},
		diff: Diff{
			common.Address{0x27}: &AccountDiff{Nonce: &common.Nonce{18}},
			common.Address{0x41}: &AccountDiff{Nonce: &common.Nonce{12}},
		},
	}

	res["empty_vs_extension"] = diffScenario{
		before: &Empty{},
		after: &Extension{path: []Nibble{5, 2}, next: &Branch{children: Children{
			2: &Account{address: common.Address{0x52, 0x27}, info: AccountInfo{Nonce: common.Nonce{18}}},
			4: &Account{address: common.Address{0x52, 0x41}, info: AccountInfo{Nonce: common.Nonce{12}}},
		}}},
		diff: Diff{
			common.Address{0x52, 0x27}: &AccountDiff{Nonce: &common.Nonce{18}},
			common.Address{0x52, 0x41}: &AccountDiff{Nonce: &common.Nonce{12}},
		},
	}

	res["empty_vs_value"] = diffScenario{
		before: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
		},
		after: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x34}},
		},
		diff: Diff{
			common.Address{0x52, 0x27}: &AccountDiff{
				Storage: map[common.Key]common.Value{{0x12}: {0x34}},
			},
		},
	}

	// -- before is an account --

	res["account_vs_empty"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{18}},
		},
		after: &Empty{},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{
				Reset: true,
			},
		},
	}

	res["account_vs_account"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info: AccountInfo{
				Balance:  common.Balance{1, 2},
				Nonce:    common.Nonce{3, 4},
				CodeHash: common.Hash{5, 6},
			},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x34}},
		},
		after: &Account{
			address: common.Address{0x12},
			info: AccountInfo{
				Balance:  common.Balance{11, 21},
				Nonce:    common.Nonce{32, 42},
				CodeHash: common.Hash{53, 63},
			},
			storage: &Value{key: common.Key{0x21}, value: common.Value{0x34, 0x5}},
		},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{
				Reset:   false,
				Balance: &common.Balance{11, 21},
				Nonce:   &common.Nonce{32, 42},
				Code:    &common.Hash{53, 63},
				Storage: map[common.Key]common.Value{
					{0x12}: {},
					{0x21}: {0x34, 0x5},
				},
			},
		},
	}

	res["account_vs_different_account"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info: AccountInfo{
				Balance:  common.Balance{1, 2},
				Nonce:    common.Nonce{3, 4},
				CodeHash: common.Hash{5, 6},
			},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x34}},
		},
		after: &Account{
			address: common.Address{0x14},
			info: AccountInfo{
				Balance:  common.Balance{11, 21},
				Nonce:    common.Nonce{32, 42},
				CodeHash: common.Hash{53, 63},
			},
			storage: &Value{key: common.Key{0x21}, value: common.Value{0x34, 0x5}},
		},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{
				Reset: true,
			},
			common.Address{0x14}: &AccountDiff{
				Reset:   false,
				Balance: &common.Balance{11, 21},
				Nonce:   &common.Nonce{32, 42},
				Code:    &common.Hash{53, 63},
				Storage: map[common.Key]common.Value{
					{0x21}: {0x34, 0x5},
				},
			},
		},
	}

	res["account_vs_branch"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{0x18}},
		},
		after: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}},
		diff: Diff{
			common.Address{0x27}: &AccountDiff{
				Nonce: &common.Nonce{0x22},
			},
		},
	}

	res["account_vs_extension"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{0x18}},
		},
		after: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			2: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			7: &Account{
				address: common.Address{0x17},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}}},
		diff: Diff{
			common.Address{0x17}: &AccountDiff{
				Nonce: &common.Nonce{0x22},
			},
		},
	}

	// -- before is a branch node --

	res["branch_vs_empty"] = diffScenario{
		before: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}},
		after: &Empty{},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{Reset: true},
			common.Address{0x27}: &AccountDiff{Reset: true},
		},
	}

	res["branch_vs_account"] = diffScenario{
		before: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}},
		after: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{0x18}},
		},
		diff: Diff{
			common.Address{0x27}: &AccountDiff{Reset: true},
		},
	}

	res["branch_vs_branch"] = diffScenario{
		before: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
			3: &Account{
				address: common.Address{0x31},
				info:    AccountInfo{Nonce: common.Nonce{0x24}},
			},
		}},
		after: &Branch{children: Children{
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
			3: &Account{
				address: common.Address{0x31},
				info:    AccountInfo{Nonce: common.Nonce{0x42}},
			},
			9: &Account{
				address: common.Address{0x94},
				info:    AccountInfo{Nonce: common.Nonce{0x15}},
			},
		}},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{Reset: true},
			common.Address{0x31}: &AccountDiff{
				Nonce: &common.Nonce{0x42},
			},
			common.Address{0x94}: &AccountDiff{
				Nonce: &common.Nonce{0x15},
			},
		},
	}

	res["branch_vs_extension"] = diffScenario{
		before: &Branch{children: Children{
			1: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			2: &Account{
				address: common.Address{0x27},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}},
		after: &Extension{path: []Nibble{1}, next: &Branch{children: Children{
			2: &Account{
				address: common.Address{0x12},
				info:    AccountInfo{Nonce: common.Nonce{0x18}},
			},
			7: &Account{
				address: common.Address{0x17},
				info:    AccountInfo{Nonce: common.Nonce{0x22}},
			},
		}}},
		diff: Diff{
			common.Address{0x17}: &AccountDiff{
				Nonce: &common.Nonce{0x22},
			},
			common.Address{0x27}: &AccountDiff{Reset: true},
		},
	}

	res["branch_vs_value"] = diffScenario{
		before: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{0x18}},
			storage: &Branch{children: Children{
				3: &Value{key: common.Key{0x34}, value: common.Value{0x23}},
				7: &Value{key: common.Key{0x71}, value: common.Value{0x56}},
			}},
		},
		after: &Account{
			address: common.Address{0x12},
			info:    AccountInfo{Nonce: common.Nonce{0x18}},
			storage: &Value{key: common.Key{0x34}, value: common.Value{0x23}},
		},
		diff: Diff{
			common.Address{0x12}: &AccountDiff{
				Storage: map[common.Key]common.Value{{0x71}: {}},
			},
		},
	}

	res["value_vs_value_different_value"] = diffScenario{
		before: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x34}},
		},
		after: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x42}},
		},
		diff: Diff{
			common.Address{0x52, 0x27}: &AccountDiff{
				Storage: map[common.Key]common.Value{
					{0x12}: {0x42},
				},
			},
		},
	}

	res["value_vs_value_different_key"] = diffScenario{
		before: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
			storage: &Value{key: common.Key{0x12}, value: common.Value{0x34}},
		},
		after: &Account{
			address: common.Address{0x52, 0x27},
			info:    AccountInfo{Nonce: common.Nonce{18}},
			storage: &Value{key: common.Key{0x14}, value: common.Value{0x42}},
		},
		diff: Diff{
			common.Address{0x52, 0x27}: &AccountDiff{
				Storage: map[common.Key]common.Value{
					{0x12}: {},
					{0x14}: {0x42},
				},
			},
		},
	}

	return res
}

func TestDiff_DiffScenariosAreValidTrees(t *testing.T) {
	for name, test := range getDiffScenarios() {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			before, _ := ctxt.Build(test.before)
			after, _ := ctxt.Build(test.after)

			ctxt.Check(t, before)
			ctxt.Check(t, after)
		})
	}
}

func TestDiff_DiffScenariosProduceCorrectDiff(t *testing.T) {
	for name, test := range getDiffScenarios() {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			before, _ := ctxt.Build(test.before)
			after, _ := ctxt.Build(test.after)

			res, err := GetDiff(ctxt, &before, &after)
			if err != test.err {
				t.Errorf("unexpected error for diffing \n%s\n%s\nerror: %v", ctxt.Print(before), ctxt.Print(after), err)
			} else if !res.Equal(test.diff) {
				t.Errorf("unexpected result for diffing \n%s\n%s\nwanted: %s\ngot: %s", ctxt.Print(before), ctxt.Print(after), test.diff, res)
			}
		})
	}
}

func TestDiff_ErrorsArePropagated(t *testing.T) {
	for name, test := range getDiffScenarios() {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			countingContext := NewMockNodeSource(ctrl)

			numCalls := 0
			countingContext.EXPECT().getReadAccess(gomock.Any()).AnyTimes().DoAndReturn(func(ref *NodeReference) (shared.ReadHandle[Node], error) {
				numCalls++
				return ctxt.getReadAccess(ref)
			})
			countingContext.EXPECT().getConfig().AnyTimes().Return(ctxt.getConfig())

			before, _ := ctxt.Build(test.before)
			after, _ := ctxt.Build(test.after)

			_, err := GetDiff(countingContext, &before, &after)
			if err != nil {
				t.Fatalf("failed to count node source calls: %v", err)
			}

			injectedError := fmt.Errorf("injected error")
			for i := 0; i < numCalls; i++ {
				calls := 0
				injectionContext := NewMockNodeSource(ctrl)
				injectionContext.EXPECT().getReadAccess(gomock.Any()).AnyTimes().DoAndReturn(func(ref *NodeReference) (shared.ReadHandle[Node], error) {
					if calls < i {
						calls++
						return ctxt.getReadAccess(ref)
					}
					return shared.ReadHandle[Node]{}, injectedError
				})
				injectionContext.EXPECT().getConfig().AnyTimes().Return(ctxt.getConfig())

				_, err := GetDiff(injectionContext, &before, &after)
				if !errors.Is(err, injectedError) {
					t.Errorf("missing expected error injected at position %d of %d, wanted %v, got %v", i, numCalls, injectedError, err)
				}
			}

		})
	}
}

func TestDiff_DiffsCanBePrinted(t *testing.T) {
	for name, test := range getDiffScenarios() {
		t.Run(name, func(t *testing.T) {
			str := test.diff.String()
			if len(str) == 0 {
				t.Errorf("Failed to print expected difference")
			}
		})
	}
}
