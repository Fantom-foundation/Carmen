// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/holiman/uint256"
	"go.uber.org/mock/gomock"
)

func TestQueryContext_QueriesAreForwarded(t *testing.T) {
	properties := map[string]struct {
		setup func(*state.MockState)
		check func(query *queryContext, t *testing.T)
	}{
		"balance": {
			func(mock *state.MockState) {
				balance, _ := common.ToBalance(big.NewInt(12))
				mock.EXPECT().GetBalance(common.Address{2}).Return(balance, nil)
			},
			func(query *queryContext, t *testing.T) {
				want := uint256.NewInt(12)
				if got := query.GetBalance(Address{2}); want.Cmp(&got) != 0 {
					t.Errorf("unexpected balance, wanted %v, got %v", want, got)
				}
			},
		},
		"nonce": {
			func(mock *state.MockState) {
				mock.EXPECT().GetNonce(common.Address{5}).Return(common.ToNonce(12), nil)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := uint64(12), query.GetNonce(Address{5}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
			},
		},
		"storage": {
			func(mock *state.MockState) {
				mock.EXPECT().GetStorage(common.Address{5}, common.Key{7}).Return(common.Value{12}, nil)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Value{12}), query.GetState(Address{5}, Key{7}); want != got {
					t.Errorf("unexpected storage value, wanted %v, got %v", want, got)
				}
			},
		},
		"code": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCode(common.Address{1}).Return([]byte{1, 2, 3}, nil)
			},
			func(query *queryContext, t *testing.T) {
				want := []byte{1, 2, 3}
				if got := query.GetCode(Address{1}); !bytes.Equal(want, got) {
					t.Errorf("unexpected code, wanted %v, got %v", want, got)
				}
			},
		},
		"code-size": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCodeSize(common.Address{1}).Return(15, nil)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := 15, query.GetCodeSize(Address{1}); want != got {
					t.Errorf("unexpected code size, wanted %v, got %v", want, got)
				}
			},
		},
		"code-hash": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCodeHash(common.Address{1}).Return(common.Hash{1, 2, 3}, nil)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Hash{1, 2, 3}), query.GetCodeHash(Address{1}); want != got {
					t.Errorf("unexpected code hash, wanted %v, got %v", want, got)
				}
			},
		},
		"state-hash": {
			func(mock *state.MockState) {
				mock.EXPECT().GetHash().Return(common.Hash{1, 2, 3}, nil)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Hash{1, 2, 3}), query.GetStateHash(); want != got {
					t.Errorf("unexpected state hash, wanted %v, got %v", want, got)
				}
			},
		},
	}
	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			state := state.NewMockState(ctrl)
			property.setup(state)
			query := &queryContext{state: state}
			property.check(query, t)
		})
	}
}

func TestQueryContext_ErrorsArePropagated(t *testing.T) {
	injectedError := fmt.Errorf("injectedError")
	properties := map[string]struct {
		setup func(*state.MockState)
		check func(query *queryContext, t *testing.T)
	}{
		"balance": {
			func(mock *state.MockState) {
				balance, _ := common.ToBalance(big.NewInt(12))
				mock.EXPECT().GetBalance(common.Address{2}).Return(balance, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				want := uint256.NewInt(0)
				if got := query.GetBalance(Address{2}); want.Cmp(&got) != 0 {
					t.Errorf("unexpected balance, wanted %v, got %v", want, got)
				}
			},
		},
		"nonce": {
			func(mock *state.MockState) {
				mock.EXPECT().GetNonce(common.Address{5}).Return(common.ToNonce(12), injectedError)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := uint64(0), query.GetNonce(Address{5}); want != got {
					t.Errorf("unexpected nonce, wanted %v, got %v", want, got)
				}
			},
		},
		"storage": {
			func(mock *state.MockState) {
				mock.EXPECT().GetStorage(common.Address{5}, common.Key{7}).Return(common.Value{12}, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Value{}), query.GetState(Address{5}, Key{7}); want != got {
					t.Errorf("unexpected storage value, wanted %v, got %v", want, got)
				}
			},
		},
		"code": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCode(common.Address{1}).Return([]byte{1, 2, 3}, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				var want []byte
				if got := query.GetCode(Address{1}); !bytes.Equal(want, got) {
					t.Errorf("unexpected code, wanted %v, got %v", want, got)
				}
			},
		},
		"code-size": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCodeSize(common.Address{1}).Return(15, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := 0, query.GetCodeSize(Address{1}); want != got {
					t.Errorf("unexpected code size, wanted %v, got %v", want, got)
				}
			},
		},
		"code-hash": {
			func(mock *state.MockState) {
				mock.EXPECT().GetCodeHash(common.Address{1}).Return(common.Hash{1, 2, 3}, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Hash{}), query.GetCodeHash(Address{1}); want != got {
					t.Errorf("unexpected code hash, wanted %v, got %v", want, got)
				}
			},
		},
		"state-hash": {
			func(mock *state.MockState) {
				mock.EXPECT().GetHash().Return(common.Hash{1, 2, 3}, injectedError)
			},
			func(query *queryContext, t *testing.T) {
				if want, got := (Hash{}), query.GetStateHash(); want != got {
					t.Errorf("unexpected state hash, wanted %v, got %v", want, got)
				}
			},
		},
	}
	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			state := state.NewMockState(ctrl)
			state.EXPECT().Check().AnyTimes()
			property.setup(state)
			query := &queryContext{state: state}

			if err := query.Check(); err != nil {
				t.Fatalf("initially, there should be no error")
			}

			// The first time the error should be triggered.
			property.check(query, t)
			if query.err != injectedError {
				t.Errorf("recorded unexpected error, wanted %v, got %v", injectedError, query.err)
			}
			if want, got := injectedError, query.Check(); !errors.Is(got, want) {
				t.Errorf("unexpected error, wanted %v, got %v", want, got)
			}

			// The second time, no call should happen (mocks only expect one call).
			property.check(query, t)
			if query.err != injectedError {
				t.Errorf("recorded unexpected error, wanted %v, got %v", injectedError, query.err)
			}
			if want, got := injectedError, query.Check(); !errors.Is(got, want) {
				t.Errorf("unexpected error, wanted %v, got %v", want, got)
			}
		})
	}
}
