// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package witness

import (
	reflect "reflect"

	common "github.com/Fantom-foundation/Carmen/go/common"
	amount "github.com/Fantom-foundation/Carmen/go/common/amount"
	immutable "github.com/Fantom-foundation/Carmen/go/common/immutable"
	tribool "github.com/Fantom-foundation/Carmen/go/common/tribool"
	gomock "go.uber.org/mock/gomock"
)

// MockProof is a mock of Proof interface.
type MockProof struct {
	ctrl     *gomock.Controller
	recorder *MockProofMockRecorder
}

// MockProofMockRecorder is the mock recorder for MockProof.
type MockProofMockRecorder struct {
	mock *MockProof
}

// NewMockProof creates a new mock instance.
func NewMockProof(ctrl *gomock.Controller) *MockProof {
	mock := &MockProof{ctrl: ctrl}
	mock.recorder = &MockProofMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProof) EXPECT() *MockProofMockRecorder {
	return m.recorder
}

// AllAddressesEmpty mocks base method.
func (m *MockProof) AllAddressesEmpty(root common.Hash, from, to common.Address) (tribool.Tribool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllAddressesEmpty", root, from, to)
	ret0, _ := ret[0].(tribool.Tribool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllAddressesEmpty indicates an expected call of AllAddressesEmpty.
func (mr *MockProofMockRecorder) AllAddressesEmpty(root, from, to any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllAddressesEmpty", reflect.TypeOf((*MockProof)(nil).AllAddressesEmpty), root, from, to)
}

// AllStatesZero mocks base method.
func (m *MockProof) AllStatesZero(root common.Hash, address common.Address, from, to common.Key) (tribool.Tribool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllStatesZero", root, address, from, to)
	ret0, _ := ret[0].(tribool.Tribool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllStatesZero indicates an expected call of AllStatesZero.
func (mr *MockProofMockRecorder) AllStatesZero(root, address, from, to any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllStatesZero", reflect.TypeOf((*MockProof)(nil).AllStatesZero), root, address, from, to)
}

// Extract mocks base method.
func (m *MockProof) Extract(root common.Hash, address common.Address, keys ...common.Key) (Proof, bool) {
	m.ctrl.T.Helper()
	varargs := []any{root, address}
	for _, a := range keys {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Extract", varargs...)
	ret0, _ := ret[0].(Proof)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Extract indicates an expected call of Extract.
func (mr *MockProofMockRecorder) Extract(root, address any, keys ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{root, address}, keys...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Extract", reflect.TypeOf((*MockProof)(nil).Extract), varargs...)
}

// GetBalance mocks base method.
func (m *MockProof) GetBalance(root common.Hash, address common.Address) (amount.Amount, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", root, address)
	ret0, _ := ret[0].(amount.Amount)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockProofMockRecorder) GetBalance(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalance", reflect.TypeOf((*MockProof)(nil).GetBalance), root, address)
}

// GetCodeHash mocks base method.
func (m *MockProof) GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeHash", root, address)
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetCodeHash indicates an expected call of GetCodeHash.
func (mr *MockProofMockRecorder) GetCodeHash(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeHash", reflect.TypeOf((*MockProof)(nil).GetCodeHash), root, address)
}

// GetElements mocks base method.
func (m *MockProof) GetElements() []immutable.Bytes {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetElements")
	ret0, _ := ret[0].([]immutable.Bytes)
	return ret0
}

// GetElements indicates an expected call of GetElements.
func (mr *MockProofMockRecorder) GetElements() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetElements", reflect.TypeOf((*MockProof)(nil).GetElements))
}

// GetNonce mocks base method.
func (m *MockProof) GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", root, address)
	ret0, _ := ret[0].(common.Nonce)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockProofMockRecorder) GetNonce(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockProof)(nil).GetNonce), root, address)
}

// GetState mocks base method.
func (m *MockProof) GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetState", root, address, key)
	ret0, _ := ret[0].(common.Value)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetState indicates an expected call of GetState.
func (mr *MockProofMockRecorder) GetState(root, address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetState", reflect.TypeOf((*MockProof)(nil).GetState), root, address, key)
}

// GetStorageElements mocks base method.
func (m *MockProof) GetStorageElements(root common.Hash, address common.Address, keys ...common.Key) ([]immutable.Bytes, common.Hash, bool) {
	m.ctrl.T.Helper()
	varargs := []any{root, address}
	for _, a := range keys {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetStorageElements", varargs...)
	ret0, _ := ret[0].([]immutable.Bytes)
	ret1, _ := ret[1].(common.Hash)
	ret2, _ := ret[2].(bool)
	return ret0, ret1, ret2
}

// GetStorageElements indicates an expected call of GetStorageElements.
func (mr *MockProofMockRecorder) GetStorageElements(root, address any, keys ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{root, address}, keys...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorageElements", reflect.TypeOf((*MockProof)(nil).GetStorageElements), varargs...)
}

// IsValid mocks base method.
func (m *MockProof) IsValid() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsValid")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsValid indicates an expected call of IsValid.
func (mr *MockProofMockRecorder) IsValid() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsValid", reflect.TypeOf((*MockProof)(nil).IsValid))
}
