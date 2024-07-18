// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

// Code generated by MockGen. DO NOT EDIT.
// Source: state.go
//
// Generated by this command:
//
//	mockgen -source state.go -destination state_mocks.go -package mpt
//

// Package mpt is a generated GoMock package.
package mpt

import (
	reflect "reflect"

	backend "github.com/Fantom-foundation/Carmen/go/backend"
	common "github.com/Fantom-foundation/Carmen/go/common"
	amount "github.com/Fantom-foundation/Carmen/go/common/amount"
	gomock "go.uber.org/mock/gomock"
)

// MockDatabase is a mock of Database interface.
type MockDatabase struct {
	ctrl     *gomock.Controller
	recorder *MockDatabaseMockRecorder
}

// MockDatabaseMockRecorder is the mock recorder for MockDatabase.
type MockDatabaseMockRecorder struct {
	mock *MockDatabase
}

// NewMockDatabase creates a new mock instance.
func NewMockDatabase(ctrl *gomock.Controller) *MockDatabase {
	mock := &MockDatabase{ctrl: ctrl}
	mock.recorder = &MockDatabaseMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDatabase) EXPECT() *MockDatabaseMockRecorder {
	return m.recorder
}

// Check mocks base method.
func (m *MockDatabase) Check(rootRef *NodeReference) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Check", rootRef)
	ret0, _ := ret[0].(error)
	return ret0
}

// Check indicates an expected call of Check.
func (mr *MockDatabaseMockRecorder) Check(rootRef any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Check", reflect.TypeOf((*MockDatabase)(nil).Check), rootRef)
}

// CheckAll mocks base method.
func (m *MockDatabase) CheckAll(rootRefs []*NodeReference) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckAll", rootRefs)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckAll indicates an expected call of CheckAll.
func (mr *MockDatabaseMockRecorder) CheckAll(rootRefs any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckAll", reflect.TypeOf((*MockDatabase)(nil).CheckAll), rootRefs)
}

// CheckErrors mocks base method.
func (m *MockDatabase) CheckErrors() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckErrors")
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckErrors indicates an expected call of CheckErrors.
func (mr *MockDatabaseMockRecorder) CheckErrors() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckErrors", reflect.TypeOf((*MockDatabase)(nil).CheckErrors))
}

// ClearStorage mocks base method.
func (m *MockDatabase) ClearStorage(rootRef *NodeReference, addr common.Address) (NodeReference, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ClearStorage", rootRef, addr)
	ret0, _ := ret[0].(NodeReference)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ClearStorage indicates an expected call of ClearStorage.
func (mr *MockDatabaseMockRecorder) ClearStorage(rootRef, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClearStorage", reflect.TypeOf((*MockDatabase)(nil).ClearStorage), rootRef, addr)
}

// Close mocks base method.
func (m *MockDatabase) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockDatabaseMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDatabase)(nil).Close))
}

// Dump mocks base method.
func (m *MockDatabase) Dump(rootRef *NodeReference) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Dump", rootRef)
}

// Dump indicates an expected call of Dump.
func (mr *MockDatabaseMockRecorder) Dump(rootRef any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Dump", reflect.TypeOf((*MockDatabase)(nil).Dump), rootRef)
}

// Flush mocks base method.
func (m *MockDatabase) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockDatabaseMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockDatabase)(nil).Flush))
}

// Freeze mocks base method.
func (m *MockDatabase) Freeze(ref *NodeReference) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Freeze", ref)
	ret0, _ := ret[0].(error)
	return ret0
}

// Freeze indicates an expected call of Freeze.
func (mr *MockDatabaseMockRecorder) Freeze(ref any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Freeze", reflect.TypeOf((*MockDatabase)(nil).Freeze), ref)
}

// GetAccountInfo mocks base method.
func (m *MockDatabase) GetAccountInfo(rootRef *NodeReference, addr common.Address) (AccountInfo, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccountInfo", rootRef, addr)
	ret0, _ := ret[0].(AccountInfo)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetAccountInfo indicates an expected call of GetAccountInfo.
func (mr *MockDatabaseMockRecorder) GetAccountInfo(rootRef, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccountInfo", reflect.TypeOf((*MockDatabase)(nil).GetAccountInfo), rootRef, addr)
}

// GetMemoryFootprint mocks base method.
func (m *MockDatabase) GetMemoryFootprint() *common.MemoryFootprint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMemoryFootprint")
	ret0, _ := ret[0].(*common.MemoryFootprint)
	return ret0
}

// GetMemoryFootprint indicates an expected call of GetMemoryFootprint.
func (mr *MockDatabaseMockRecorder) GetMemoryFootprint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMemoryFootprint", reflect.TypeOf((*MockDatabase)(nil).GetMemoryFootprint))
}

// GetValue mocks base method.
func (m *MockDatabase) GetValue(rootRef *NodeReference, addr common.Address, key common.Key) (common.Value, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetValue", rootRef, addr, key)
	ret0, _ := ret[0].(common.Value)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetValue indicates an expected call of GetValue.
func (mr *MockDatabaseMockRecorder) GetValue(rootRef, addr, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetValue", reflect.TypeOf((*MockDatabase)(nil).GetValue), rootRef, addr, key)
}

// HasEmptyStorage mocks base method.
func (m *MockDatabase) HasEmptyStorage(rootRef *NodeReference, addr common.Address) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasEmptyStorage", rootRef, addr)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasEmptyStorage indicates an expected call of HasEmptyStorage.
func (mr *MockDatabaseMockRecorder) HasEmptyStorage(rootRef, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasEmptyStorage", reflect.TypeOf((*MockDatabase)(nil).HasEmptyStorage), rootRef, addr)
}

// SetAccountInfo mocks base method.
func (m *MockDatabase) SetAccountInfo(rootRef *NodeReference, addr common.Address, info AccountInfo) (NodeReference, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetAccountInfo", rootRef, addr, info)
	ret0, _ := ret[0].(NodeReference)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetAccountInfo indicates an expected call of SetAccountInfo.
func (mr *MockDatabaseMockRecorder) SetAccountInfo(rootRef, addr, info any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetAccountInfo", reflect.TypeOf((*MockDatabase)(nil).SetAccountInfo), rootRef, addr, info)
}

// SetValue mocks base method.
func (m *MockDatabase) SetValue(rootRef *NodeReference, addr common.Address, key common.Key, value common.Value) (NodeReference, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetValue", rootRef, addr, key, value)
	ret0, _ := ret[0].(NodeReference)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetValue indicates an expected call of SetValue.
func (mr *MockDatabaseMockRecorder) SetValue(rootRef, addr, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetValue", reflect.TypeOf((*MockDatabase)(nil).SetValue), rootRef, addr, key, value)
}

// VisitTrie mocks base method.
func (m *MockDatabase) VisitTrie(rootRef *NodeReference, visitor NodeVisitor) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "VisitTrie", rootRef, visitor)
	ret0, _ := ret[0].(error)
	return ret0
}

// VisitTrie indicates an expected call of VisitTrie.
func (mr *MockDatabaseMockRecorder) VisitTrie(rootRef, visitor any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "VisitTrie", reflect.TypeOf((*MockDatabase)(nil).VisitTrie), rootRef, visitor)
}

// setHashesFor mocks base method.
func (m *MockDatabase) setHashesFor(root *NodeReference, hashes *NodeHashes) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "setHashesFor", root, hashes)
	ret0, _ := ret[0].(error)
	return ret0
}

// setHashesFor indicates an expected call of setHashesFor.
func (mr *MockDatabaseMockRecorder) setHashesFor(root, hashes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "setHashesFor", reflect.TypeOf((*MockDatabase)(nil).setHashesFor), root, hashes)
}

// updateHashesFor mocks base method.
func (m *MockDatabase) updateHashesFor(ref *NodeReference) (common.Hash, *NodeHashes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateHashesFor", ref)
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(*NodeHashes)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// updateHashesFor indicates an expected call of updateHashesFor.
func (mr *MockDatabaseMockRecorder) updateHashesFor(ref any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateHashesFor", reflect.TypeOf((*MockDatabase)(nil).updateHashesFor), ref)
}

// MockLiveState is a mock of LiveState interface.
type MockLiveState struct {
	ctrl     *gomock.Controller
	recorder *MockLiveStateMockRecorder
}

// MockLiveStateMockRecorder is the mock recorder for MockLiveState.
type MockLiveStateMockRecorder struct {
	mock *MockLiveState
}

// NewMockLiveState creates a new mock instance.
func NewMockLiveState(ctrl *gomock.Controller) *MockLiveState {
	mock := &MockLiveState{ctrl: ctrl}
	mock.recorder = &MockLiveStateMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLiveState) EXPECT() *MockLiveStateMockRecorder {
	return m.recorder
}

// Apply mocks base method.
func (m *MockLiveState) Apply(block uint64, update common.Update) (common.Releaser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Apply", block, update)
	ret0, _ := ret[0].(common.Releaser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Apply indicates an expected call of Apply.
func (mr *MockLiveStateMockRecorder) Apply(block, update any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Apply", reflect.TypeOf((*MockLiveState)(nil).Apply), block, update)
}

// Close mocks base method.
func (m *MockLiveState) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockLiveStateMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockLiveState)(nil).Close))
}

// CreateAccount mocks base method.
func (m *MockLiveState) CreateAccount(address common.Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateAccount indicates an expected call of CreateAccount.
func (mr *MockLiveStateMockRecorder) CreateAccount(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateAccount", reflect.TypeOf((*MockLiveState)(nil).CreateAccount), address)
}

// DeleteAccount mocks base method.
func (m *MockLiveState) DeleteAccount(address common.Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteAccount indicates an expected call of DeleteAccount.
func (mr *MockLiveStateMockRecorder) DeleteAccount(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccount", reflect.TypeOf((*MockLiveState)(nil).DeleteAccount), address)
}

// Exists mocks base method.
func (m *MockLiveState) Exists(address common.Address) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exists", address)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists.
func (mr *MockLiveStateMockRecorder) Exists(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockLiveState)(nil).Exists), address)
}

// Flush mocks base method.
func (m *MockLiveState) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockLiveStateMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockLiveState)(nil).Flush))
}

// GetBalance mocks base method.
func (m *MockLiveState) GetBalance(address common.Address) (amount.Amount, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", address)
	ret0, _ := ret[0].(amount.Amount)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockLiveStateMockRecorder) GetBalance(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalance", reflect.TypeOf((*MockLiveState)(nil).GetBalance), address)
}

// GetCode mocks base method.
func (m *MockLiveState) GetCode(address common.Address) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCode", address)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCode indicates an expected call of GetCode.
func (mr *MockLiveStateMockRecorder) GetCode(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCode", reflect.TypeOf((*MockLiveState)(nil).GetCode), address)
}

// GetCodeForHash mocks base method.
func (m *MockLiveState) GetCodeForHash(hash common.Hash) []byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeForHash", hash)
	ret0, _ := ret[0].([]byte)
	return ret0
}

// GetCodeForHash indicates an expected call of GetCodeForHash.
func (mr *MockLiveStateMockRecorder) GetCodeForHash(hash any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeForHash", reflect.TypeOf((*MockLiveState)(nil).GetCodeForHash), hash)
}

// GetCodeHash mocks base method.
func (m *MockLiveState) GetCodeHash(address common.Address) (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeHash", address)
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeHash indicates an expected call of GetCodeHash.
func (mr *MockLiveStateMockRecorder) GetCodeHash(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeHash", reflect.TypeOf((*MockLiveState)(nil).GetCodeHash), address)
}

// GetCodeSize mocks base method.
func (m *MockLiveState) GetCodeSize(address common.Address) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeSize", address)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeSize indicates an expected call of GetCodeSize.
func (mr *MockLiveStateMockRecorder) GetCodeSize(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeSize", reflect.TypeOf((*MockLiveState)(nil).GetCodeSize), address)
}

// GetCodes mocks base method.
func (m *MockLiveState) GetCodes() (map[common.Hash][]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodes")
	ret0, _ := ret[0].(map[common.Hash][]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodes indicates an expected call of GetCodes.
func (mr *MockLiveStateMockRecorder) GetCodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodes", reflect.TypeOf((*MockLiveState)(nil).GetCodes))
}

// GetHash mocks base method.
func (m *MockLiveState) GetHash() (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHash")
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHash indicates an expected call of GetHash.
func (mr *MockLiveStateMockRecorder) GetHash() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHash", reflect.TypeOf((*MockLiveState)(nil).GetHash))
}

// GetMemoryFootprint mocks base method.
func (m *MockLiveState) GetMemoryFootprint() *common.MemoryFootprint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMemoryFootprint")
	ret0, _ := ret[0].(*common.MemoryFootprint)
	return ret0
}

// GetMemoryFootprint indicates an expected call of GetMemoryFootprint.
func (mr *MockLiveStateMockRecorder) GetMemoryFootprint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMemoryFootprint", reflect.TypeOf((*MockLiveState)(nil).GetMemoryFootprint))
}

// GetNonce mocks base method.
func (m *MockLiveState) GetNonce(address common.Address) (common.Nonce, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", address)
	ret0, _ := ret[0].(common.Nonce)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockLiveStateMockRecorder) GetNonce(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockLiveState)(nil).GetNonce), address)
}

// GetSnapshotableComponents mocks base method.
func (m *MockLiveState) GetSnapshotableComponents() []backend.Snapshotable {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSnapshotableComponents")
	ret0, _ := ret[0].([]backend.Snapshotable)
	return ret0
}

// GetSnapshotableComponents indicates an expected call of GetSnapshotableComponents.
func (mr *MockLiveStateMockRecorder) GetSnapshotableComponents() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSnapshotableComponents", reflect.TypeOf((*MockLiveState)(nil).GetSnapshotableComponents))
}

// GetStorage mocks base method.
func (m *MockLiveState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorage", address, key)
	ret0, _ := ret[0].(common.Value)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorage indicates an expected call of GetStorage.
func (mr *MockLiveStateMockRecorder) GetStorage(address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorage", reflect.TypeOf((*MockLiveState)(nil).GetStorage), address, key)
}

// Root mocks base method.
func (m *MockLiveState) Root() NodeReference {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Root")
	ret0, _ := ret[0].(NodeReference)
	return ret0
}

// Root indicates an expected call of Root.
func (mr *MockLiveStateMockRecorder) Root() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Root", reflect.TypeOf((*MockLiveState)(nil).Root))
}

// RunPostRestoreTasks mocks base method.
func (m *MockLiveState) RunPostRestoreTasks() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunPostRestoreTasks")
	ret0, _ := ret[0].(error)
	return ret0
}

// RunPostRestoreTasks indicates an expected call of RunPostRestoreTasks.
func (mr *MockLiveStateMockRecorder) RunPostRestoreTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunPostRestoreTasks", reflect.TypeOf((*MockLiveState)(nil).RunPostRestoreTasks))
}

// SetBalance mocks base method.
func (m *MockLiveState) SetBalance(address common.Address, balance amount.Amount) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetBalance", address, balance)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetBalance indicates an expected call of SetBalance.
func (mr *MockLiveStateMockRecorder) SetBalance(address, balance any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBalance", reflect.TypeOf((*MockLiveState)(nil).SetBalance), address, balance)
}

// SetCode mocks base method.
func (m *MockLiveState) SetCode(address common.Address, code []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetCode", address, code)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetCode indicates an expected call of SetCode.
func (mr *MockLiveStateMockRecorder) SetCode(address, code any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetCode", reflect.TypeOf((*MockLiveState)(nil).SetCode), address, code)
}

// SetNonce mocks base method.
func (m *MockLiveState) SetNonce(address common.Address, nonce common.Nonce) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetNonce", address, nonce)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetNonce indicates an expected call of SetNonce.
func (mr *MockLiveStateMockRecorder) SetNonce(address, nonce any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetNonce", reflect.TypeOf((*MockLiveState)(nil).SetNonce), address, nonce)
}

// SetStorage mocks base method.
func (m *MockLiveState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetStorage", address, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetStorage indicates an expected call of SetStorage.
func (mr *MockLiveStateMockRecorder) SetStorage(address, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStorage", reflect.TypeOf((*MockLiveState)(nil).SetStorage), address, key, value)
}

// UpdateHashes mocks base method.
func (m *MockLiveState) UpdateHashes() (common.Hash, *NodeHashes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateHashes")
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(*NodeHashes)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// UpdateHashes indicates an expected call of UpdateHashes.
func (mr *MockLiveStateMockRecorder) UpdateHashes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateHashes", reflect.TypeOf((*MockLiveState)(nil).UpdateHashes))
}

// closeWithError mocks base method.
func (m *MockLiveState) closeWithError(externalError error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "closeWithError", externalError)
	ret0, _ := ret[0].(error)
	return ret0
}

// closeWithError indicates an expected call of closeWithError.
func (mr *MockLiveStateMockRecorder) closeWithError(externalError any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "closeWithError", reflect.TypeOf((*MockLiveState)(nil).closeWithError), externalError)
}

// setHashes mocks base method.
func (m *MockLiveState) setHashes(hashes *NodeHashes) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "setHashes", hashes)
	ret0, _ := ret[0].(error)
	return ret0
}

// setHashes indicates an expected call of setHashes.
func (mr *MockLiveStateMockRecorder) setHashes(hashes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "setHashes", reflect.TypeOf((*MockLiveState)(nil).setHashes), hashes)
}
