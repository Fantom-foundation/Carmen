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
//	mockgen -source state.go -destination state_mock.go -package state
//

// Package state is a generated GoMock package.
package state

import (
	reflect "reflect"

	backend "github.com/Fantom-foundation/Carmen/go/backend"
	common "github.com/Fantom-foundation/Carmen/go/common"
	amount "github.com/Fantom-foundation/Carmen/go/common/amount"
	gomock "go.uber.org/mock/gomock"
)

// MockState is a mock of State interface.
type MockState struct {
	ctrl     *gomock.Controller
	recorder *MockStateMockRecorder
}

// MockStateMockRecorder is the mock recorder for MockState.
type MockStateMockRecorder struct {
	mock *MockState
}

// NewMockState creates a new mock instance.
func NewMockState(ctrl *gomock.Controller) *MockState {
	mock := &MockState{ctrl: ctrl}
	mock.recorder = &MockStateMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockState) EXPECT() *MockStateMockRecorder {
	return m.recorder
}

// Apply mocks base method.
func (m *MockState) Apply(block uint64, update common.Update) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Apply", block, update)
	ret0, _ := ret[0].(error)
	return ret0
}

// Apply indicates an expected call of Apply.
func (mr *MockStateMockRecorder) Apply(block, update any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Apply", reflect.TypeOf((*MockState)(nil).Apply), block, update)
}

// Check mocks base method.
func (m *MockState) Check() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Check")
	ret0, _ := ret[0].(error)
	return ret0
}

// Check indicates an expected call of Check.
func (mr *MockStateMockRecorder) Check() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Check", reflect.TypeOf((*MockState)(nil).Check))
}

// Close mocks base method.
func (m *MockState) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockStateMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockState)(nil).Close))
}

// CreateSnapshot mocks base method.
func (m *MockState) CreateSnapshot() (backend.Snapshot, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateSnapshot")
	ret0, _ := ret[0].(backend.Snapshot)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateSnapshot indicates an expected call of CreateSnapshot.
func (mr *MockStateMockRecorder) CreateSnapshot() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateSnapshot", reflect.TypeOf((*MockState)(nil).CreateSnapshot))
}

// Exists mocks base method.
func (m *MockState) Exists(address common.Address) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exists", address)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists.
func (mr *MockStateMockRecorder) Exists(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockState)(nil).Exists), address)
}

// Flush mocks base method.
func (m *MockState) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockStateMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockState)(nil).Flush))
}

// GetArchiveBlockHeight mocks base method.
func (m *MockState) GetArchiveBlockHeight() (uint64, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetArchiveBlockHeight")
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetArchiveBlockHeight indicates an expected call of GetArchiveBlockHeight.
func (mr *MockStateMockRecorder) GetArchiveBlockHeight() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetArchiveBlockHeight", reflect.TypeOf((*MockState)(nil).GetArchiveBlockHeight))
}

// GetArchiveState mocks base method.
func (m *MockState) GetArchiveState(block uint64) (State, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetArchiveState", block)
	ret0, _ := ret[0].(State)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetArchiveState indicates an expected call of GetArchiveState.
func (mr *MockStateMockRecorder) GetArchiveState(block any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetArchiveState", reflect.TypeOf((*MockState)(nil).GetArchiveState), block)
}

// GetBalance mocks base method.
func (m *MockState) GetBalance(address common.Address) (amount.Amount, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", address)
	ret0, _ := ret[0].(amount.Amount)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockStateMockRecorder) GetBalance(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalance", reflect.TypeOf((*MockState)(nil).GetBalance), address)
}

// GetCode mocks base method.
func (m *MockState) GetCode(address common.Address) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCode", address)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCode indicates an expected call of GetCode.
func (mr *MockStateMockRecorder) GetCode(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCode", reflect.TypeOf((*MockState)(nil).GetCode), address)
}

// GetCodeHash mocks base method.
func (m *MockState) GetCodeHash(address common.Address) (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeHash", address)
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeHash indicates an expected call of GetCodeHash.
func (mr *MockStateMockRecorder) GetCodeHash(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeHash", reflect.TypeOf((*MockState)(nil).GetCodeHash), address)
}

// GetCodeSize mocks base method.
func (m *MockState) GetCodeSize(address common.Address) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeSize", address)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeSize indicates an expected call of GetCodeSize.
func (mr *MockStateMockRecorder) GetCodeSize(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeSize", reflect.TypeOf((*MockState)(nil).GetCodeSize), address)
}

// GetHash mocks base method.
func (m *MockState) GetHash() (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHash")
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHash indicates an expected call of GetHash.
func (mr *MockStateMockRecorder) GetHash() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHash", reflect.TypeOf((*MockState)(nil).GetHash))
}

// GetMemoryFootprint mocks base method.
func (m *MockState) GetMemoryFootprint() *common.MemoryFootprint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMemoryFootprint")
	ret0, _ := ret[0].(*common.MemoryFootprint)
	return ret0
}

// GetMemoryFootprint indicates an expected call of GetMemoryFootprint.
func (mr *MockStateMockRecorder) GetMemoryFootprint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMemoryFootprint", reflect.TypeOf((*MockState)(nil).GetMemoryFootprint))
}

// GetNonce mocks base method.
func (m *MockState) GetNonce(address common.Address) (common.Nonce, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", address)
	ret0, _ := ret[0].(common.Nonce)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockStateMockRecorder) GetNonce(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockState)(nil).GetNonce), address)
}

// GetProof mocks base method.
func (m *MockState) GetProof() (backend.Proof, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProof")
	ret0, _ := ret[0].(backend.Proof)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetProof indicates an expected call of GetProof.
func (mr *MockStateMockRecorder) GetProof() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProof", reflect.TypeOf((*MockState)(nil).GetProof))
}

// GetSnapshotVerifier mocks base method.
func (m *MockState) GetSnapshotVerifier(metadata []byte) (backend.SnapshotVerifier, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSnapshotVerifier", metadata)
	ret0, _ := ret[0].(backend.SnapshotVerifier)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSnapshotVerifier indicates an expected call of GetSnapshotVerifier.
func (mr *MockStateMockRecorder) GetSnapshotVerifier(metadata any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSnapshotVerifier", reflect.TypeOf((*MockState)(nil).GetSnapshotVerifier), metadata)
}

// GetStorage mocks base method.
func (m *MockState) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorage", address, key)
	ret0, _ := ret[0].(common.Value)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorage indicates an expected call of GetStorage.
func (mr *MockStateMockRecorder) GetStorage(address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorage", reflect.TypeOf((*MockState)(nil).GetStorage), address, key)
}

// Restore mocks base method.
func (m *MockState) Restore(data backend.SnapshotData) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Restore", data)
	ret0, _ := ret[0].(error)
	return ret0
}

// Restore indicates an expected call of Restore.
func (mr *MockStateMockRecorder) Restore(data any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Restore", reflect.TypeOf((*MockState)(nil).Restore), data)
}

// MockLiveDB is a mock of LiveDB interface.
type MockLiveDB struct {
	ctrl     *gomock.Controller
	recorder *MockLiveDBMockRecorder
}

// MockLiveDBMockRecorder is the mock recorder for MockLiveDB.
type MockLiveDBMockRecorder struct {
	mock *MockLiveDB
}

// NewMockLiveDB creates a new mock instance.
func NewMockLiveDB(ctrl *gomock.Controller) *MockLiveDB {
	mock := &MockLiveDB{ctrl: ctrl}
	mock.recorder = &MockLiveDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLiveDB) EXPECT() *MockLiveDBMockRecorder {
	return m.recorder
}

// Apply mocks base method.
func (m *MockLiveDB) Apply(block uint64, update common.Update) (common.Releaser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Apply", block, update)
	ret0, _ := ret[0].(common.Releaser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Apply indicates an expected call of Apply.
func (mr *MockLiveDBMockRecorder) Apply(block, update any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Apply", reflect.TypeOf((*MockLiveDB)(nil).Apply), block, update)
}

// Close mocks base method.
func (m *MockLiveDB) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockLiveDBMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockLiveDB)(nil).Close))
}

// Exists mocks base method.
func (m *MockLiveDB) Exists(address common.Address) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exists", address)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists.
func (mr *MockLiveDBMockRecorder) Exists(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockLiveDB)(nil).Exists), address)
}

// Flush mocks base method.
func (m *MockLiveDB) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockLiveDBMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockLiveDB)(nil).Flush))
}

// GetBalance mocks base method.
func (m *MockLiveDB) GetBalance(address common.Address) (amount.Amount, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", address)
	ret0, _ := ret[0].(amount.Amount)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockLiveDBMockRecorder) GetBalance(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalance", reflect.TypeOf((*MockLiveDB)(nil).GetBalance), address)
}

// GetCode mocks base method.
func (m *MockLiveDB) GetCode(address common.Address) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCode", address)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCode indicates an expected call of GetCode.
func (mr *MockLiveDBMockRecorder) GetCode(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCode", reflect.TypeOf((*MockLiveDB)(nil).GetCode), address)
}

// GetCodeHash mocks base method.
func (m *MockLiveDB) GetCodeHash(address common.Address) (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeHash", address)
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeHash indicates an expected call of GetCodeHash.
func (mr *MockLiveDBMockRecorder) GetCodeHash(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeHash", reflect.TypeOf((*MockLiveDB)(nil).GetCodeHash), address)
}

// GetCodeSize mocks base method.
func (m *MockLiveDB) GetCodeSize(address common.Address) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeSize", address)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCodeSize indicates an expected call of GetCodeSize.
func (mr *MockLiveDBMockRecorder) GetCodeSize(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeSize", reflect.TypeOf((*MockLiveDB)(nil).GetCodeSize), address)
}

// GetHash mocks base method.
func (m *MockLiveDB) GetHash() (common.Hash, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHash")
	ret0, _ := ret[0].(common.Hash)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHash indicates an expected call of GetHash.
func (mr *MockLiveDBMockRecorder) GetHash() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHash", reflect.TypeOf((*MockLiveDB)(nil).GetHash))
}

// GetMemoryFootprint mocks base method.
func (m *MockLiveDB) GetMemoryFootprint() *common.MemoryFootprint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMemoryFootprint")
	ret0, _ := ret[0].(*common.MemoryFootprint)
	return ret0
}

// GetMemoryFootprint indicates an expected call of GetMemoryFootprint.
func (mr *MockLiveDBMockRecorder) GetMemoryFootprint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMemoryFootprint", reflect.TypeOf((*MockLiveDB)(nil).GetMemoryFootprint))
}

// GetNonce mocks base method.
func (m *MockLiveDB) GetNonce(address common.Address) (common.Nonce, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", address)
	ret0, _ := ret[0].(common.Nonce)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockLiveDBMockRecorder) GetNonce(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockLiveDB)(nil).GetNonce), address)
}

// GetSnapshotableComponents mocks base method.
func (m *MockLiveDB) GetSnapshotableComponents() []backend.Snapshotable {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSnapshotableComponents")
	ret0, _ := ret[0].([]backend.Snapshotable)
	return ret0
}

// GetSnapshotableComponents indicates an expected call of GetSnapshotableComponents.
func (mr *MockLiveDBMockRecorder) GetSnapshotableComponents() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSnapshotableComponents", reflect.TypeOf((*MockLiveDB)(nil).GetSnapshotableComponents))
}

// GetStorage mocks base method.
func (m *MockLiveDB) GetStorage(address common.Address, key common.Key) (common.Value, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorage", address, key)
	ret0, _ := ret[0].(common.Value)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorage indicates an expected call of GetStorage.
func (mr *MockLiveDBMockRecorder) GetStorage(address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorage", reflect.TypeOf((*MockLiveDB)(nil).GetStorage), address, key)
}

// RunPostRestoreTasks mocks base method.
func (m *MockLiveDB) RunPostRestoreTasks() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunPostRestoreTasks")
	ret0, _ := ret[0].(error)
	return ret0
}

// RunPostRestoreTasks indicates an expected call of RunPostRestoreTasks.
func (mr *MockLiveDBMockRecorder) RunPostRestoreTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunPostRestoreTasks", reflect.TypeOf((*MockLiveDB)(nil).RunPostRestoreTasks))
}
