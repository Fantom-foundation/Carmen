// Code generated by MockGen. DO NOT EDIT.
// Source: state.go

// Package state is a generated GoMock package.
package state

import (
	reflect "reflect"

	common "github.com/Fantom-foundation/Carmen/go/common"
	gomock "github.com/golang/mock/gomock"
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

// CreateAccount mocks base method.
func (m *MockState) CreateAccount(address common.Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateAccount indicates an expected call of CreateAccount.
func (mr *MockStateMockRecorder) CreateAccount(address interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateAccount", reflect.TypeOf((*MockState)(nil).CreateAccount), address)
}

// DeleteAccount mocks base method.
func (m *MockState) DeleteAccount(address common.Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteAccount indicates an expected call of DeleteAccount.
func (mr *MockStateMockRecorder) DeleteAccount(address interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccount", reflect.TypeOf((*MockState)(nil).DeleteAccount), address)
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

// GetAccountState mocks base method.
func (m *MockState) GetAccountState(address common.Address) (common.AccountState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccountState", address)
	ret0, _ := ret[0].(common.AccountState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccountState indicates an expected call of GetAccountState.
func (mr *MockStateMockRecorder) GetAccountState(address interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccountState", reflect.TypeOf((*MockState)(nil).GetAccountState), address)
}

// GetBalance mocks base method.
func (m *MockState) GetBalance(address common.Address) (common.Balance, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", address)
	ret0, _ := ret[0].(common.Balance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockStateMockRecorder) GetBalance(address interface{}) *gomock.Call {
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
func (mr *MockStateMockRecorder) GetCode(address interface{}) *gomock.Call {
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
func (mr *MockStateMockRecorder) GetCodeHash(address interface{}) *gomock.Call {
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
func (mr *MockStateMockRecorder) GetCodeSize(address interface{}) *gomock.Call {
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

// GetNonce mocks base method.
func (m *MockState) GetNonce(address common.Address) (common.Nonce, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", address)
	ret0, _ := ret[0].(common.Nonce)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockStateMockRecorder) GetNonce(address interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockState)(nil).GetNonce), address)
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
func (mr *MockStateMockRecorder) GetStorage(address, key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorage", reflect.TypeOf((*MockState)(nil).GetStorage), address, key)
}

// SetBalance mocks base method.
func (m *MockState) SetBalance(address common.Address, balance common.Balance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetBalance", address, balance)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetBalance indicates an expected call of SetBalance.
func (mr *MockStateMockRecorder) SetBalance(address, balance interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBalance", reflect.TypeOf((*MockState)(nil).SetBalance), address, balance)
}

// SetCode mocks base method.
func (m *MockState) SetCode(address common.Address, code []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetCode", address, code)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetCode indicates an expected call of SetCode.
func (mr *MockStateMockRecorder) SetCode(address, code interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetCode", reflect.TypeOf((*MockState)(nil).SetCode), address, code)
}

// SetNonce mocks base method.
func (m *MockState) SetNonce(address common.Address, nonce common.Nonce) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetNonce", address, nonce)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetNonce indicates an expected call of SetNonce.
func (mr *MockStateMockRecorder) SetNonce(address, nonce interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetNonce", reflect.TypeOf((*MockState)(nil).SetNonce), address, nonce)
}

// SetStorage mocks base method.
func (m *MockState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetStorage", address, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetStorage indicates an expected call of SetStorage.
func (mr *MockStateMockRecorder) SetStorage(address, key, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStorage", reflect.TypeOf((*MockState)(nil).SetStorage), address, key, value)
}
