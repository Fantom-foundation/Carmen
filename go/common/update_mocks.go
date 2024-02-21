// Code generated by MockGen. DO NOT EDIT.
// Source: update.go
//
// Generated by this command:
//
//	mockgen -source update.go -destination update_mocks.go -package common
//

// Package common is a generated GoMock package.
package common

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockUpdateTarget is a mock of UpdateTarget interface.
type MockUpdateTarget struct {
	ctrl     *gomock.Controller
	recorder *MockUpdateTargetMockRecorder
}

// MockUpdateTargetMockRecorder is the mock recorder for MockUpdateTarget.
type MockUpdateTargetMockRecorder struct {
	mock *MockUpdateTarget
}

// NewMockUpdateTarget creates a new mock instance.
func NewMockUpdateTarget(ctrl *gomock.Controller) *MockUpdateTarget {
	mock := &MockUpdateTarget{ctrl: ctrl}
	mock.recorder = &MockUpdateTargetMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockUpdateTarget) EXPECT() *MockUpdateTargetMockRecorder {
	return m.recorder
}

// CreateAccount mocks base method.
func (m *MockUpdateTarget) CreateAccount(address Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateAccount indicates an expected call of CreateAccount.
func (mr *MockUpdateTargetMockRecorder) CreateAccount(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateAccount", reflect.TypeOf((*MockUpdateTarget)(nil).CreateAccount), address)
}

// DeleteAccount mocks base method.
func (m *MockUpdateTarget) DeleteAccount(address Address) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccount", address)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteAccount indicates an expected call of DeleteAccount.
func (mr *MockUpdateTargetMockRecorder) DeleteAccount(address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccount", reflect.TypeOf((*MockUpdateTarget)(nil).DeleteAccount), address)
}

// SetBalance mocks base method.
func (m *MockUpdateTarget) SetBalance(address Address, balance Balance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetBalance", address, balance)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetBalance indicates an expected call of SetBalance.
func (mr *MockUpdateTargetMockRecorder) SetBalance(address, balance any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBalance", reflect.TypeOf((*MockUpdateTarget)(nil).SetBalance), address, balance)
}

// SetCode mocks base method.
func (m *MockUpdateTarget) SetCode(address Address, code []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetCode", address, code)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetCode indicates an expected call of SetCode.
func (mr *MockUpdateTargetMockRecorder) SetCode(address, code any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetCode", reflect.TypeOf((*MockUpdateTarget)(nil).SetCode), address, code)
}

// SetNonce mocks base method.
func (m *MockUpdateTarget) SetNonce(address Address, nonce Nonce) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetNonce", address, nonce)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetNonce indicates an expected call of SetNonce.
func (mr *MockUpdateTargetMockRecorder) SetNonce(address, nonce any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetNonce", reflect.TypeOf((*MockUpdateTarget)(nil).SetNonce), address, nonce)
}

// SetStorage mocks base method.
func (m *MockUpdateTarget) SetStorage(address Address, key Key, value Value) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetStorage", address, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetStorage indicates an expected call of SetStorage.
func (mr *MockUpdateTargetMockRecorder) SetStorage(address, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStorage", reflect.TypeOf((*MockUpdateTarget)(nil).SetStorage), address, key, value)
}
