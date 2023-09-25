// Code generated by MockGen. DO NOT EDIT.
// Source: verification.go

// Package mpt is a generated GoMock package.
package mpt

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockVerificationObserver is a mock of VerificationObserver interface.
type MockVerificationObserver struct {
	ctrl     *gomock.Controller
	recorder *MockVerificationObserverMockRecorder
}

// MockVerificationObserverMockRecorder is the mock recorder for MockVerificationObserver.
type MockVerificationObserverMockRecorder struct {
	mock *MockVerificationObserver
}

// NewMockVerificationObserver creates a new mock instance.
func NewMockVerificationObserver(ctrl *gomock.Controller) *MockVerificationObserver {
	mock := &MockVerificationObserver{ctrl: ctrl}
	mock.recorder = &MockVerificationObserverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockVerificationObserver) EXPECT() *MockVerificationObserverMockRecorder {
	return m.recorder
}

// EndVerification mocks base method.
func (m *MockVerificationObserver) EndVerification(res error) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "EndVerification", res)
}

// EndVerification indicates an expected call of EndVerification.
func (mr *MockVerificationObserverMockRecorder) EndVerification(res interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EndVerification", reflect.TypeOf((*MockVerificationObserver)(nil).EndVerification), res)
}

// Progress mocks base method.
func (m *MockVerificationObserver) Progress(msg string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Progress", msg)
}

// Progress indicates an expected call of Progress.
func (mr *MockVerificationObserverMockRecorder) Progress(msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Progress", reflect.TypeOf((*MockVerificationObserver)(nil).Progress), msg)
}

// StartVerification mocks base method.
func (m *MockVerificationObserver) StartVerification() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StartVerification")
}

// StartVerification indicates an expected call of StartVerification.
func (mr *MockVerificationObserverMockRecorder) StartVerification() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartVerification", reflect.TypeOf((*MockVerificationObserver)(nil).StartVerification))
}
