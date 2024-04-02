// Code generated by MockGen. DO NOT EDIT.
// Source: write_buffer.go
//
// Generated by this command:
//
//	mockgen -source write_buffer.go -destination write_buffer_mocks.go -package mpt
//

// Package mpt is a generated GoMock package.
package mpt

import (
	reflect "reflect"

	shared "github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	gomock "go.uber.org/mock/gomock"
)

// MockWriteBuffer is a mock of WriteBuffer interface.
type MockWriteBuffer struct {
	ctrl     *gomock.Controller
	recorder *MockWriteBufferMockRecorder
}

// MockWriteBufferMockRecorder is the mock recorder for MockWriteBuffer.
type MockWriteBufferMockRecorder struct {
	mock *MockWriteBuffer
}

// NewMockWriteBuffer creates a new mock instance.
func NewMockWriteBuffer(ctrl *gomock.Controller) *MockWriteBuffer {
	mock := &MockWriteBuffer{ctrl: ctrl}
	mock.recorder = &MockWriteBufferMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWriteBuffer) EXPECT() *MockWriteBufferMockRecorder {
	return m.recorder
}

// Add mocks base method.
func (m *MockWriteBuffer) Add(arg0 NodeId, arg1 *shared.Shared[Node]) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Add", arg0, arg1)
}

// Add indicates an expected call of Add.
func (mr *MockWriteBufferMockRecorder) Add(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockWriteBuffer)(nil).Add), arg0, arg1)
}

// Cancel mocks base method.
func (m *MockWriteBuffer) Cancel(arg0 NodeId) (*shared.Shared[Node], bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cancel", arg0)
	ret0, _ := ret[0].(*shared.Shared[Node])
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Cancel indicates an expected call of Cancel.
func (mr *MockWriteBufferMockRecorder) Cancel(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockWriteBuffer)(nil).Cancel), arg0)
}

// Close mocks base method.
func (m *MockWriteBuffer) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockWriteBufferMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockWriteBuffer)(nil).Close))
}

// Flush mocks base method.
func (m *MockWriteBuffer) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockWriteBufferMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockWriteBuffer)(nil).Flush))
}

// MockNodeSink is a mock of NodeSink interface.
type MockNodeSink struct {
	ctrl     *gomock.Controller
	recorder *MockNodeSinkMockRecorder
}

// MockNodeSinkMockRecorder is the mock recorder for MockNodeSink.
type MockNodeSinkMockRecorder struct {
	mock *MockNodeSink
}

// NewMockNodeSink creates a new mock instance.
func NewMockNodeSink(ctrl *gomock.Controller) *MockNodeSink {
	mock := &MockNodeSink{ctrl: ctrl}
	mock.recorder = &MockNodeSinkMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodeSink) EXPECT() *MockNodeSinkMockRecorder {
	return m.recorder
}

// Write mocks base method.
func (m *MockNodeSink) Write(arg0 NodeId, arg1 shared.ViewHandle[Node]) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockNodeSinkMockRecorder) Write(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockNodeSink)(nil).Write), arg0, arg1)
}