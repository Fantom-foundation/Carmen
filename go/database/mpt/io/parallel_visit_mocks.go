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
// Source: parallel_visit.go
//
// Generated by this command:
//
//	mockgen -source parallel_visit.go -destination parallel_visit_mocks.go -package io
//
// Package io is a generated GoMock package.
package io

import (
	reflect "reflect"

	mpt "github.com/Fantom-foundation/Carmen/go/database/mpt"
	gomock "go.uber.org/mock/gomock"
)

// MocknodeSourceFactory is a mock of nodeSourceFactory interface.
type MocknodeSourceFactory struct {
	ctrl     *gomock.Controller
	recorder *MocknodeSourceFactoryMockRecorder
}

// MocknodeSourceFactoryMockRecorder is the mock recorder for MocknodeSourceFactory.
type MocknodeSourceFactoryMockRecorder struct {
	mock *MocknodeSourceFactory
}

// NewMocknodeSourceFactory creates a new mock instance.
func NewMocknodeSourceFactory(ctrl *gomock.Controller) *MocknodeSourceFactory {
	mock := &MocknodeSourceFactory{ctrl: ctrl}
	mock.recorder = &MocknodeSourceFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocknodeSourceFactory) EXPECT() *MocknodeSourceFactoryMockRecorder {
	return m.recorder
}

// open mocks base method.
func (m *MocknodeSourceFactory) open() (nodeSource, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "open")
	ret0, _ := ret[0].(nodeSource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// open indicates an expected call of open.
func (mr *MocknodeSourceFactoryMockRecorder) open() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "open", reflect.TypeOf((*MocknodeSourceFactory)(nil).open))
}

// MocknodeSource is a mock of nodeSource interface.
type MocknodeSource struct {
	ctrl     *gomock.Controller
	recorder *MocknodeSourceMockRecorder
}

// MocknodeSourceMockRecorder is the mock recorder for MocknodeSource.
type MocknodeSourceMockRecorder struct {
	mock *MocknodeSource
}

// NewMocknodeSource creates a new mock instance.
func NewMocknodeSource(ctrl *gomock.Controller) *MocknodeSource {
	mock := &MocknodeSource{ctrl: ctrl}
	mock.recorder = &MocknodeSourceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocknodeSource) EXPECT() *MocknodeSourceMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MocknodeSource) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MocknodeSourceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MocknodeSource)(nil).Close))
}

// get mocks base method.
func (m *MocknodeSource) get(arg0 mpt.NodeId) (mpt.Node, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "get", arg0)
	ret0, _ := ret[0].(mpt.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// get indicates an expected call of get.
func (mr *MocknodeSourceMockRecorder) get(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "get", reflect.TypeOf((*MocknodeSource)(nil).get), arg0)
}