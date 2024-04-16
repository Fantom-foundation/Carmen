//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

// Code generated by MockGen. DO NOT EDIT.
// Source: node_cache.go
//
// Generated by this command:
//
//	mockgen -source node_cache.go -destination node_cache_mocks.go -package mpt
//
// Package mpt is a generated GoMock package.
package mpt

import (
	reflect "reflect"

	common "github.com/Fantom-foundation/Carmen/go/common"
	shared "github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	gomock "go.uber.org/mock/gomock"
)

// MockNodeCache is a mock of NodeCache interface.
type MockNodeCache struct {
	ctrl     *gomock.Controller
	recorder *MockNodeCacheMockRecorder
}

// MockNodeCacheMockRecorder is the mock recorder for MockNodeCache.
type MockNodeCacheMockRecorder struct {
	mock *MockNodeCache
}

// NewMockNodeCache creates a new mock instance.
func NewMockNodeCache(ctrl *gomock.Controller) *MockNodeCache {
	mock := &MockNodeCache{ctrl: ctrl}
	mock.recorder = &MockNodeCacheMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodeCache) EXPECT() *MockNodeCacheMockRecorder {
	return m.recorder
}

// ForEach mocks base method.
func (m *MockNodeCache) ForEach(arg0 func(NodeId, *shared.Shared[Node])) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ForEach", arg0)
}

// ForEach indicates an expected call of ForEach.
func (mr *MockNodeCacheMockRecorder) ForEach(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockNodeCache)(nil).ForEach), arg0)
}

// Get mocks base method.
func (m *MockNodeCache) Get(r *NodeReference) (*shared.Shared[Node], bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", r)
	ret0, _ := ret[0].(*shared.Shared[Node])
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockNodeCacheMockRecorder) Get(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockNodeCache)(nil).Get), r)
}

// GetMemoryFootprint mocks base method.
func (m *MockNodeCache) GetMemoryFootprint() *common.MemoryFootprint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMemoryFootprint")
	ret0, _ := ret[0].(*common.MemoryFootprint)
	return ret0
}

// GetMemoryFootprint indicates an expected call of GetMemoryFootprint.
func (mr *MockNodeCacheMockRecorder) GetMemoryFootprint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMemoryFootprint", reflect.TypeOf((*MockNodeCache)(nil).GetMemoryFootprint))
}

// GetOrSet mocks base method.
func (m *MockNodeCache) GetOrSet(arg0 *NodeReference, arg1 *shared.Shared[Node]) (*shared.Shared[Node], bool, NodeId, *shared.Shared[Node], bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrSet", arg0, arg1)
	ret0, _ := ret[0].(*shared.Shared[Node])
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(NodeId)
	ret3, _ := ret[3].(*shared.Shared[Node])
	ret4, _ := ret[4].(bool)
	return ret0, ret1, ret2, ret3, ret4
}

// GetOrSet indicates an expected call of GetOrSet.
func (mr *MockNodeCacheMockRecorder) GetOrSet(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrSet", reflect.TypeOf((*MockNodeCache)(nil).GetOrSet), arg0, arg1)
}

// Release mocks base method.
func (m *MockNodeCache) Release(r *NodeReference) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Release", r)
}

// Release indicates an expected call of Release.
func (mr *MockNodeCacheMockRecorder) Release(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Release", reflect.TypeOf((*MockNodeCache)(nil).Release), r)
}

// Touch mocks base method.
func (m *MockNodeCache) Touch(r *NodeReference) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Touch", r)
}

// Touch indicates an expected call of Touch.
func (mr *MockNodeCacheMockRecorder) Touch(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Touch", reflect.TypeOf((*MockNodeCache)(nil).Touch), r)
}
