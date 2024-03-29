// Code generated by MockGen. DO NOT EDIT.
// Source: messaging/zaws/listener.go

// Package mock_zaws is a generated GoMock package.
package mock

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockISQSListener is a mock of ISQSListener interface.
type MockISQSListener struct {
	ctrl     *gomock.Controller
	recorder *MockISQSListenerMockRecorder
}

// MockISQSListenerMockRecorder is the mock recorder for MockISQSListener.
type MockISQSListenerMockRecorder struct {
	mock *MockISQSListener
}

// NewMockISQSListener creates a new mock instance.
func NewMockISQSListener(ctrl *gomock.Controller) *MockISQSListener {
	mock := &MockISQSListener{ctrl: ctrl}
	mock.recorder = &MockISQSListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockISQSListener) EXPECT() *MockISQSListenerMockRecorder {
	return m.recorder
}

// Listen mocks base method.
func (m *MockISQSListener) Listen() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Listen")
}

// Listen indicates an expected call of Listen.
func (mr *MockISQSListenerMockRecorder) Listen() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Listen", reflect.TypeOf((*MockISQSListener)(nil).Listen))
}
