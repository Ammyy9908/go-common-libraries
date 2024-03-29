// Code generated by MockGen. DO NOT EDIT.
// Source: messaging/zaws/manager.go

// Package mock_zaws is a generated GoMock package.
package zaws

import (
	reflect "reflect"

	sns "github.com/aws/aws-sdk-go-v2/service/sns"
	sqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	gomock "github.com/golang/mock/gomock"
)

// MockIManager is a mock of IManager interface.
type MockIManager struct {
	ctrl     *gomock.Controller
	recorder *MockIManagerMockRecorder
}

// MockIManagerMockRecorder is the mock recorder for MockIManager.
type MockIManagerMockRecorder struct {
	mock *MockIManager
}

// NewMockIManager creates a new mock instance.
func NewMockIManager(ctrl *gomock.Controller) *MockIManager {
	mock := &MockIManager{ctrl: ctrl}
	mock.recorder = &MockIManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIManager) EXPECT() *MockIManagerMockRecorder {
	return m.recorder
}

// CreateQueue mocks base method.
func (m *MockIManager) CreateQueue(queueName string, config QueueConfig) (*sqs.CreateQueueOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateQueue", queueName, config)
	ret0, _ := ret[0].(*sqs.CreateQueueOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateQueue indicates an expected call of CreateQueue.
func (mr *MockIManagerMockRecorder) CreateQueue(queueName, config interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateQueue", reflect.TypeOf((*MockIManager)(nil).CreateQueue), queueName, config)
}

// CreateTopic mocks base method.
func (m *MockIManager) CreateTopic(topicName string, tags map[string]string) (*sns.CreateTopicOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTopic", topicName, tags)
	ret0, _ := ret[0].(*sns.CreateTopicOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateTopic indicates an expected call of CreateTopic.
func (mr *MockIManagerMockRecorder) CreateTopic(topicName, tags interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTopic", reflect.TypeOf((*MockIManager)(nil).CreateTopic), topicName, tags)
}

// SubscribeQueueToTopic mocks base method.
func (m *MockIManager) SubscribeQueueToTopic(queueName, topicName string, raw bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeQueueToTopic", queueName, topicName, raw)
	ret0, _ := ret[0].(error)
	return ret0
}

// SubscribeQueueToTopic indicates an expected call of SubscribeQueueToTopic.
func (mr *MockIManagerMockRecorder) SubscribeQueueToTopic(queueName, topicName, raw interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SubscribeQueueToTopic", reflect.TypeOf((*MockIManager)(nil).SubscribeQueueToTopic), queueName, topicName, raw)
}
