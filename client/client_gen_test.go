// Code generated by MockGen. DO NOT EDIT.
// Source: ../eventlog/eventlog.go

// Package client_test is a generated GoMock package.
package client_test

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	eventlog "github.com/romshark/eventlog/eventlog"
)

// MockEventLogger is a mock of EventLogger interface.
type MockEventLogger struct {
	ctrl     *gomock.Controller
	recorder *MockEventLoggerMockRecorder
}

// MockEventLoggerMockRecorder is the mock recorder for MockEventLogger.
type MockEventLoggerMockRecorder struct {
	mock *MockEventLogger
}

// NewMockEventLogger creates a new mock instance.
func NewMockEventLogger(ctrl *gomock.Controller) *MockEventLogger {
	mock := &MockEventLogger{ctrl: ctrl}
	mock.recorder = &MockEventLoggerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventLogger) EXPECT() *MockEventLoggerMockRecorder {
	return m.recorder
}

// Append mocks base method.
func (m *MockEventLogger) Append(event eventlog.EventData) (uint64, uint64, time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Append", event)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(time.Time)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// Append indicates an expected call of Append.
func (mr *MockEventLoggerMockRecorder) Append(event interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Append", reflect.TypeOf((*MockEventLogger)(nil).Append), event)
}

// AppendCheck mocks base method.
func (m *MockEventLogger) AppendCheck(assumedVersion uint64, event eventlog.EventData) (uint64, time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppendCheck", assumedVersion, event)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(time.Time)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// AppendCheck indicates an expected call of AppendCheck.
func (mr *MockEventLoggerMockRecorder) AppendCheck(assumedVersion, event interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendCheck", reflect.TypeOf((*MockEventLogger)(nil).AppendCheck), assumedVersion, event)
}

// AppendCheckMulti mocks base method.
func (m *MockEventLogger) AppendCheckMulti(assumedVersion uint64, events ...eventlog.EventData) (uint64, uint64, time.Time, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{assumedVersion}
	for _, a := range events {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AppendCheckMulti", varargs...)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(time.Time)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// AppendCheckMulti indicates an expected call of AppendCheckMulti.
func (mr *MockEventLoggerMockRecorder) AppendCheckMulti(assumedVersion interface{}, events ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{assumedVersion}, events...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendCheckMulti", reflect.TypeOf((*MockEventLogger)(nil).AppendCheckMulti), varargs...)
}

// AppendMulti mocks base method.
func (m *MockEventLogger) AppendMulti(events ...eventlog.EventData) (uint64, uint64, uint64, time.Time, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{}
	for _, a := range events {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AppendMulti", varargs...)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(uint64)
	ret3, _ := ret[3].(time.Time)
	ret4, _ := ret[4].(error)
	return ret0, ret1, ret2, ret3, ret4
}

// AppendMulti indicates an expected call of AppendMulti.
func (mr *MockEventLoggerMockRecorder) AppendMulti(events ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendMulti", reflect.TypeOf((*MockEventLogger)(nil).AppendMulti), events...)
}

// Close mocks base method.
func (m *MockEventLogger) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockEventLoggerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockEventLogger)(nil).Close))
}

// MetadataLen mocks base method.
func (m *MockEventLogger) MetadataLen() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MetadataLen")
	ret0, _ := ret[0].(int)
	return ret0
}

// MetadataLen indicates an expected call of MetadataLen.
func (mr *MockEventLoggerMockRecorder) MetadataLen() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MetadataLen", reflect.TypeOf((*MockEventLogger)(nil).MetadataLen))
}

// Scan mocks base method.
func (m *MockEventLogger) Scan(version uint64, reverse bool, fn eventlog.ScanFn) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Scan", version, reverse, fn)
	ret0, _ := ret[0].(error)
	return ret0
}

// Scan indicates an expected call of Scan.
func (mr *MockEventLoggerMockRecorder) Scan(version, reverse, fn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Scan", reflect.TypeOf((*MockEventLogger)(nil).Scan), version, reverse, fn)
}

// ScanMetadata mocks base method.
func (m *MockEventLogger) ScanMetadata(fn func(string, string) bool) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ScanMetadata", fn)
}

// ScanMetadata indicates an expected call of ScanMetadata.
func (mr *MockEventLoggerMockRecorder) ScanMetadata(fn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScanMetadata", reflect.TypeOf((*MockEventLogger)(nil).ScanMetadata), fn)
}

// Version mocks base method.
func (m *MockEventLogger) Version() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Version")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// Version indicates an expected call of Version.
func (mr *MockEventLoggerMockRecorder) Version() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Version", reflect.TypeOf((*MockEventLogger)(nil).Version))
}

// VersionInitial mocks base method.
func (m *MockEventLogger) VersionInitial() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "VersionInitial")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// VersionInitial indicates an expected call of VersionInitial.
func (mr *MockEventLoggerMockRecorder) VersionInitial() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "VersionInitial", reflect.TypeOf((*MockEventLogger)(nil).VersionInitial))
}