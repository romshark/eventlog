package client

import (
	"time"

	"github.com/romshark/eventlog/eventlog"
)

var (
	// ErrOffsetOutOfBound indicates an out-of-bound offset
	ErrOffsetOutOfBound = eventlog.ErrOffsetOutOfBound

	// ErrMismatchingVersions indicates mismatching versions
	ErrMismatchingVersions = eventlog.ErrMismatchingVersions

	// ErrInvalidPayload indicates an invalid payload
	ErrInvalidPayload = eventlog.ErrInvalidPayload
)

// Event represents a logged event
type Event struct {
	Offset  string                 `json:"offset"`
	Time    time.Time              `json:"time"`
	Payload map[string]interface{} `json:"payload"`
}

// Client represents an abstract client
type Client interface {
	Append(payload ...map[string]interface{}) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	AppendBytes(payload []byte) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	AppendCheck(
		assumedVersion string,
		payload map[string]interface{},
	) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	AppendCheckMulti(
		assumedVersion string,
		payload ...map[string]interface{},
	) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	AppendCheckBytes(
		assumedVersion string,
		payload []byte,
	) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	Read(
		offset string,
		n uint64,
	) ([]Event, error)

	Begin() (string, error)
}
