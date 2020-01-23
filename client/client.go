package client

import (
	"errors"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

var (
	// ErrOffsetOutOfBound indicates an out-of-bound offset
	ErrOffsetOutOfBound = eventlog.ErrOffsetOutOfBound

	// ErrMismatchingVersions indicates mismatching versions
	ErrMismatchingVersions = eventlog.ErrMismatchingVersions

	// ErrInvalidPayload indicates an invalid payload
	ErrInvalidPayload = errors.New("invalid payload")
)

// Event represents a logged event
type Event struct {
	Time    time.Time              `json:"time"`
	Payload map[string]interface{} `json:"payload"`
}

// Client represents an abstract client
type Client interface {
	Append(
		payload map[string]interface{},
	) error

	AppendBytes(
		payload []byte,
	) error

	AppendCheck(
		offset uint64,
		payload map[string]interface{},
	) error

	AppendCheckBytes(
		offset uint64,
		payload []byte,
	) error

	Read(
		offset uint64,
		n uint64,
	) ([]Event, error)
}
