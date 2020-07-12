package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// Event represents a logged event
type Event struct {
	Offset  string                 `json:"offset"`
	Time    time.Time              `json:"time"`
	Payload map[string]interface{} `json:"payload"`
}

type Client struct {
	impl Implementer
}

// New creates a new eventlog client
func New(impl Implementer) *Client {
	return &Client{
		impl: impl,
	}
}

// Append appends one or multiple new events onto the log
func (c *Client) Append(
	ctx context.Context,
	payload ...map[string]interface{},
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	var body []byte
	if body, err = encodeJSON(payload); err != nil {
		return
	}
	return c.appendJSON(ctx, false, "", body)
}

// AppendCheck appends one or multiple new events onto the logs
// if the assumed version matches the actual log version,
// otherwise the operation is rejected and ErrMismatchingVersions is returned.
func (c *Client) AppendCheck(
	ctx context.Context,
	assumedVersion string,
	payload ...map[string]interface{},
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if assumedVersion == "" {
		err = errors.New("no assumed version")
		return
	}

	var body []byte
	if body, err = encodeJSON(payload); err != nil {
		return
	}
	return c.appendJSON(ctx, true, assumedVersion, body)
}

// AppendJSON appends one or multiple new events
// in JSON format onto the log.
func (c *Client) AppendJSON(
	ctx context.Context,
	payload []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.appendJSON(ctx, false, "", payload)
}

// AppendCheckJSON appends one or multiple new events
// in JSON format onto the logs if the assumed version
// matches the actual log version, otherwise the operation
// is rejected and ErrMismatchingVersions is returned.
func (c *Client) AppendCheckJSON(
	ctx context.Context,
	assumedVersion string,
	payload []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.appendJSON(ctx, true, assumedVersion, payload)
}

func (c *Client) appendJSON(
	ctx context.Context,
	assumeVersion bool,
	assumedVersion string,
	payloadJSON []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if len(payloadJSON) < 1 {
		err = ErrInvalidPayload
		return
	}

	return c.impl.AppendJSON(ctx, assumeVersion, assumedVersion, payloadJSON)
}

// Read reads n number of events from (including) the given version
func (c *Client) Read(
	ctx context.Context,
	version string,
	n uint64,
) ([]Event, error) {
	return c.impl.Read(ctx, version, n)
}

// Begin implements Client.Begin
func (c *Client) Begin(ctx context.Context) (string, error) {
	return c.impl.Begin(ctx)
}

func (c *Client) Version(ctx context.Context) (string, error) {
	return c.impl.Version(ctx)
}

// Listen establishes a websocket connection to the server
// and starts listening for version update notifications
// calling onUpdate when one is received.
func (c *Client) Listen(ctx context.Context, onUpdate func([]byte)) error {
	return c.impl.Listen(ctx, onUpdate)
}

// TryAppend keeps executing transaction until either cancelled,
// succeeded (assumed and actual event log versions match) or failed due to an error.
func (c *Client) TryAppend(
	ctx context.Context,
	assumedVersion string,
	transaction func() (events []map[string]interface{}, err error),
	sync func() (string, error),
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.tryAppendJSON(ctx, assumedVersion, transaction, sync)
}

// TryAppendJSON keeps executing transaction until either cancelled,
// succeeded (assumed and actual event log versions match) or failed due to an error.
func (c *Client) TryAppendJSON(
	ctx context.Context,
	assumedVersion string,
	transaction func() (events []byte, err error),
	sync func() (string, error),
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.tryAppendJSON(ctx, assumedVersion, transaction, sync)
}

// tryAppendJSON keeps executing transaction until either cancelled,
// succeeded (assumed and actual event log versions match) or failed due to an error.
func (c *Client) tryAppendJSON(
	ctx context.Context,
	assumedVersion string,
	transaction interface{},
	sync func() (string, error),
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	// Reapeat until either cancelled, succeeded or failed
	for {
		// Check context for cancelation
		if err = ctx.Err(); err != nil {
			return
		}

		var events []byte
		switch t := transaction.(type) {
		case func() ([]map[string]interface{}, error):
			var payload []map[string]interface{}
			if payload, err = t(); err != nil {
				return
			}
			if events, err = encodeJSON(payload); err != nil {
				return
			}
		case func() ([]byte, error):
			if events, err = t(); err != nil {
				return
			}
		default:
			panic("unsupported transaction function type")
		}

		// Try to append new events onto the event log
		offset, newVersion, tm, err = c.AppendCheckJSON(ctx, assumedVersion, events)
		switch {
		case errors.Is(err, ErrMismatchingVersions):
			// The projection is out of sync, synchronize & repeat
			if assumedVersion, err = sync(); err != nil {
				return
			}
			continue
		case err != nil:
			// Append failed for unexpected reason
			return
		}

		// Transaction successfully committed
		break
	}
	return
}

var (
	// ErrOffsetOutOfBound indicates an out-of-bound offset
	ErrOffsetOutOfBound = eventlog.ErrOffsetOutOfBound

	// ErrMismatchingVersions indicates mismatching versions
	ErrMismatchingVersions = eventlog.ErrMismatchingVersions

	// ErrInvalidPayload indicates an invalid payload
	ErrInvalidPayload = eventlog.ErrInvalidPayload
)

type Log interface {
	Printf(format string, v ...interface{})
}

// Implementer represents a client implementer
type Implementer interface {
	AppendJSON(
		ctx context.Context,
		assumeVersion bool,
		assumedVersion string,
		payloadJSON []byte,
	) (
		offset string,
		newVersion string,
		tm time.Time,
		err error,
	)

	Read(
		ctx context.Context,
		offset string,
		n uint64,
	) ([]Event, error)

	Begin(context.Context) (string, error)

	Version(context.Context) (string, error)

	Listen(ctx context.Context, onUpdate func([]byte)) error
}

func encodeJSON(payload []map[string]interface{}) ([]byte, error) {
	switch l := len(payload); {
	case l < 1:
		return nil, ErrInvalidPayload
	case l == 1:
		events, err := json.Marshal(payload[0])
		if err != nil {
			return nil, fmt.Errorf("marshaling event body: %w", err)
		}
		return events, nil
	default:
		events, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshaling multiple event bodies: %w", err)
		}
		return events, nil
	}
}
