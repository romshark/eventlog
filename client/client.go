package client

import (
	"context"
	"errors"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// Event represents a logged event
type Event struct {
	Offset  string    `json:"offset"`
	Time    time.Time `json:"time"`
	Payload []byte    `json:"payload"`
	Next    string    `json:"next"`
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
	if assumedVersion == "" {
		err = ErrInvalidVersion
		return
	}
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

// Scan reads a limited number of events at the given offset version
// calling the onEvent callback for every received event
func (c *Client) Scan(
	ctx context.Context,
	version string,
	limit uint,
	onEvent func(e Event) error,
) error {
	for i := uint(0); ; i++ {
		if limit > 0 && i >= limit {
			break
		}
		e, err := c.impl.Read(ctx, version)
		if err != nil {
			return err
		}
		if err := onEvent(e); err != nil {
			return err
		}
		if e.Next == "" {
			break
		}
		version = e.Next
	}
	return nil
}

// Read reads an event at the given offset version
func (c *Client) Read(
	ctx context.Context,
	version string,
) (Event, error) {
	return c.impl.Read(ctx, version)
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
	// Reapeat until either cancelled, succeeded or failed
	for {
		// Check context for cancelation
		if err = ctx.Err(); err != nil {
			return
		}

		var events []byte
		if events, err = transaction(); err != nil {
			return
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
	ErrOffsetOutOfBound    = eventlog.ErrOffsetOutOfBound
	ErrMismatchingVersions = eventlog.ErrMismatchingVersions
	ErrInvalidPayload      = eventlog.ErrInvalidPayload
	ErrInvalidVersion      = errors.New("invalid version")
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
	) (Event, error)

	Begin(context.Context) (string, error)

	Version(context.Context) (string, error)

	Listen(ctx context.Context, onUpdate func([]byte)) error
}
