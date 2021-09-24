package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal"
)

type Version = string

type Event struct {
	EventData
	Time            time.Time `json:"time"`
	Version         Version   `json:"version"`
	VersionNext     Version   `json:"version-next"`
	VersionPrevious Version   `json:"version-previous"`
}

type EventData = eventlog.EventData

type Client struct {
	impl Connecter
}

// New creates a new eventlog client.
func New(impl Connecter) *Client {
	return &Client{
		impl: impl,
	}
}

// Append appends a new event onto the log.
func (c *Client) Append(
	ctx context.Context,
	event EventData,
) (
	versionPrevious Version,
	version Version,
	tm time.Time,
	err error,
) {
	return c.impl.Append(ctx, event)
}

// AppendMulti appends one or multiple new events onto the log.
func (c *Client) AppendMulti(
	ctx context.Context,
	events ...EventData,
) (
	versionPrevious Version,
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	if len(events) < 1 {
		err = ErrNoEvents
		return
	}
	return c.impl.AppendMulti(ctx, events...)
}

// AppendCheck appends a new event onto the log
// if the assumed version matches the latest log version,
// otherwise the operation is rejected and ErrMismatchingVersions is returned.
func (c *Client) AppendCheck(
	ctx context.Context,
	assumedVersion Version,
	event EventData,
) (
	version Version,
	tm time.Time,
	err error,
) {
	if assumedVersion == "" {
		err = ErrInvalidVersion
		return
	}
	return c.impl.AppendCheck(ctx, assumedVersion, event)
}

// AppendCheckMulti appends one or multiple new events onto the log
// if the assumed version matches the latest log version,
// otherwise the operation is rejected and ErrMismatchingVersions is returned.
func (c *Client) AppendCheckMulti(
	ctx context.Context,
	assumedVersion Version,
	events ...EventData,
) (
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	if assumedVersion == "" {
		err = ErrInvalidVersion
		return
	}
	if len(events) < 1 {
		err = ErrNoEvents
		return
	}
	return c.impl.AppendCheckMulti(ctx, assumedVersion, events...)
}

// Metadata returns all metadata fields.
func (c *Client) Metadata(ctx context.Context) (map[string]string, error) {
	return c.impl.Metadata(ctx)
}

// Scan reads events at the given version
// calling fn for every received event.
// Scans in reverse if reverse == true.
func (c *Client) Scan(
	ctx context.Context,
	version Version,
	reverse bool,
	fn func(Event) error,
) error {
	return c.impl.Scan(ctx, version, reverse, fn)
}

// VersionInitial returns either the first version of the log or
// "0" if the log is empty.
func (c *Client) VersionInitial(ctx context.Context) (Version, error) {
	return c.impl.VersionInitial(ctx)
}

// Version returns the latest version of the log or
// "0" if the log is empty.
func (c *Client) Version(ctx context.Context) (Version, error) {
	return c.impl.Version(ctx)
}

// Listen establishes a websocket connection to the server
// and starts listening for version update notifications
// calling onUpdate when one is received.
func (c *Client) Listen(ctx context.Context, onUpdate func([]byte)) error {
	return c.impl.Listen(ctx, onUpdate)
}

// TryAppend keeps executing transaction until either cancelled,
// succeeded (assumed == latest event log version) or failed due to an error.
func (c *Client) TryAppend(
	ctx context.Context,
	assumedVersion Version,
	transaction func() (event EventData, err error),
	sync func() (Version, error),
) (
	versionPrevious Version,
	version Version,
	tm time.Time,
	err error,
) {
	// Reapeat until either cancelled, succeeded or failed
	for {
		// Check context for cancelation
		if err = ctx.Err(); err != nil {
			return
		}

		var event EventData
		if event, err = transaction(); err != nil {
			return
		}

		version, tm, err = c.AppendCheck(ctx, assumedVersion, event)
		switch {
		case errors.Is(err, ErrMismatchingVersions):
			// The projection is out of sync,
			// synchronize, update assumed version and repeat
			if assumedVersion, err = sync(); err != nil {
				return
			}
			continue
		case err != nil:
			// Append failed for unexpected reasons
			version = ""
			tm = time.Time{}
			return
		}

		// Transaction successfully committed
		versionPrevious = assumedVersion
		break
	}
	return
}

// TryAppendMulti keeps executing transaction until either cancelled,
// succeeded (assumed == latest event log version) or failed due to an error.
func (c *Client) TryAppendMulti(
	ctx context.Context,
	assumedVersion Version,
	transaction func() (events []EventData, err error),
	sync func() (Version, error),
) (
	versionPrevious Version,
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	// Reapeat until either cancelled, succeeded or failed
	for {
		// Check context for cancelation
		if err = ctx.Err(); err != nil {
			return
		}

		var events []EventData
		if events, err = transaction(); err != nil {
			return
		}

		versionFirst, version, tm, err = c.AppendCheckMulti(
			ctx, assumedVersion, events...,
		)
		switch {
		case errors.Is(err, ErrMismatchingVersions):
			// The projection is out of sync,
			// synchronize, update assumed version and repeat
			if assumedVersion, err = sync(); err != nil {
				return
			}
			continue
		case err != nil:
			// Append failed for unexpected reasons
			versionFirst = ""
			version = ""
			tm = time.Time{}
			return
		}

		// Transaction successfully committed
		versionPrevious = assumedVersion
		break
	}
	return
}

// Error values
var (
	ErrMismatchingVersions = eventlog.ErrMismatchingVersions
	ErrInvalidPayload      = eventlog.ErrInvalidPayload
	ErrInvalidVersion      = errors.New("invalid version")
	ErrMalformedVersion    = errors.New("malformed version")
	ErrLabelTooLong        = fmt.Errorf(
		"label must not exceed %d bytes",
		internal.MaxLabelLen,
	)
	ErrNoEvents = errors.New("no events")
)

type Log interface {
	Printf(format string, v ...interface{})
}

// Connecter represents an eventlog connecter.
type Connecter interface {
	Metadata(ctx context.Context) (map[string]string, error)

	Append(
		ctx context.Context,
		event EventData,
	) (
		versionPrevious Version,
		version Version,
		tm time.Time,
		err error,
	)

	AppendMulti(
		ctx context.Context,
		events ...EventData,
	) (
		versionPrevious Version,
		versionFirst Version,
		version Version,
		tm time.Time,
		err error,
	)

	AppendCheck(
		ctx context.Context,
		assumedVersion Version,
		event EventData,
	) (
		version Version,
		tm time.Time,
		err error,
	)

	AppendCheckMulti(
		ctx context.Context,
		assumedVersion Version,
		eventsEncoded ...EventData,
	) (
		versionFirst Version,
		version Version,
		tm time.Time,
		err error,
	)

	Scan(
		ctx context.Context,
		version Version,
		reverse bool,
		fn func(Event) error,
	) error

	VersionInitial(context.Context) (Version, error)

	Version(context.Context) (Version, error)

	Listen(ctx context.Context, onUpdate func([]byte)) error
}
