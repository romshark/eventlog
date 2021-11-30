package eventlog

import (
	"errors"
	"fmt"
	"time"

	"github.com/romshark/eventlog/internal"
	"github.com/romshark/eventlog/internal/broadcast"
	"github.com/romshark/eventlog/internal/jsonminify"
)

// ScanFn is called by EventLog.Scan for each scanned event.
type ScanFn func(Event) error

type Event struct {
	EventData
	Timestamp       uint64
	Version         uint64
	VersionNext     uint64
	VersionPrevious uint64
}

type EventData struct {
	Label       []byte
	PayloadJSON []byte
}

// Validate validates the label and JSON payload.
func (e EventData) Validate() error {
	if err := ValidateLabel(e.Label); err != nil {
		return err
	}
	if err := ValidatePayloadJSON(e.PayloadJSON); err != nil {
		return err
	}
	return nil
}

// EventLogger represents an abstract event logger.
type EventLogger interface {
	// MetadataLen returns the number of metadata fields.
	MetadataLen() int

	// ScanMetadata iterates over all metadata fields calling fn for each.
	// The scan is interrupted if fn returns false.
	ScanMetadata(fn func(field, value string) bool)

	// Version returns the latest version of the log.
	Version() uint64

	// VersionInitial returns either the initial version of the log
	// or 0 if the log is empty.
	VersionInitial() uint64

	// Append appends an event with the given payload to the log.
	Append(event EventData) (
		versionPrevious uint64,
		version uint64,
		tm time.Time,
		err error,
	)

	// AppendMulti appends multiple events with the given payloads to the log.
	AppendMulti(events ...EventData) (
		versionPrevious uint64,
		versionFirst uint64,
		version uint64,
		tm time.Time,
		err error,
	)

	// AppendCheck appends an event with the given payload
	// only if assumedVersion matches the latest version of the log,
	// otherwise the event is rejected and ErrMismatchingVersions is returned.
	AppendCheck(
		assumedVersion uint64,
		event EventData,
	) (
		version uint64,
		tm time.Time,
		err error,
	)

	// AppendCheckMulti appends multiple events with the given payloads
	// only if assumedVersion matches the latest version of the log,
	// otherwise the events are rejected and ErrMismatchingVersions is returned.
	AppendCheckMulti(
		assumedVersion uint64,
		events ...EventData,
	) (
		versionFirst uint64,
		version uint64,
		tm time.Time,
		err error,
	)

	// Scan reads events starting (including) at the given version.
	// Events are read in reversed order if reverse == true.
	// fn is called for every scanned event.
	Scan(version uint64, reverse bool, fn ScanFn) error

	Close() error
}

var (
	ErrMismatchingVersions = errors.New("mismatching versions")
	ErrInvalidVersion      = errors.New("invalid version")
	ErrLabelTooLong        = fmt.Errorf(
		"label must not exceed %d bytes",
		internal.MaxLabelLen,
	)
	ErrLabelContainsIllegalChars = errors.New(
		"label contains illegal characters",
	)
	ErrPayloadSizeLimitExceeded = errors.New("payload size limit exceeded")
)

type EventLog struct {
	impl      EventLogger
	broadcast *broadcast.Broadcast
}

func New(impl EventLogger) *EventLog {
	return &EventLog{
		impl:      impl,
		broadcast: broadcast.New(),
	}
}

// Version returns the latest version of the log.
func (e *EventLog) Version() uint64 {
	return e.impl.Version()
}

// VersionInitial returns either the version of the first event in the log or
// 0 if the log is empty.
func (e *EventLog) VersionInitial() uint64 {
	return e.impl.VersionInitial()
}

// MetadataLen returns the number of metadata fields.
func (e *EventLog) MetadataLen() int {
	return e.impl.MetadataLen()
}

// ScanMetadata iterates over all metadata fields calling fn for each.
// The scan is interrupted if fn returns false.
func (e *EventLog) ScanMetadata(fn func(field, value string) bool) {
	e.impl.ScanMetadata(fn)
}

// Append appends an event with the given payload to the log.
func (e *EventLog) Append(event EventData) (
	versionPrevious uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	if err = event.Validate(); err != nil {
		return
	}
	event.PayloadJSON = jsonminify.Minify(event.PayloadJSON)

	if versionPrevious, version, tm, err = e.impl.Append(event); err != nil {
		return
	}

	e.broadcast.Broadcast(version)
	return
}

// AppendMulti appends multiple events with the given payloads to the log.
func (e *EventLog) AppendMulti(events ...EventData) (
	versionPrevious uint64,
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	for _, e := range events {
		if err = e.Validate(); err != nil {
			return
		}
	}
	for i := range events {
		events[i].PayloadJSON = jsonminify.Minify(events[i].PayloadJSON)
	}

	if versionPrevious, versionFirst, version, tm, err =
		e.impl.AppendMulti(events...); err != nil {
		return
	}

	e.broadcast.Broadcast(version)
	return
}

// AppendCheck appends an event with the given payload
// only if assumedVersion matches the latest version of the log,
// otherwise the event is rejected and ErrMismatchingVersions is returned.
func (e *EventLog) AppendCheck(
	assumedVersion uint64,
	event EventData,
) (
	version uint64,
	tm time.Time,
	err error,
) {
	if err = event.Validate(); err != nil {
		return
	}
	event.PayloadJSON = jsonminify.Minify(event.PayloadJSON)

	if version, tm, err = e.impl.AppendCheck(
		assumedVersion,
		event,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(version)
	return
}

// AppendCheckMulti appends multiple events with the given payloads
// only if assumedVersion matches the latest version of the log,
// otherwise the events are rejected and ErrMismatchingVersions is returned.
func (e *EventLog) AppendCheckMulti(
	assumedVersion uint64,
	events ...EventData,
) (
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	for _, e := range events {
		if err = e.Validate(); err != nil {
			return
		}
	}
	for i := range events {
		events[i].PayloadJSON = jsonminify.Minify(events[i].PayloadJSON)
	}

	if versionFirst, version, tm, err = e.impl.AppendCheckMulti(
		assumedVersion, events...,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(version)
	return
}

// Scan reads events starting at the given version.
// Events are read in reversed order if reverse == true.
// fn is called for every scanned event.
//
// WARNING: Calling Append, AppendMulti, AppendCheck, AppendCheckMulti
// and Close in fn will cause a deadlock!
func (e *EventLog) Scan(version uint64, reverse bool, fn ScanFn) error {
	return e.impl.Scan(version, reverse, fn)
}

// Close closes the eventlog.
//
// WARNING: The eventlog instance must not be used after close.
func (e *EventLog) Close() error {
	if err := e.impl.Close(); err != nil {
		return err
	}
	return nil
}

// Subscribe creates an update subscription returning
// a channel that's triggered when a push is performed successfully.
func (e *EventLog) Subscribe() (channel <-chan uint64, close func()) {
	c := make(chan uint64)
	return c, e.broadcast.Subscribe(c)
}
