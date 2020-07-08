package eventlog

import (
	"errors"
	"time"

	"github.com/romshark/eventlog/internal/broadcast"
	"github.com/romshark/eventlog/internal/jsonminify"
)

// ScanFn is called by EventLog.Scan for each scanned event
type ScanFn func(timestamp uint64, payloadJSON []byte, offset uint64) error

// Implementer represents an event log engine's implementer
type Implementer interface {
	// Version returns the current version of the log
	Version() uint64

	// FirstOffset returns the offset of the first entry in the log
	FirstOffset() uint64

	// Append appends an event with the given payload to the log
	Append(payloadJSON []byte) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	)

	// Append appends multiple events with the given payloads to the log
	AppendMulti(payloadsJSON ...[]byte) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	)

	// AppendCheck appends an event with the given payload
	// only if the offset matches the offset of the last
	// entry in the log minus 1. If the offset doesn't match
	// the log's version it will be rejected and ErrMismatchingVersions
	// is returned instead
	AppendCheck(
		assumedVersion uint64,
		payloadJSON []byte,
	) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	)

	// AppendCheckMulti appends multiple events with the given payloads
	// only if the offset matches the offset of the last
	// entry in the log minus 1. If the offset doesn't match
	// the log's version it will be rejected and ErrMismatchingVersions
	// is returned instead
	AppendCheckMulti(
		assumedVersion uint64,
		payloadsJSON ...[]byte,
	) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	)

	// Scan reads n events at the given offset
	// calling the given callback function for each read entry.
	// The scan is resumed as long as there are events to be read
	// and the callback function returns true.
	// If the returned nextOffset is 0 then there are no more
	// entries to be scanned after the last scanned one
	Scan(
		offset uint64,
		n uint64,
		fn ScanFn,
	) (
		nextOffset uint64,
		err error,
	)

	Close() error
}

var (
	ErrOffsetOutOfBound    = errors.New("offset out of bound")
	ErrMismatchingVersions = errors.New("mismatching versions")
	ErrInvalidOffset       = errors.New("invalid offset")
)

type EventLog struct {
	impl      Implementer
	broadcast *broadcast.Broadcast
}

func New(impl Implementer) *EventLog {
	return &EventLog{
		impl:      impl,
		broadcast: broadcast.New(),
	}
}

// Version returns the current version of the log
func (e *EventLog) Version() uint64 {
	return e.impl.Version()
}

// FirstOffset returns the offset of the first entry in the log
func (e *EventLog) FirstOffset() uint64 {
	return e.impl.FirstOffset()
}

// Append appends an event with the given payload to the log
func (e *EventLog) Append(payloadJSON []byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = ValidatePayloadJSON(payloadJSON); err != nil {
		return
	}
	payloadJSON = jsonminify.Minify(payloadJSON)

	if offset, newVersion, tm, err = e.impl.Append(
		payloadJSON,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(newVersion)
	return
}

// AppendMulti appends multiple events with the given payloads to the log
func (e *EventLog) AppendMulti(payloadsJSON ...[]byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	for _, p := range payloadsJSON {
		if err = ValidatePayloadJSON(p); err != nil {
			return
		}
	}
	for i, p := range payloadsJSON {
		payloadsJSON[i] = jsonminify.Minify(p)
	}

	if offset, newVersion, tm, err = e.impl.AppendMulti(
		payloadsJSON...,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(newVersion)
	return
}

// AppendCheck appends an event with the given payload
// only if the offset matches the offset of the last
// entry in the log minus 1. If the offset doesn't match
// the log's version it will be rejected and ErrMismatchingVersions
// is returned instead
func (e *EventLog) AppendCheck(
	assumedVersion uint64,
	payloadJSON []byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = ValidatePayloadJSON(payloadJSON); err != nil {
		return
	}
	payloadJSON = jsonminify.Minify(payloadJSON)

	if offset, newVersion, tm, err = e.impl.AppendCheck(
		assumedVersion,
		payloadJSON,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(newVersion)
	return
}

// AppendCheckMulti appends multiple events with the given payloads
// only if the offset matches the offset of the last
// entry in the log minus 1. If the offset doesn't match
// the log's version it will be rejected and ErrMismatchingVersions
// is returned instead
func (e *EventLog) AppendCheckMulti(
	assumedVersion uint64,
	payloadsJSON ...[]byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	for _, p := range payloadsJSON {
		if err = ValidatePayloadJSON(p); err != nil {
			return
		}
	}
	for i, p := range payloadsJSON {
		payloadsJSON[i] = jsonminify.Minify(p)
	}

	if offset, newVersion, tm, err = e.impl.AppendCheckMulti(
		assumedVersion,
		payloadsJSON...,
	); err != nil {
		return
	}

	e.broadcast.Broadcast(newVersion)
	return
}

// Scan reads n events at the given offset
// calling the given callback function for each read entry.
// The scan is resumed as long as there are events to be read
// and the callback function returns true.
// If the returned nextOffset is 0 then there are no more
// entries to be scanned after the last scanned one
func (e *EventLog) Scan(
	offset uint64,
	n uint64,
	fn ScanFn,
) (
	nextOffset uint64,
	err error,
) {
	return e.impl.Scan(offset, n, fn)
}

func (e *EventLog) Close() error {
	if err := e.impl.Close(); err != nil {
		return err
	}
	return nil
}

// Subscribe creates an update subscription returning
// a channel that's triggered when a push is performed successfuly
func (e *EventLog) Subscribe() (channel <-chan uint64, close func()) {
	c := make(chan uint64)
	return c, e.broadcast.Subscribe(c)
}
