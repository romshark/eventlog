package eventlog

import (
	"errors"
	"time"

	"github.com/valyala/fastjson"
)

// ScanFn is called by EventLog.Scan for each scanned event
type ScanFn func(timestamp uint64, payload []byte, offset uint64) error

// EventLog represents an event log
type EventLog interface {
	// Version returns the current version of the log
	Version() uint64

	// FirstOffset returns the offset of the first entry in the log
	FirstOffset() uint64

	// Append appends an event with the given payload to the log
	Append(payload []byte) (
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
		payload []byte,
	) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	)

	// Scan reads n events at the given offset
	// calling the given callback function for each read entry.
	// The scan is resumed as long as there are events to be read
	// and the callback function returns true
	Scan(offset uint64, n uint64, fn ScanFn) error
}

var (
	// ErrOffsetOutOfBound indicates an out-of-bound offset
	ErrOffsetOutOfBound = errors.New("offset out of bound")

	// ErrMismatchingVersions indicates mismatching versions
	ErrMismatchingVersions = errors.New("mismatching versions")

	// ErrInvalidPayload indicates an invalid event payload
	ErrInvalidPayload = errors.New("invalid payload")
)

// VerifyPayload returns an error if the given JSON payload is invalid
func VerifyPayload(payload []byte) error {
	if len(payload) < 1 {
		return ErrInvalidPayload
	}

	i := 0
	inObject := false
CHECK_INIT:
	for ; i < len(payload); i++ {
		switch payload[i] {
		case 0x20:
			// Space
		case 0x09:
			// Horizontal tab
		case 0x0A:
			// Line feed
		case 0x0D:
			// Carriage return
		case 0x7B:
			// Object open
			inObject = true
			break CHECK_INIT
		default:
			// The document doesn't begin with a '{'
			return ErrInvalidPayload
		}
	}

	if !inObject {
		return ErrInvalidPayload
	}

CHECK_INOBJ:
	for i++; i < len(payload); i++ {
		switch payload[i] {
		case 0x20:
			// Space
		case 0x09:
			// Horizontal tab
		case 0x0A:
			// Line feed
		case 0x0D:
			// Carriage return
		case 0x7D:
			// Object close
			// The '{' is directly followed by a '}', the object's empty
			return ErrInvalidPayload
		default:
			break CHECK_INOBJ
		}
	}

	if err := fastjson.ValidateBytes(payload); err != nil {
		return ErrInvalidPayload
	}
	return nil
}
