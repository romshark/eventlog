package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/hex"
)

type Inmem struct {
	e *eventlog.EventLog
}

func NewInmem(e *eventlog.EventLog) *Inmem {
	return &Inmem{e}
}

func (c *Inmem) AppendJSON(
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
	var of, nv uint64
	if assumeVersion {
		var av uint64
		av, err = hex.ReadUint64(unsafeStringToBytes(assumedVersion))
		if err != nil {
			return
		}

		if payloadJSON[0] == '{' {
			of, nv, tm, err = c.e.AppendCheck(av, payloadJSON)
		} else {
			var b [][]byte
			if b, err = unmarshalMultiple(payloadJSON); err != nil {
				return
			}
			of, nv, tm, err = c.e.AppendCheckMulti(av, b...)
		}
	} else {
		if payloadJSON[0] == '{' {
			of, nv, tm, err = c.e.Append(payloadJSON)
		} else {
			var b [][]byte
			if b, err = unmarshalMultiple(payloadJSON); err != nil {
				return
			}
			of, nv, tm, err = c.e.AppendMulti(b...)
		}
	}
	if err != nil {
		return
	}

	offset = fmt.Sprintf("%x", of)
	newVersion = fmt.Sprintf("%x", nv)
	return
}

// Read implements Client.Read
func (c *Inmem) Read(
	ctx context.Context,
	offset string,
) (Event, error) {
	var e Event
	of, err := hex.ReadUint64(unsafeStringToBytes(offset))
	if err != nil {
		return Event{}, err
	}

	next, err := c.e.Scan(of, 1, func(
		timestamp uint64,
		payloadJSON []byte,
		offset uint64,
	) error {
		p := make([]byte, len(payloadJSON))
		copy(p, payloadJSON)

		e.Offset = fmt.Sprintf("%x", offset)
		e.Time = time.Unix(int64(timestamp), 0)
		e.Payload = p
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	e.Next = fmt.Sprintf("%x", next)
	return e, nil
}

// Begin implements Client.Begin
func (c *Inmem) Begin(ctx context.Context) (string, error) {
	return fmt.Sprintf("%x", c.e.FirstOffset()), nil
}

func (c *Inmem) Version(ctx context.Context) (string, error) {
	return fmt.Sprintf("%x", c.e.Version()), nil
}

// Listen establishes a websocket connection to the server
// and starts listening for version update notifications
// calling onUpdate when one is received.
func (c *Inmem) Listen(ctx context.Context, onUpdate func([]byte)) error {
	ch, cancel := c.e.Subscribe()
	done := make(chan struct{})
	defer func() {
		cancel()
		close(done)
	}()

	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()

	for v := range ch {
		onUpdate([]byte(fmt.Sprintf("%x", v)))
	}
	return ctx.Err()
}

func unsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func unmarshalMultiple(b []byte) ([][]byte, error) {
	var a []json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, eventlog.ErrInvalidPayload
	}
	return *(*[][]byte)(unsafe.Pointer(&a)), nil
}
