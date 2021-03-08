package client

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/hex"
	"github.com/romshark/eventlog/internal/msgcodec"
)

type Inmem struct {
	e *eventlog.EventLog
}

func NewInmem(e *eventlog.EventLog) *Inmem {
	return &Inmem{e}
}

func (c *Inmem) Metadata(ctx context.Context) (
	m map[string]string,
	err error,
) {
	l := c.e.MetadataLen()
	if l < 1 {
		return nil, nil
	}
	m = make(map[string]string, l)
	c.e.ScanMetadata(func(f, v string) bool {
		if err = ctx.Err(); err != nil {
			return false
		}
		m[f] = v
		return true
	})
	return m, nil
}

func (c *Inmem) Append(
	ctx context.Context,
	assumeVersion bool,
	assumedVersion string,
	eventsEncoded []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	var events []eventlog.Event
	if err = msgcodec.ScanBytesBinary(
		eventsEncoded,
		func(n int) error {
			events = make([]eventlog.Event, 0, n)
			return nil
		},
		func(label []byte, payloadJSON []byte) error {
			events = append(events, eventlog.Event{
				Label:       string(label),
				PayloadJSON: payloadJSON,
			})
			return nil
		},
	); err != nil {
		if err == msgcodec.ErrMalformedMessage {
			err = ErrInvalidPayload
		}
		return
	}

	var of, nv uint64
	if assumeVersion {
		var av uint64
		av, err = hex.ReadUint64(unsafeS2B(assumedVersion))
		if err != nil {
			return
		}

		if len(events) < 2 {
			of, nv, tm, err = c.e.AppendCheck(av, events[0])
		} else {
			of, nv, tm, err = c.e.AppendCheckMulti(av, events...)
		}
	} else {
		if len(events) < 2 {
			of, nv, tm, err = c.e.Append(events[0])
		} else {
			of, nv, tm, err = c.e.AppendMulti(events...)
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
	of, err := hex.ReadUint64(unsafeS2B(offset))
	if err != nil {
		return Event{}, err
	}

	next, err := c.e.Scan(of, 1, func(
		offset uint64,
		timestamp uint64,
		label []byte,
		payloadJSON []byte,
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

func unsafeS2B(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}
