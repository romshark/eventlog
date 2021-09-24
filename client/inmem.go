package client

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/hex"
)

// Inmem is an in-memory eventlog connecter.
type Inmem struct{ e *eventlog.EventLog }

// NewInmem create a new in-memory eventlog connecter.
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

// Append implements Connecter.Append.
func (c *Inmem) Append(ctx context.Context, event EventData) (
	versionPrevious Version,
	version Version,
	tm time.Time,
	err error,
) {
	pv, v, tm, err := c.e.Append(event)
	if err != nil {
		return
	}
	versionPrevious = fmt.Sprintf("%x", pv)
	version = fmt.Sprintf("%x", v)
	return
}

// AppendMulti implements Connecter.AppendMulti.
func (c *Inmem) AppendMulti(ctx context.Context, events ...EventData) (
	versionPrevious Version,
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	pv, fv, v, tm, err := c.e.AppendMulti(events...)
	if err != nil {
		return
	}
	versionPrevious = fmt.Sprintf("%x", pv)
	versionFirst = fmt.Sprintf("%x", fv)
	version = fmt.Sprintf("%x", v)
	return
}

// AppendCheck implements Connecter.AppendCheck.
func (c *Inmem) AppendCheck(
	ctx context.Context,
	assumedVersion Version,
	event EventData,
) (
	version Version,
	tm time.Time,
	err error,
) {
	var av uint64
	av, err = hex.ReadUint64(unsafeS2B(assumedVersion))
	if err != nil {
		return
	}
	v, tm, err := c.e.AppendCheck(av, event)
	if err != nil {
		return
	}
	version = fmt.Sprintf("%x", v)
	return
}

// AppendCheckMulti implements Connecter.AppendCheckMulti.
func (c *Inmem) AppendCheckMulti(
	ctx context.Context,
	assumedVersion Version,
	events ...EventData,
) (
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	var av uint64
	av, err = hex.ReadUint64(unsafeS2B(assumedVersion))
	if err != nil {
		return
	}
	fv, v, tm, err := c.e.AppendCheckMulti(av, events...)
	if err != nil {
		return
	}
	versionFirst = fmt.Sprintf("%x", fv)
	version = fmt.Sprintf("%x", v)
	return
}

// Scan implements Connecter.Scan.
func (c *Inmem) Scan(
	ctx context.Context,
	version Version,
	reverse bool,
	fn func(Event) error,
) error {
	v, err := hex.ReadUint64(unsafeS2B(version))
	if err != nil {
		return ErrMalformedVersion
	}

	return c.e.Scan(v, reverse, func(v eventlog.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		var d EventData

		d.PayloadJSON = make([]byte, len(v.PayloadJSON))
		copy(d.PayloadJSON, v.PayloadJSON)

		d.Label = make([]byte, len(v.Label))
		copy(d.Label, v.Label)

		return fn(Event{
			Time:            time.Unix(int64(v.Timestamp), 0).UTC(),
			Version:         fmt.Sprintf("%x", v.Version),
			VersionPrevious: fmt.Sprintf("%x", v.VersionPrevious),
			VersionNext:     fmt.Sprintf("%x", v.VersionNext),
			EventData:       d,
		})
	})
}

// VersionInitial implements Connecter.VersionInitial.
func (c *Inmem) VersionInitial(ctx context.Context) (Version, error) {
	return fmt.Sprintf("%x", c.e.VersionInitial()), nil
}

// Version implements Connecter.Version.
func (c *Inmem) Version(ctx context.Context) (Version, error) {
	return fmt.Sprintf("%x", c.e.Version()), nil
}

// Listen implements Connecter.Listen.
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
