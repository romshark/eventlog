package inmem

import (
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// Make sure *Inmem implements eventlog.Implementer
var _ eventlog.Implementer = new(Inmem)

type inmemEvent struct {
	Timestamp uint64
	Payload   []byte
}

func newInmemEvent(payload []byte, tm time.Time) inmemEvent {
	p := make([]byte, len(payload))
	copy(p, payload)

	return inmemEvent{
		Timestamp: uint64(tm.UTC().Unix()),
		Payload:   p,
	}
}

// Inmem is a volatile in-memory event log
type Inmem struct {
	lock  sync.RWMutex
	store []inmemEvent
}

// NewInmem returns a new volatile in-memory event log instance
func NewInmem() (*Inmem, error) {
	return &Inmem{}, nil
}

func (l *Inmem) Version() uint64 {
	l.lock.RLock()
	v := len(l.store)
	l.lock.RUnlock()
	return uint64(v)
}

func (l *Inmem) FirstOffset() uint64 { return 0 }

// Scan reads a maximum of n events starting at the given offset.
// If offset+n exceeds the length of the log then a smaller number
// of events is returned. Of n is 0 then all events starting at the
// given offset are returned
func (l *Inmem) Scan(
	offset uint64,
	n uint64,
	fn eventlog.ScanFn,
) (
	nextOffset uint64,
	err error,
) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	ln := uint64(len(l.store))

	if offset >= ln {
		return 0, eventlog.ErrOffsetOutOfBound
	}

	var events []inmemEvent
	if n > 0 {
		if offset+n > ln {
			events = l.store[offset:]
			nextOffset = offset + uint64(len(events))
		} else {
			nextOffset = offset + n
			events = l.store[offset:nextOffset]
		}
	} else {
		events = l.store[offset:]
		nextOffset = offset + uint64(len(events))
	}

	for _, e := range events {
		if err = fn(e.Timestamp, e.Payload, offset); err != nil {
			return
		}
		offset++
	}

	return
}

func (l *Inmem) Append(payloadJSON []byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now()
	ev := newInmemEvent(payloadJSON, tm)

	l.lock.Lock()
	defer l.lock.Unlock()

	l.store = append(l.store, ev)
	ln := uint64(len(l.store))

	offset = ln - 1
	newVersion = ln
	return
}

func (l *Inmem) AppendMulti(payloadsJSON ...[]byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	ev := make([]inmemEvent, len(payloadsJSON))
	tm = time.Now()
	for i, p := range payloadsJSON {
		ev[i] = newInmemEvent(p, tm)
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	offset = uint64(len(l.store))
	l.store = append(l.store, ev...)
	newVersion = uint64(len(l.store))
	return
}

func (l *Inmem) AppendCheck(
	assumedVersion uint64,
	payloadJSON []byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now()
	ev := newInmemEvent(payloadJSON, tm)

	l.lock.Lock()
	defer l.lock.Unlock()

	if assumedVersion != uint64(len(l.store)) {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	l.store = append(l.store, ev)
	ln := uint64(len(l.store))

	offset = ln - 1
	newVersion = ln
	return
}

func (l *Inmem) AppendCheckMulti(
	assumedVersion uint64,
	payloadsJSON ...[]byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	ev := make([]inmemEvent, len(payloadsJSON))
	tm = time.Now()
	for i, p := range payloadsJSON {
		ev[i] = newInmemEvent(p, tm)
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	if assumedVersion != uint64(len(l.store)) {
		err = eventlog.ErrMismatchingVersions
		return
	}

	l.store = append(l.store, ev...)
	ln := uint64(len(l.store))

	offset = ln - 1
	newVersion = ln
	return
}
