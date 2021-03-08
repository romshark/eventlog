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
	Label     []byte
	Payload   []byte
}

func newInmemEvent(event eventlog.Event, tm time.Time) inmemEvent {
	p := make([]byte, len(event.PayloadJSON))
	copy(p, event.PayloadJSON)

	return inmemEvent{
		Timestamp: uint64(tm.UTC().Unix()),
		Label:     []byte(event.Label),
		Payload:   p,
	}
}

// Inmem is a volatile in-memory event log
type Inmem struct {
	metadata map[string]string
	lock     sync.RWMutex
	store    []inmemEvent
}

// New returns a new volatile in-memory event log instance
func New(metadata map[string]string) *Inmem {
	return &Inmem{
		metadata: metadata,
	}
}

func (l *Inmem) MetadataLen() int {
	return len(l.metadata)
}

func (l *Inmem) ScanMetadata(fn func(field, value string) bool) {
	for f, v := range l.metadata {
		if !fn(f, v) {
			return
		}
	}
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
		} else {
			events = l.store[offset : offset+n]
		}
	} else {
		events = l.store[offset:]
	}

	for _, e := range events {
		if err = fn(offset, e.Timestamp, e.Label, e.Payload); err != nil {
			nextOffset = offset + 1
			return
		}
		offset++
	}
	nextOffset = offset

	return
}

func (l *Inmem) Append(event eventlog.Event) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now()
	ev := newInmemEvent(event, tm)

	l.lock.Lock()
	defer l.lock.Unlock()

	l.store = append(l.store, ev)
	ln := uint64(len(l.store))

	offset = ln - 1
	newVersion = ln
	return
}

func (l *Inmem) AppendMulti(events ...eventlog.Event) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	ev := make([]inmemEvent, len(events))
	tm = time.Now()
	for i, e := range events {
		ev[i] = newInmemEvent(e, tm)
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
	event eventlog.Event,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now()
	ev := newInmemEvent(event, tm)

	l.lock.Lock()
	defer l.lock.Unlock()

	if assumedVersion != uint64(len(l.store)) {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	offset = uint64(len(l.store))
	l.store = append(l.store, ev)
	newVersion = uint64(len(l.store))
	return
}

func (l *Inmem) AppendCheckMulti(
	assumedVersion uint64,
	events ...eventlog.Event,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	ev := make([]inmemEvent, len(events))
	tm = time.Now()
	for i, e := range events {
		ev[i] = newInmemEvent(e, tm)
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	if assumedVersion != uint64(len(l.store)) {
		err = eventlog.ErrMismatchingVersions
		return
	}

	offset = uint64(len(l.store))
	l.store = append(l.store, ev...)
	newVersion = uint64(len(l.store))
	return
}

func (l *Inmem) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.store = []inmemEvent{}
	return nil
}
