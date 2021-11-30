package inmem

import (
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// Make sure *Inmem implements EventLogger
var _ eventlog.EventLogger = new(Inmem)

type inmemEvent struct {
	Timestamp uint64
	Label     []byte
	Payload   []byte
}

func newInmemEvent(event eventlog.EventData, tm time.Time) inmemEvent {
	l := make([]byte, len(event.Label))
	copy(l, event.Label)

	p := make([]byte, len(event.PayloadJSON))
	copy(p, event.PayloadJSON)

	return inmemEvent{
		Timestamp: uint64(tm.UTC().Unix()),
		Label:     l,
		Payload:   p,
	}
}

// Inmem is a volatile in-memory event log
type Inmem struct {
	payloadLimit int
	metadata     map[string]string
	lock         sync.RWMutex
	store        []inmemEvent
}

// New returns a new volatile in-memory event log instance
func New(
	payloadLimit int,
	metadata map[string]string,
) *Inmem {
	return &Inmem{
		payloadLimit: payloadLimit,
		metadata:     metadata,
	}
}

func (m *Inmem) checkLimit(payloadJSON []byte) error {
	if len(payloadJSON) > m.payloadLimit {
		return eventlog.ErrPayloadSizeLimitExceeded
	}
	return nil
}

func (m *Inmem) MetadataLen() int {
	return len(m.metadata)
}

func (m *Inmem) ScanMetadata(fn func(field, value string) bool) {
	for f, v := range m.metadata {
		if !fn(f, v) {
			return
		}
	}
}

func (m *Inmem) Version() uint64 {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return uint64(len(m.store))
}

func (m *Inmem) VersionInitial() uint64 {
	m.lock.RLock()
	ln := len(m.store)
	m.lock.RUnlock()
	if ln < 1 {
		return 0
	}
	return 1
}

// Scan reads events starting at the given version.
// Events are read in reversed order if reverse == true.
// fn is called for every scanned event.
//
// WARNING: Calling Append, AppendMulti, AppendCheck, AppendCheckMulti
// and Close in fn will cause a deadlock!
func (m *Inmem) Scan(
	version uint64,
	reverse bool,
	fn eventlog.ScanFn,
) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	ln := uint64(len(m.store))

	if version > ln || version < 1 {
		return eventlog.ErrInvalidVersion
	}

	versionPrevious := version - 1
	nextVersion := version + 1

	if reverse {
		for ; version > 0; version-- {
			e := m.store[version-1]

			pv := versionPrevious
			if version < 2 {
				pv = 0
			}
			nv := nextVersion
			if version >= ln {
				nv = 0
			}

			if err := fn(eventlog.Event{
				Version:         version,
				VersionPrevious: pv,
				VersionNext:     nv,
				Timestamp:       e.Timestamp,
				EventData: eventlog.EventData{
					Label:       e.Label,
					PayloadJSON: e.Payload,
				},
			}); err != nil {
				return err
			}
			nextVersion--
			versionPrevious--
		}
	} else {
		for ; version <= ln; version++ {
			e := m.store[version-1]

			pv := versionPrevious
			if version < 2 {
				pv = 0
			}
			nv := nextVersion
			if version >= ln {
				nv = 0
			}

			if err := fn(eventlog.Event{
				Version:         version,
				VersionPrevious: pv,
				VersionNext:     nv,
				Timestamp:       e.Timestamp,
				EventData: eventlog.EventData{
					Label:       e.Label,
					PayloadJSON: e.Payload,
				},
			}); err != nil {
				return err
			}
			nextVersion++
			versionPrevious++
		}
	}
	return nil
}

func (m *Inmem) Append(event eventlog.EventData) (
	versionPrevious uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	if err = m.checkLimit(event.PayloadJSON); err != nil {
		return
	}

	tm = time.Now().UTC()
	ev := newInmemEvent(event, tm)

	m.lock.Lock()
	defer m.lock.Unlock()

	versionPrevious = uint64(len(m.store))
	m.store = append(m.store, ev)
	version = uint64(len(m.store))
	return
}

func (m *Inmem) AppendMulti(events ...eventlog.EventData) (
	versionPrevious uint64,
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	for _, e := range events {
		if err = m.checkLimit(e.PayloadJSON); err != nil {
			return
		}
	}

	ev := make([]inmemEvent, len(events))
	tm = time.Now().UTC()
	for i, e := range events {
		ev[i] = newInmemEvent(e, tm)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if len(events) < 1 {
		tm = time.Time{}
		versionPrevious = uint64(len(m.store))
		version = uint64(len(m.store))
		return
	}

	versionPrevious = uint64(len(m.store))
	versionFirst = uint64(len(m.store)) + 1
	m.store = append(m.store, ev...)
	version = uint64(len(m.store))
	return
}

func (m *Inmem) AppendCheck(
	assumedVersion uint64,
	event eventlog.EventData,
) (
	version uint64,
	tm time.Time,
	err error,
) {
	if err = m.checkLimit(event.PayloadJSON); err != nil {
		return
	}

	tm = time.Now().UTC()
	ev := newInmemEvent(event, tm)

	m.lock.Lock()
	defer m.lock.Unlock()

	if assumedVersion != uint64(len(m.store)) {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	m.store = append(m.store, ev)
	version = uint64(len(m.store))
	return
}

func (m *Inmem) AppendCheckMulti(
	assumedVersion uint64,
	events ...eventlog.EventData,
) (
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	for _, e := range events {
		if err = m.checkLimit(e.PayloadJSON); err != nil {
			return
		}
	}

	ev := make([]inmemEvent, len(events))
	tm = time.Now().UTC()
	for i, e := range events {
		ev[i] = newInmemEvent(e, tm)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if assumedVersion != uint64(len(m.store)) {
		err = eventlog.ErrMismatchingVersions
		return
	}

	if len(events) < 1 {
		tm = time.Time{}
		version = uint64(len(m.store))
		return
	}

	versionFirst = uint64(len(m.store)) + 1
	m.store = append(m.store, ev...)
	version = uint64(len(m.store))
	return
}

func (m *Inmem) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.store = []inmemEvent{}
	return nil
}
