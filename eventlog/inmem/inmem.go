package inmem

import (
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// Make sure *Inmem implements EventLog
var _ eventlog.EventLog = new(Inmem)

type inmemEvent struct {
	Timestamp uint64
	Payload   []byte
}

func newInmemEvent(payload []byte) inmemEvent {
	p := make([]byte, len(payload))
	copy(p, payload)

	return inmemEvent{
		Timestamp: uint64(time.Now().UTC().Unix()),
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

// Version implements EventLog.Version
func (l *Inmem) Version() uint64 {
	l.lock.RLock()
	v := len(l.store)
	l.lock.RUnlock()
	return uint64(v)
}

// FirstOffset implements EventLog.FirstOffset
func (l *Inmem) FirstOffset() uint64 { return 0 }

// Scan reads a maximum of n events starting at the given offset.
// If offset+n exceeds the length of the log then a smaller number
// of events is returned. Of n is 0 then all events starting at the
// given offset are returned
func (l *Inmem) Scan(offset uint64, n uint64, fn eventlog.ScanFn) error {
	l.lock.RLock()
	defer l.lock.RUnlock()

	ln := uint64(len(l.store))

	if offset >= ln {
		return eventlog.ErrOffsetOutOfBound
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
		if err := fn(e.Timestamp, e.Payload, offset); err != nil {
			return err
		}
		offset++
	}
	return nil
}

// Append appends a new entry onto the event log
func (l *Inmem) Append(payload []byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = eventlog.VerifyPayload(payload); err != nil {
		return
	}

	ev := newInmemEvent(payload)

	l.lock.Lock()
	defer l.lock.Unlock()

	l.store = append(l.store, ev)
	ln := uint64(len(l.store))

	tm = time.Unix(int64(ev.Timestamp), 0)
	offset = ln - 1
	newVersion = ln
	return
}

// AppendCheck appends a new entry onto the event log
// expecting offset to be the offset of the last entry plus 1
func (l *Inmem) AppendCheck(
	assumedVersion uint64,
	payload []byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = eventlog.VerifyPayload(payload); err != nil {
		return
	}

	ev := newInmemEvent(payload)

	l.lock.Lock()
	defer l.lock.Unlock()

	if assumedVersion != uint64(len(l.store)) {
		err = eventlog.ErrMismatchingVersions
		return
	}

	l.store = append(l.store, ev)
	ln := uint64(len(l.store))

	tm = time.Unix(int64(ev.Timestamp), 0)
	offset = ln - 1
	newVersion = ln
	return
}
