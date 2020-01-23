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
		Timestamp: uint64(time.Now().Unix()),
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

// Scan reads a maximum of n events starting at the given offset.
// If offset+n exceeds the length of the log then a smaller number
// of events is returned. Of n is 0 then all events starting at the
// given offset are returned
func (l *Inmem) Scan(offset uint64, n uint64, fn eventlog.ScanFn) error {
	defer l.lock.RUnlock()
	l.lock.RLock()

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
		fn(e.Timestamp, e.Payload)
	}
	return nil
}

// Append appends a new entry onto the event log
func (l *Inmem) Append(payload []byte) error {
	if err := eventlog.VerifyPayload(payload); err != nil {
		return err
	}

	ev := newInmemEvent(payload)

	defer l.lock.Unlock()
	l.lock.Lock()

	l.store = append(l.store, ev)

	return nil
}

// AppendCheck appends a new entry onto the event log
// expecting offset to be the offset of the last entry plus 1
func (l *Inmem) AppendCheck(
	offset uint64,
	payload []byte,
) error {
	if err := eventlog.VerifyPayload(payload); err != nil {
		return err
	}

	ev := newInmemEvent(payload)

	defer l.lock.Unlock()
	l.lock.Lock()

	if offset != uint64(len(l.store)) {
		return eventlog.ErrMismatchingVersions
	}

	l.store = append(l.store, ev)

	return nil
}
