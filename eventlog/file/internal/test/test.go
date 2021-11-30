package test

import (
	"io"
	"testing"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal"
	bin "github.com/romshark/eventlog/internal/bin"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func EventLen(e eventlog.Event) int64 {
	return int64(
		8 + // Checksum
			8 + // Timestamp
			2 + // Label length
			4 + // Payload length
			len(e.Label) + // Label
			len(e.PayloadJSON) + // Payload
			8) // Previous version
}

func Checksum(t *testing.T, e eventlog.Event) uint64 {
	checksum, err := internal.Checksum(
		make([]byte, 8),
		xxhash.New(),
		e.Timestamp,
		[]byte(e.Label),
		[]byte(e.PayloadJSON),
		e.VersionPrevious,
	)
	require.NoError(t, err)
	return checksum
}

func LabelLen(e eventlog.Event) uint16 {
	return uint16(len(e.Label))
}

func PayloadLen(e eventlog.Event) uint32 {
	return uint32(len(e.PayloadJSON))
}

func NewBuffer() []byte {
	return make([]byte, file.MinReadBufferLen)
}

type FakeSrc []byte

func (f FakeSrc) ReadAt(buf []byte, offset int64) (read int, err error) {
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}
	d := f[offset:]
	if len(buf) > len(d) {
		buf = buf[:len(d)]
	}
	copy(buf, d)
	return len(buf), nil
}

func (f FakeSrc) Len() (uint64, error) { return uint64(len(f)), nil }

func Compose(
	t *testing.T,
	parts ...interface{},
) (f FakeSrc, headerLength int64) {
	c := bin.Compose(parts...)
	hlen, err := internal.ReadHeader(
		NewBuffer(),
		FakeSrc(c),
		xxhash.New(),
		internal.Config{
			MinPayloadLen: 7,
			MaxPayloadLen: 512,
		},
		nil,
	)
	require.NotZero(t, hlen)
	require.NoError(t, err)
	return FakeSrc(c), hlen
}

func ValidLog(t *testing.T) []eventlog.Event {
	_, _, hlen := ValidFileHeader(t)
	e1 := eventlog.Event{
		Timestamp:       7777777777,
		Version:         uint64(hlen),
		VersionPrevious: 0,
		EventData: eventlog.EventData{
			Label:       []byte(""),
			PayloadJSON: []byte(`{"x":0}`),
		},
	}
	e2 := eventlog.Event{
		Timestamp:       8888888888,
		Version:         uint64(hlen + EventLen(e1)),
		VersionPrevious: e1.Version,
		EventData: eventlog.EventData{
			Label:       []byte("foo_bar"),
			PayloadJSON: []byte(`{"medium":"size"}`),
		},
	}
	e3 := eventlog.Event{
		Timestamp:       9999999999,
		Version:         uint64(hlen + EventLen(e1) + EventLen(e2)),
		VersionPrevious: e2.Version,
		VersionNext:     0,
		EventData: eventlog.EventData{
			Label:       []byte("x"),
			PayloadJSON: []byte(`{"алфавит":"Кириллица"}`),
		},
	}

	e1.VersionNext = e2.Version
	e2.VersionNext = e3.Version

	return []eventlog.Event{e1, e2, e3}
}

func ValidFileHeader(t *testing.T) (
	parts []interface{},
	creation time.Time,
	len int64,
) {
	var h eventlog.Event
	h.PayloadJSON = []byte(`{"m":"d"}`)
	creation = time.Now().UTC()
	return []interface{}{
		// header
		uint32(file.SupportedProtoVersion),
		// metadata
		Checksum(t, h),    // Checksum
		h.Timestamp,       // Timestamp
		LabelLen(h),       // Label length
		PayloadLen(h),     // Payload length
		h.PayloadJSON,     // Payload
		h.VersionPrevious, // Previous version
	}, creation, 4 + EventLen(h)
}

func ComposeWithValidHeader(
	t *testing.T,
	parts ...interface{},
) (f FakeSrc, creation time.Time, headerLength int64) {
	headerParts, creation, headerLen := ValidFileHeader(t)
	return bin.Compose(append(headerParts, parts...)...), creation, headerLen
}
