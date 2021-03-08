package test

import (
	"io"
	"testing"

	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal"
	bin "github.com/romshark/eventlog/internal/bin"

	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/require"
)

func NewBuffer() internal.ReadBuffer {
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

func Compose(
	t *testing.T,
	parts ...interface{},
) (f FakeSrc, headerLength int64) {
	c := bin.Compose(parts...)
	hlen, err := internal.ReadHeader(
		NewBuffer(),
		FakeSrc(c),
		xxhash.New(),
		internal.ReaderConf{
			MinPayloadLen: 7,
			MaxPayloadLen: 512,
		},
		nil,
	)
	require.NotZero(t, hlen)
	require.NoError(t, err)
	return FakeSrc(c), hlen
}

func ComposeWithValidHeader(
	t *testing.T,
	parts ...interface{},
) (f FakeSrc, headerLength int64) {
	m := TestEvent{
		Timestamp: 0,
		Label:     "",
		Payload:   `{"m":"d"}`,
	}
	header := []interface{}{
		// header
		uint32(file.SupportedProtoVersion),
		// metadata
		m.Checksum(t),  // checksum
		m.Timestamp,    // timestamp
		m.LabelLen(),   // label length
		m.PayloadLen(), // payload length
		m.Payload,      // payload
	}
	return bin.Compose(append(header, parts...)...), 35
}

func ValidEvent1() TestEvent {
	return TestEvent{
		Timestamp: 8888888888,
		Label:     "",
		Payload:   `{"x":0}`,
	}
}

func ValidEvent2() TestEvent {
	return TestEvent{
		Timestamp: 9999999999,
		Label:     "foo_bar",
		Payload:   `{"medium":"size"}`,
	}
}

type TestEvent struct {
	Timestamp uint64
	Label     string
	Payload   string
}

func (e TestEvent) LabelLen() uint16 {
	return uint16(len(e.Label))
}

func (e TestEvent) PayloadLen() uint32 {
	return uint32(len(e.Payload))
}

func (e TestEvent) Len() int64 {
	return int64(22 + len(e.Label) + len(e.Payload))
}

func (e TestEvent) Checksum(t *testing.T) uint64 {
	c := internal.ChecksumT(t, e.Timestamp, e.Label, e.Payload)
	return c
}
