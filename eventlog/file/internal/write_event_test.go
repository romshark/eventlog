package internal_test

import (
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/stretchr/testify/require"
)

func TestWriteEvent(t *testing.T) {
	for _, tt := range []struct {
		name                string
		event               eventlog.Event
		checksum            uint64
		timestamp           uint64
		offset              int64
		expectedWriterCalls []interface{}
	}{
		{
			name: "with_label",
			event: eventlog.Event{
				Label:       "foo",
				PayloadJSON: []byte(`{"x":0}`),
			},
			checksum:  42,
			timestamp: 99999999,
			offset:    10,
			expectedWriterCalls: []interface{}{
				// Checksum
				writeCall{internal.MakeU64LE(42), 10},
				// Timestamp
				writeCall{internal.MakeU64LE(99999999), 10 + 8},
				// Label length
				writeCall{internal.MakeU16LE(3), 10 + 8 + 8},
				// Payload length
				writeCall{internal.MakeU32LE(7), 10 + 8 + 8 + 2},
				// Label
				writeCall{[]byte("foo"), 10 + 8 + 8 + 2 + 4},
				// Payload
				writeCall{[]byte(`{"x":0}`), 10 + 8 + 8 + 2 + 4 + 3},
				syncCall{},
			},
		},
		{
			name: "without_label_without_offset",
			event: eventlog.Event{
				Label:       "",
				PayloadJSON: []byte(`{"x":"xyz"}`),
			},
			checksum:  9999,
			timestamp: 7777777777,
			offset:    0,
			expectedWriterCalls: []interface{}{
				// Checksum
				writeCall{internal.MakeU64LE(9999), 0},
				// Timestamp
				writeCall{internal.MakeU64LE(7777777777), 8},
				// Label length
				writeCall{internal.MakeU16LE(0), 8 + 8},
				// Payload length
				writeCall{internal.MakeU32LE(11), 8 + 8 + 2},
				// Payload
				writeCall{[]byte(`{"x":"xyz"}`), 8 + 8 + 2 + 4},
				syncCall{},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			w := &testWriterRecorder{}
			buf := make([]byte, 1024)
			written, err := internal.WriteEvent(
				w, buf, tt.checksum, tt.offset, tt.timestamp, tt.event,
			)
			r.NoError(err)

			expectedLen := 8 + // checksum
				8 + // timestamp
				2 + // label len
				4 + // payload len
				len(tt.event.Label) +
				len(tt.event.PayloadJSON)

			r.Equal(expectedLen, written)
			r.Equal(tt.expectedWriterCalls, w.calls)
		})
	}
}

type testWriterRecorder struct {
	calls []interface{}
}

func (w *testWriterRecorder) WriteAt(b []byte, o int64) (int, error) {
	c := make([]byte, len(b))
	copy(c, b)
	w.calls = append(w.calls, writeCall{offset: o, data: c})
	return len(b), nil
}

func (w *testWriterRecorder) Sync() error {
	w.calls = append(w.calls, syncCall{})
	return nil
}

type writeCall struct {
	data   []byte
	offset int64
}

type syncCall struct{}

type testHasherRecorder struct {
	sum64 uint64
	calls [][]byte
}

func (h *testHasherRecorder) Write(b []byte) (int, error) {
	c := make([]byte, len(b))
	copy(c, b)
	h.calls = append(h.calls, c)
	return len(b), nil
}

func (h *testHasherRecorder) Sum64() uint64 { return h.sum64 }
