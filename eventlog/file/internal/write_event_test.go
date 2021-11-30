package internal_test

import (
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"

	"github.com/stretchr/testify/require"
)

func TestWriteEvent(t *testing.T) {
	type WC = writeCall
	for _, tt := range []struct {
		name                string
		event               eventlog.Event
		checksum            uint64
		offset              int64
		expectedWriterCalls []interface{}
	}{
		{
			name: "with_label",
			event: eventlog.Event{
				Timestamp:       99999999,
				Version:         44444444,
				VersionPrevious: 33333333,
				EventData: eventlog.EventData{
					Label:       []byte("foo"),
					PayloadJSON: []byte(`{"x":0}`),
				},
			},
			checksum: 42,
			offset:   10,
			expectedWriterCalls: []interface{}{
				WC{internal.MakeU64LE(42), 10},            // Checksum
				WC{internal.MakeU64LE(99999999), 10 + 8},  // Timestamp
				WC{internal.MakeU16LE(3), 10 + 8 + 8},     // Label length
				WC{internal.MakeU32LE(7), 10 + 8 + 8 + 2}, // Payload length
				WC{[]byte("foo"), 10 + 8 + 8 + 2 + 4},     // Label
				WC{ // Payload
					[]byte(`{"x":0}`),
					10 + 8 + 8 + 2 + 4 + int64(len("foo")),
				},
				WC{ // Previous version
					internal.MakeU64LE(33333333),
					10 + 8 + 8 + 2 + 4 + 3 + int64(len(`{"x":0}`)),
				},
				syncCall{},
			},
		},
		{
			name: "without_label_without_offset",
			event: eventlog.Event{
				Timestamp: 7777777777,
				Version:   55555555,
				EventData: eventlog.EventData{
					Label:       nil,
					PayloadJSON: []byte(`{"x":"xyz"}`),
				},
			},
			checksum: 9999,
			offset:   0,
			expectedWriterCalls: []interface{}{
				WC{internal.MakeU64LE(9999), 0},          // Checksum
				WC{internal.MakeU64LE(7777777777), 8},    // Timestamp
				WC{internal.MakeU16LE(0), 8 + 8},         // Label length
				WC{internal.MakeU32LE(11), 8 + 8 + 2},    // Payload length
				WC{[]byte(`{"x":"xyz"}`), 8 + 8 + 2 + 4}, // Payload
				WC{ // Previous version
					internal.MakeU64LE(0),
					8 + 8 + 2 + 4 + int64(len(`{"x":"xyz"}`)),
				},
				syncCall{},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			w := &testWriterRecorder{}
			buf := make([]byte, 1024)
			written, err := internal.WriteEvent(
				w, buf, tt.checksum, tt.offset, tt.event, internal.Config{
					MinPayloadLen: 7,
					MaxPayloadLen: 512,
				},
			)
			r.NoError(err)

			expectedLen := 8 + // Checksum
				8 + // Timestamp
				2 + // Label len
				4 + // Payload len
				len(tt.event.Label) +
				len(tt.event.PayloadJSON) +
				8 // Previous version

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
