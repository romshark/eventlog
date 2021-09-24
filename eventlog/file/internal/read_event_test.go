package internal_test

import (
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/romshark/eventlog/eventlog/file/internal/test"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestReadEvent(t *testing.T) {
	type ExpectedResult struct {
		Checksum        uint64
		Timestamp       uint64
		Label           string
		Payload         string
		PreviousVersion uint64
		BytesRead       int
		CheckErr        func(*testing.T, error)
	}

	for _, tt := range []struct {
		name           string
		expectedReads  []test.ExpectedRead
		offset         int64
		expectedResult ExpectedResult
	}{
		{
			name:   "with_label",
			offset: 10,
			expectedReads: []test.ExpectedRead{
				{ // Checksum
					Offset: 10,
					Write: internal.MakeU64LE(test.Checksum(
						t,
						eventlog.Event{
							EventData: eventlog.EventData{
								Label:       []byte("label"),       // Label
								PayloadJSON: []byte(`{"x":"xyz"}`), // Payload

							},
							Timestamp:       5555555555, // Timestamp
							VersionPrevious: 44444444,   // Previous Version
						},
					)),
				}, { // Timestamp
					Offset: 10 + 8,
					Write:  internal.MakeU64LE(5555555555),
				}, { // Label length
					Offset: 10 + 8 + 8,
					Write:  internal.MakeU16LE(5),
				}, { // Payload length
					Offset: 10 + 8 + 8 + 2,
					Write:  internal.MakeU32LE(11),
				}, { // Label
					Offset: 10 + 8 + 8 + 2 + 4,
					Write:  []byte("label"),
				}, { // Payload
					Offset: 10 + 8 + 8 + 2 + 4 + int64(len("label")),
					Write:  []byte(`{"x":"xyz"}`),
				}, { // Previous version
					Offset: 10 + 8 + 8 + 2 + 4 +
						int64(len("label")) +
						int64(len(`{"x":"xyz"}`)),
					Write: internal.MakeU64LE(44444444),
				},
			},
			expectedResult: ExpectedResult{
				Checksum: test.Checksum(t, eventlog.Event{
					EventData: eventlog.EventData{
						Label:       []byte("label"),       // Label
						PayloadJSON: []byte(`{"x":"xyz"}`), // Payload

					},
					Timestamp:       5555555555, // Timestamp
					VersionPrevious: 44444444,   // Previous Version
				}),
				Timestamp:       5555555555,
				Label:           "label",
				Payload:         `{"x":"xyz"}`,
				PreviousVersion: 44444444,
				BytesRead: file.EntryHeaderLen +
					len("label") +
					len(`{"x":"xyz"}`),
				CheckErr: func(t *testing.T, err error) {
					require.NoError(t, err)
				},
			},
		},
		{
			name:   "without_label",
			offset: 10,
			expectedReads: []test.ExpectedRead{
				{ // Checksum
					Offset: 10,
					Write: internal.MakeU64LE(test.Checksum(t, eventlog.Event{
						EventData: eventlog.EventData{
							PayloadJSON: []byte(`{"x":"xyz"}`), // Payload
						},
						Timestamp:       5555555555, // Timestamp
						VersionPrevious: 44444444,   // Previous version
					})),
				}, { // Timestamp
					Offset: 10 + 8,
					Write:  internal.MakeU64LE(5555555555),
				}, { // Label length
					Offset: 10 + 8 + 8,
					Write:  internal.MakeU16LE(0),
				}, { // Payload length
					Offset: 10 + 8 + 8 + 2,
					Write:  internal.MakeU32LE(11),
				}, { // Payload
					Offset: 10 + 8 + 8 + 2 + 4,
					Write:  []byte(`{"x":"xyz"}`),
				}, { // Previous version
					Offset: 10 + 8 + 8 + 2 + 4 + int64(len(`{"x":"xyz"}`)),
					Write:  internal.MakeU64LE(44444444),
				},
			},
			expectedResult: ExpectedResult{
				Checksum: test.Checksum(t, eventlog.Event{
					EventData: eventlog.EventData{
						PayloadJSON: []byte(`{"x":"xyz"}`), // Payload

					},
					Timestamp:       5555555555, // Timestamp
					VersionPrevious: 44444444,   // Previoius version
				}),
				Timestamp:       5555555555,
				Payload:         `{"x":"xyz"}`,
				PreviousVersion: 44444444,
				BytesRead:       file.EntryHeaderLen + len(`{"x":"xyz"}`),
				CheckErr: func(t *testing.T, err error) {
					require.NoError(t, err)
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			conf := internal.ReaderConf{
				MinPayloadLen: file.MinPayloadLen,
				MaxPayloadLen: file.MaxPayloadLen,
			}
			buf := make([]byte, file.EntryHeaderLen+
				file.MaxLabelLen+
				conf.MaxPayloadLen)
			rec := test.NewReadRecorder(t, tt.expectedReads...)
			checksum, event, bytesRead, err :=
				internal.ReadEvent(buf, rec, xxhash.New(), tt.offset, conf)
			r.NoError(err)
			r.Equal(tt.expectedResult.Checksum, checksum)
			r.Zero(event.VersionNext)
			r.Equal(uint64(tt.offset), event.Version)
			r.Equal(tt.expectedResult.Timestamp, event.Timestamp)
			r.Equal(tt.expectedResult.Label, string(event.Label))
			r.Equal(tt.expectedResult.Payload, string(event.PayloadJSON))
			r.Equal(tt.expectedResult.PreviousVersion, event.VersionPrevious)
			r.Equal(int64(tt.expectedResult.BytesRead), bytesRead)
			tt.expectedResult.CheckErr(t, err)
		})
	}
}
