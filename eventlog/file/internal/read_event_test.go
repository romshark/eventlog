package internal_test

import (
	"testing"

	"github.com/cespare/xxhash"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/stretchr/testify/require"
)

func TestReadEvent(t *testing.T) {
	type ExpectedResult struct {
		Checksum  uint64
		Timestamp uint64
		Offset    uint64
		Label     string
		Payload   []byte
		BytesRead int
		CheckErr  func(*testing.T, error)
	}

	for _, tt := range []struct {
		name           string
		expectedReads  []ExpectedRead
		offset         int64
		expectedResult ExpectedResult
	}{
		{
			name:   "with_label",
			offset: 10,
			expectedReads: []ExpectedRead{
				{Offset: 10, Write: internal.MakeU64LE(internal.ChecksumT(
					t,
					5555555555,    // Timestamp
					"label",       // Label
					`{"x":"xyz"}`, // Payload
				))}, // Checksum
				{18, internal.MakeU64LE(5555555555)}, // Timestamp
				{26, internal.MakeU16LE(5)},          // Label length
				{28, internal.MakeU32LE(11)},         // Payload length
				{32, []byte("label")},                // Label
				{37, []byte(`{"x":"xyz"}`)},          // Payload
			},
			expectedResult: ExpectedResult{
				Checksum: internal.ChecksumT(
					t,
					5555555555,    // Timestamp
					"label",       // Label
					`{"x":"xyz"}`, // Payload
				),
				Timestamp: 5555555555,
				Offset:    10,
				Label:     "label",
				Payload:   []byte(`{"x":"xyz"}`),
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
			expectedReads: []ExpectedRead{
				{Offset: 10, Write: internal.MakeU64LE(internal.ChecksumT(
					t,
					5555555555,    // Timestamp
					"",            // Label
					`{"x":"xyz"}`, // Payload
				))}, // Checksum
				{18, internal.MakeU64LE(5555555555)}, // Timestamp
				{26, internal.MakeU16LE(0)},          // Label length
				{28, internal.MakeU32LE(11)},         // Payload length
				{32, []byte(`{"x":"xyz"}`)},          // Payload
			},
			expectedResult: ExpectedResult{
				Checksum: internal.ChecksumT(
					t,
					5555555555,    // Timestamp
					"",            // Label
					`{"x":"xyz"}`, // Payload
				),
				Timestamp: 5555555555,
				Offset:    10,
				Payload:   []byte(`{"x":"xyz"}`),
				BytesRead: file.EntryHeaderLen + len(`{"x":"xyz"}`),
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
			rec := NewReadRecorder(t, tt.expectedReads...)
			checksum, timestamp, label, payload, bytesRead, err :=
				internal.ReadEvent(buf, rec, xxhash.New(), tt.offset, conf)
			r.Equal(tt.expectedResult.Checksum, checksum)
			r.Equal(tt.expectedResult.Timestamp, timestamp)
			r.Equal(tt.expectedResult.Label, string(label))
			r.Equal(tt.expectedResult.Payload, payload)
			r.Equal(int64(tt.expectedResult.BytesRead), bytesRead)
			tt.expectedResult.CheckErr(t, err)
		})
	}
}
