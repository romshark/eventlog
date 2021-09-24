package internal_test

import (
	"encoding/json"
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/romshark/eventlog/eventlog/file/internal/test"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestReadHeader(t *testing.T) {
	meta := map[string]string{
		"field1": "value1",
		"field2": "value2",
	}

	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	mev := eventlog.Event{
		EventData: eventlog.EventData{
			PayloadJSON: metaJSON,
		},
	}

	type ER = test.ExpectedRead

	rec := test.NewReadRecorder(t,
		test.ExpectedRead{ // File format version
			Offset: 0,
			Write:  internal.MakeU32LE(5),
		}, test.ExpectedRead{ // Header checksum
			Offset: 4,
			Write:  internal.MakeU64LE(test.Checksum(t, mev)),
		}, test.ExpectedRead{ // File creation time
			Offset: 4 + 8,
			Write:  internal.MakeU64LE(mev.Timestamp),
		}, test.ExpectedRead{ // Label length (unused)
			Offset: 4 + 8 + 8,
			Write:  internal.MakeU16LE(test.LabelLen(mev)),
		}, test.ExpectedRead{ // Metadata length
			Offset: 4 + 8 + 8 + 2,
			Write:  internal.MakeU32LE(test.PayloadLen(mev)),
		}, test.ExpectedRead{ // Metadata
			Offset: 4 + 8 + 8 + 2 + 4,
			Write:  metaJSON,
		}, test.ExpectedRead{ // Previous version (unused)
			Offset: 4 + 8 + 8 + 2 + 4 + int64(test.PayloadLen(mev)),
			Write:  internal.MakeU64LE(mev.VersionPrevious),
		},
	)

	metadata := make(map[string]string)
	headerLen, err := internal.ReadHeader(
		test.NewBuffer(),
		rec,
		xxhash.New(),
		internal.ReaderConf{
			MinPayloadLen: 7,
			MaxPayloadLen: 512,
		},
		func(f, v string) error {
			metadata[f] = v
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(4+8+8+2+4+len(metaJSON)+8), headerLen)

	require.Len(t, metadata, 2)
	for f, v := range meta {
		require.Contains(t, metadata, f)
		require.Equal(t, v, metadata[f])
	}
}
