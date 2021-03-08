package internal_test

import (
	"encoding/json"
	"testing"

	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/romshark/eventlog/eventlog/file/internal/test"

	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/require"
)

func TestReadHeader(t *testing.T) {
	meta := map[string]string{
		"field1": "value1",
		"field2": "value2",
	}

	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	mev := test.TestEvent{Payload: string(metaJSON)}

	rec := NewReadRecorder(t,
		ExpectedRead{Offset: 0, Write: internal.MakeU32LE(4)},
		ExpectedRead{Offset: 4, Write: internal.MakeU64LE(mev.Checksum(t))},
		ExpectedRead{Offset: 12, Write: internal.MakeU64LE(mev.Timestamp)},
		ExpectedRead{Offset: 20, Write: internal.MakeU16LE(mev.LabelLen())},
		ExpectedRead{Offset: 22, Write: internal.MakeU32LE(mev.PayloadLen())},
		ExpectedRead{Offset: 26, Write: metaJSON},
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
	require.Equal(t, int64(26+len(metaJSON)), headerLen)

	require.Len(t, metadata, 2)
	for f, v := range meta {
		require.Contains(t, metadata, f)
		require.Equal(t, v, metadata[f])
	}
}
