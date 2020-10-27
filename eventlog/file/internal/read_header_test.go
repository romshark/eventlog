package internal_test

import (
	"testing"

	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/stretchr/testify/require"
)

func TestReadHeader(t *testing.T) {
	const expectedVersion = uint32(123)
	rec := NewReadRecorder(t,
		ExpectedRead{Offset: 0, Write: internal.MakeU32LE(expectedVersion)},
	)
	checkVersion := func(v uint32) error {
		require.Equal(t, expectedVersion, v)
		return nil
	}
	require.NoError(t, internal.ReadHeader(
		make([]byte, 4), rec, checkVersion,
	))
}
