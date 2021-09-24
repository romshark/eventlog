package file_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), fmt.Sprintf(
		"test_%s_%s",
		strings.ReplaceAll(t.Name(), "/", "_"),
		time.Now().Format("2006_01_02T15_04_05_999999999Z07_00"),
	))

	meta := map[string]string{
		"name": "testlog",
	}

	err := file.Create(filePath, meta, 0777)
	require.NoError(t, err)

	l, err := file.Open(filePath)
	require.NoError(t, err)
	require.NotNil(t, l)

	_, v1, _, err := l.Append(
		eventlog.EventData{PayloadJSON: []byte(`{"i":0}`)},
	)
	require.NoError(t, err)
	require.Greater(t, v1, uint64(0))

	_, v2, _, err := l.Append(
		eventlog.EventData{PayloadJSON: []byte(`{"i":1}`)},
	)
	require.NoError(t, err)
	require.Greater(t, v2, v1)

	invalidOffset := v1 + 1
	err = l.Scan(invalidOffset, false, func(e eventlog.Event) error {
		panic("unexpected call")
	})
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)

	{
		mf := make(map[string]string, l.MetadataLen())
		l.ScanMetadata(func(field, value string) bool {
			mf[field] = value
			return true
		})

		require.Len(t, mf, len(meta))
		for f, v := range meta {
			require.Contains(t, mf, f)
			require.Equal(t, v, mf[f])
		}
	}
}
