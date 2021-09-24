package file_test

// import (
// 	"errors"
// 	"fmt"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/romshark/eventlog/eventlog"
// 	"github.com/romshark/eventlog/eventlog/file"
// 	"github.com/stretchr/testify/require"
// )

// func TestChecksum(t *testing.T) {
// 	filePath := fmt.Sprintf(
// 		"%s/test_%s_%s",
// 		t.TempDir(),
// 		strings.ReplaceAll(t.Name(), "/", "_"),
// 		time.Now().Format(time.RFC3339Nano),
// 	)

// 	meta := map[string]string{
// 		"name": "testlog",
// 	}

// 	err := file.Create(filePath, meta, 0777)
// 	require.NoError(t, err)

// 	l, err := file.Open(filePath)
// 	require.NoError(t, err)
// 	require.NotNil(t, l)

// 	_, v1, _, err := l.Append(eventlog.Event{PayloadJSON: []byte(`{"i":0}`)})
// 	require.NoError(t, err)
// 	require.Greater(t, v1, uint64(0))

// 	_, v2, _, err := l.Append(eventlog.Event{PayloadJSON: []byte(`{"i":1}`)})
// 	require.NoError(t, err)
// 	require.Greater(t, v2, v1)

// 	invalidOffset := v1 + 1
// 	nextOffset, err := l.Scan(invalidOffset, 1, func(
// 		uint64, uint64, []byte, []byte,
// 	) error {
// 		panic("unexpected call")
// 	})
// 	require.Error(t, err)
// 	require.True(
// 		t,
// 		errors.Is(err, eventlog.ErrInvalidOffset),
// 		"unexpected error: %s", err,
// 	)
// 	require.Zero(t, nextOffset)

// 	{
// 		mf := make(map[string]string, l.MetadataLen())
// 		l.ScanMetadata(func(field, value string) bool {
// 			mf[field] = value
// 			return true
// 		})

// 		require.Len(t, mf, len(meta))
// 		for f, v := range meta {
// 			require.Contains(t, mf, f)
// 			require.Equal(t, v, mf[f])
// 		}
// 	}
// }
