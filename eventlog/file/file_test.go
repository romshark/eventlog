package file_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	filePath := fmt.Sprintf(
		"./test_%s_%s",
		strings.ReplaceAll(t.Name(), "/", "_"),
		time.Now().Format(time.RFC3339Nano),
	)

	t.Cleanup(func() {
		if err := os.Remove(filePath); err != nil {
			t.Logf("cleaning up file: %s", err)
		}
	})

	l, err := file.New(filePath)
	require.NoError(t, err)
	require.NotNil(t, l)

	_, v1, _, err := l.Append(eventlog.Event{PayloadJSON: []byte(`{"i":0}`)})
	require.NoError(t, err)
	require.Greater(t, v1, uint64(0))

	_, v2, _, err := l.Append(eventlog.Event{PayloadJSON: []byte(`{"i":1}`)})
	require.NoError(t, err)
	require.Greater(t, v2, v1)

	invalidOffset := v1 + 1
	nextOffset, err := l.Scan(invalidOffset, 1, func(
		uint64, uint64, []byte, []byte,
	) error {
		panic("unexpected call")
	})
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: %s", err,
	)
	require.Zero(t, nextOffset)
}
