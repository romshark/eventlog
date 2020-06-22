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

	l, err := file.NewFile(filePath)
	require.NoError(t, err)
	require.NotNil(t, l)

	_, v, _, err := l.Append([]byte(`{"i":0}`))
	require.NoError(t, err)
	require.Greater(t, v, uint64(0))

	_, _, _, err = l.Append([]byte(`{"i":1}`))
	require.NoError(t, err)

	nextOffset, err := l.Scan(v+1, 1, func(
		timestamp uint64,
		payloadJSON []byte,
		offset uint64,
	) error {
		return fmt.Errorf("unexpected call")
	})
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: %s", err,
	)
	require.Zero(t, nextOffset)
}
