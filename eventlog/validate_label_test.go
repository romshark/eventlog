package eventlog_test

import (
	"testing"

	"github.com/romshark/eventlog/internal/makestr"

	"github.com/romshark/eventlog/eventlog"
	"github.com/stretchr/testify/require"
)

func TestValidateLabel(t *testing.T) {
	for _, t1 := range []struct {
		name        string
		label       string
		expectedErr error
	}{
		{"empty", "", nil},
		{"valid", "baz", nil},
		{"max", makestr.Make(65535, 'x'), nil},
		{"too long", makestr.Make(65536, 'x'), eventlog.ErrLabelTooLong},
		{"invalid char", "??", eventlog.ErrLabelContainsIllegalChars},
	} {
		t.Run(t1.name, func(t *testing.T) {
			err := eventlog.ValidateLabel(t1.label)
			require.Equal(t, t1.expectedErr, err)
		})
	}
}
