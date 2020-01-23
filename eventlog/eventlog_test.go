package eventlog_test

import (
	"fmt"
	"testing"

	eventlog "github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/const"

	"github.com/stretchr/testify/require"
)

func TestVerifyPayload(t *testing.T) {
	for input, successExpected := range consts.JSONValidationTest() {
		t.Run(fmt.Sprintf("%t_%s", successExpected, input), func(t *testing.T) {
			err := eventlog.VerifyPayload([]byte(input))
			if successExpected {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
