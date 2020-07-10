package itoa_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/romshark/eventlog/internal/itoa"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	for _, i := range []uint32{
		0, 1, 6, 10, 100, 6789,
		9_999, 99_999, 999_999, 9_999_999, 99_999_999,
		math.MaxUint32,
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			require.NoError(t, itoa.U32toa(buf, i))
			require.Equal(t, fmt.Sprintf("%d", i), buf.String())
		})
	}
}
