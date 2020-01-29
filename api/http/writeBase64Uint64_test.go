package http

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/romshark/eventlog/internal/bufpool"
	"github.com/stretchr/testify/require"
)

func TestWriteBase64Uint64(t *testing.T) {
	p := bufpool.NewPool(64)
	b := p.Get()

	for input, expect := range map[uint64]string{
		1234567890:     "0gKWSQAAAAA",
		0:              "AAAAAAAAAAA",
		math.MaxUint64: "__________8",
	} {
		t.Run(fmt.Sprintf("%d_%q", input, expect), func(t *testing.T) {
			w := new(bytes.Buffer)

			require.NoError(t, writeBase64Uint64(input, b, w))
			require.Equal(t, expect, w.String())

			d := base64.NewDecoder(base64.RawURLEncoding, w)
			r := make([]byte, 8)
			n, err := io.ReadFull(d, r)
			require.True(t, n > 0)
			require.NoError(t, err)

			require.Equal(t, input, binary.LittleEndian.Uint64(r))
		})
	}
}
