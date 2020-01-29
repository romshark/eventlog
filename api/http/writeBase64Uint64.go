package http

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/romshark/eventlog/internal/bufpool"
)

func writeBase64Uint64(
	i uint64,
	buffer *bufpool.Buffer,
	out io.Writer,
) error {
	buffer.Reset()
	buf := buffer.Bytes()
	buf8 := buf[:8]
	encoder := base64.NewEncoder(base64.RawURLEncoding, out)

	binary.LittleEndian.PutUint64(buf8, i)

	// Encode
	_, err := encoder.Write(buf8)
	if err != nil {
		return fmt.Errorf("encoding base64: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("closing base64 encoder: %w", err)
	}

	return nil
}
