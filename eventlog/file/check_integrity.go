package file

import (
	"context"
	"fmt"
	"io"

	"github.com/cespare/xxhash"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"
)

type OffsetReader = internal.OffsetReader
type ReadBuffer = internal.ReadBuffer

const MinReadBufferLen = MaxLabelLen + MaxPayloadLen

func CheckIntegrity(
	ctx context.Context,
	buffer ReadBuffer,
	reader OffsetReader,
	onEntry func(
		offset int64,
		checksum uint64,
		timestamp uint64,
		label []byte,
		payloadJSON []byte,
	) error,
) error {
	buffer.MustValidate(readConfig)

	if err := internal.ReadHeader(buffer, reader, checkVersion); err != nil {
		return err
	}

	var previousTime uint64

	for i := int64(FileHeaderLen); ; {
		if err := ctx.Err(); err != nil {
			return err
		}

		checksum, timestamp, label, payload, n, err := internal.ReadEvent(
			buffer, reader, xxhash.New(), i, readConfig,
		)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("reading entry at offset %d: %w", i, err)
		}

		if timestamp < previousTime {
			return fmt.Errorf(
				"invalid timestamp (%d) at offset %d"+
					" greater than previous (%d)",
				timestamp, i, previousTime,
			)
		}
		previousTime = timestamp

		e := eventlog.Event{
			Label:       string(label),
			PayloadJSON: payload,
		}
		if err := e.Validate(); err != nil {
			return fmt.Errorf("invalid payload at offset %d: %w", i, err)
		}

		if err := onEntry(
			i,
			checksum,
			timestamp,
			label,
			payload,
		); err != nil {
			return err
		}
		i += int64(n)
	}

	return nil
}
